package dsql

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/metrics"
)

// acquireKind picks an acquire's queue at the gate.
type acquireKind int

const (
	acquireRead acquireKind = iota
	acquireWrite
)

func (k acquireKind) String() string {
	if k == acquireWrite {
		return "write"
	}
	return "read"
}

type acquireKindKey struct{}

// withAcquireKind marks the acquires made with ctx. Unmarked acquires are reads.
func withAcquireKind(ctx context.Context, kind acquireKind) context.Context {
	return context.WithValue(ctx, acquireKindKey{}, kind)
}

func acquireKindOf(ctx context.Context) acquireKind {
	if kind, ok := ctx.Value(acquireKindKey{}).(acquireKind); ok {
		return kind
	}
	return acquireRead
}

const (
	// priorityQueue holds writes, and calls without a deadline, which would otherwise
	// wait behind writes without end.
	priorityQueue = 0
	readQueue     = 1

	// holdMargin reserves more than the p90 hold before a caller's deadline, so a caller
	// admitted late still has time if its statements run long.
	holdMargin           = 1.5
	holdSamples          = 256
	holdEstimateInterval = 50 * time.Millisecond
)

// errGateRefused is a connection throttle, so a refused call reports a throttled fact and
// returns ResourceExhausted.
var errGateRefused = fmt.Errorf("%w: the pool can't serve the call before its deadline", errConnectThrottled)

// acquireGate admits at most as many callers as the pool has connections, writes ahead of
// reads, so the calls under way finish before new ones start. pgxpool alone serves callers
// first come, first served, and on a short pool they all wait until their deadlines pass.
//
// The gate refuses at once a caller that can't be admitted in time to finish before its
// deadline, judged from recent hold times, and refuses a waiting caller once too little
// time is left.
type acquireGate struct {
	size    int
	holds   [2]holdTimes // by acquireKind
	metrics gateMetrics

	mu     sync.Mutex
	held   int
	queues [2]list.List // of *gateWaiter

	waited atomic.Int64 // admissions that waited, for queued facts
}

type gateWaiter struct {
	ready   chan struct{}
	granted bool
}

func newAcquireGate(size int, handler metrics.Handler) *acquireGate {
	return &acquireGate{size: max(size, 1), metrics: newGateMetrics(handler)}
}

func (g *acquireGate) acquire(ctx context.Context) (*gateTicket, error) {
	kind := acquireKindOf(ctx)
	deadline, hasDeadline := ctx.Deadline()
	queue := readQueue
	if kind == acquireWrite || !hasDeadline {
		queue = priorityQueue
	}

	g.mu.Lock()
	ahead := g.queues[priorityQueue].Len()
	if queue == readQueue {
		ahead += g.queues[readQueue].Len()
	}
	if g.held < g.size && ahead == 0 {
		g.held++
		g.mu.Unlock()
		return g.ticket(kind), nil
	}
	var reserve time.Duration
	if hasDeadline {
		p90, _ := g.estimate(kind)
		reserve = time.Duration(holdMargin * float64(p90))
		_, meanRead := g.estimate(acquireRead)
		_, meanWrite := g.estimate(acquireWrite)
		// The callers ahead start first, and then a caller holding a slot has to finish.
		wait := time.Duration(float64(ahead+1) / float64(g.size) * float64(max(meanRead, meanWrite)))
		if time.Until(deadline) < wait+reserve {
			g.mu.Unlock()
			g.metrics.refused(kind)
			return nil, errGateRefused
		}
	}
	w := &gateWaiter{ready: make(chan struct{})}
	e := g.queues[queue].PushBack(w)
	g.mu.Unlock()

	start := time.Now()
	var expired <-chan time.Time
	if hasDeadline {
		timer := time.NewTimer(time.Until(deadline) - reserve)
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case <-w.ready:
		return g.ticketAfterWait(kind, start), nil
	case <-expired:
	case <-ctx.Done():
	}
	g.mu.Lock()
	if w.granted {
		g.mu.Unlock()
		return g.ticketAfterWait(kind, start), nil
	}
	g.queues[queue].Remove(e)
	g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.metrics.refused(kind)
	return nil, errGateRefused
}

func (g *acquireGate) ticket(kind acquireKind) *gateTicket {
	return &gateTicket{gate: g, kind: kind, admitted: time.Now()}
}

func (g *acquireGate) ticketAfterWait(kind acquireKind, start time.Time) *gateTicket {
	g.waited.Add(1)
	g.metrics.admittedAfterWait(kind, time.Since(start))
	return g.ticket(kind)
}

// release hands the slot to the first waiter, writes first.
func (g *acquireGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := range g.queues {
		if e := g.queues[i].Front(); e != nil {
			g.queues[i].Remove(e)
			w := e.Value.(*gateWaiter) //nolint:revive // type-safe: the queues hold only *gateWaiter
			w.granted = true
			close(w.ready)
			return
		}
	}
	g.held--
}

// estimate returns the p90 and the mean of the kind's recent holds, or while the kind has
// none, the other kind's, so a kind that hasn't got through lately isn't admitted with no
// time reserved.
func (g *acquireGate) estimate(kind acquireKind) (p90, mean time.Duration) {
	if p90, mean, ok := g.holds[kind].estimate(); ok {
		return p90, mean
	}
	p90, mean, _ = g.holds[1-kind].estimate()
	return p90, mean
}

// gateTicket is an admission. It holds a slot until it's released.
type gateTicket struct {
	gate     *acquireGate
	kind     acquireKind
	admitted time.Time
	released atomic.Bool
}

// release gives the slot back, and when the caller used the connection, records how long
// the caller held it.
func (t *gateTicket) release(used bool) {
	if !t.released.CompareAndSwap(false, true) {
		return
	}
	if used {
		t.gate.holds[t.kind].record(time.Since(t.admitted))
	}
	t.gate.release()
}

// holdTimes keeps the most recent hold times.
type holdTimes struct {
	mu       sync.Mutex
	ring     [holdSamples]time.Duration
	next     int
	count    int
	computed time.Time
	p90      time.Duration
	mean     time.Duration
}

func (h *holdTimes) record(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ring[h.next] = d
	h.next = (h.next + 1) % holdSamples
	h.count = min(h.count+1, holdSamples)
}

// estimate returns the p90 and the mean of the recent hold times, recomputed at most every
// holdEstimateInterval, and false when there are none.
func (h *holdTimes) estimate() (p90, mean time.Duration, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0, 0, false
	}
	if now := time.Now(); h.computed.IsZero() || now.Sub(h.computed) >= holdEstimateInterval {
		sorted := slices.Clone(h.ring[:h.count])
		slices.Sort(sorted)
		var sum time.Duration
		for _, d := range sorted {
			sum += d
		}
		h.p90 = sorted[int(math.Ceil(0.9*float64(len(sorted))))-1]
		h.mean = sum / time.Duration(len(sorted))
		h.computed = now
	}
	return h.p90, h.mean, true
}

type gateMetrics struct {
	waits    metrics.CounterIface
	waitTime metrics.TimerIface
	refusals metrics.CounterIface
}

func newGateMetrics(handler metrics.Handler) gateMetrics {
	if handler == nil {
		handler = metrics.NoopMetricsHandler
	}
	return gateMetrics{
		waits:    handler.Counter("dsql_pool_gate_waits_total"),
		waitTime: handler.Timer("dsql_pool_gate_wait"),
		refusals: handler.Counter("dsql_pool_gate_refusals_total"),
	}
}

func (m gateMetrics) admittedAfterWait(kind acquireKind, wait time.Duration) {
	tag := metrics.StringTag("kind", kind.String())
	m.waits.Record(1, tag)
	m.waitTime.Record(wait, tag)
}

func (m gateMetrics) refused(kind acquireKind) {
	m.refusals.Record(1, metrics.StringTag("kind", kind.String()))
}
