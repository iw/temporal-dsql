package dsql

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/gate"
)

// acquireKind picks an acquire's queue at the gate.
type acquireKind = gate.Kind

const (
	acquireRead  = gate.Read
	acquireWrite = gate.Write
)

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

// errGateRefused is a connection throttle, so a refused call reports a throttled fact and
// returns ResourceExhausted.
var errGateRefused = fmt.Errorf("%w: the pool can't serve the call before its deadline", errConnectThrottled)

// acquireGate admits callers to the pool by gate.Policy, writes ahead of reads, and waits
// for the policy's decisions: a queued caller waits until a release hands it a slot, its
// context ends, or too little time is left before its deadline.
type acquireGate struct {
	policy  *gate.Policy
	clock   clock.TimeSource
	metrics gateMetrics

	waited atomic.Int64 // admissions that waited, for queued facts
}

// gateClock is the wall clock with time.Now's monotonic reading, which
// clock.RealTimeSource drops when it converts to UTC, so the gate times waits and holds
// on the monotonic clock as time.Since does.
type gateClock struct{ clock.RealTimeSource }

func (gateClock) Now() time.Time { return time.Now() }

func newAcquireGate(size int, handler metrics.Handler) *acquireGate {
	timeSource := gateClock{}
	return &acquireGate{
		policy:  gate.NewPolicy(size, timeSource),
		clock:   timeSource,
		metrics: newGateMetrics(handler),
	}
}

func (g *acquireGate) acquire(ctx context.Context) (*gate.Ticket, error) {
	kind := acquireKindOf(ctx)
	deadline, hasDeadline := ctx.Deadline()
	admission := g.policy.Admit(kind, deadline, hasDeadline)
	if admission.Ticket != nil {
		return admission.Ticket, nil
	}
	w := admission.Waiter
	if w == nil {
		g.metrics.refused(kind)
		return nil, errGateRefused
	}

	start := g.clock.Now()
	var expired <-chan time.Time
	if hasDeadline {
		fired, timer := g.clock.NewTimer(w.GiveUp().Sub(g.clock.Now()))
		defer timer.Stop()
		expired = fired
	}
	select {
	case <-w.Ready():
		return g.ticketAfterWait(w, start), nil
	case <-expired:
	case <-ctx.Done():
	}
	if g.policy.Withdraw(w) {
		return g.ticketAfterWait(w, start), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.metrics.refused(kind)
	return nil, errGateRefused
}

func (g *acquireGate) ticketAfterWait(w *gate.Waiter, start time.Time) *gate.Ticket {
	g.waited.Add(1)
	g.metrics.admittedAfterWait(w.Kind(), g.clock.Since(start))
	return w.Ticket()
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
