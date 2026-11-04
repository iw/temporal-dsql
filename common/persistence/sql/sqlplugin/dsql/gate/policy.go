// Package gate is the admission policy of the DSQL plugin's pool gate: which callers get
// one of the pool's connections, in what order, and which it refuses because it can't serve
// them before their deadlines. The policy reads time from a clock.TimeSource and never
// blocks. The plugin's gate waits on its decisions with channels and timers, and
// tools/adaptivesim follows the same decisions on simulated time.
package gate

import (
	"container/list"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
)

// Kind picks an acquire's queue at the gate.
type Kind int

const (
	Read Kind = iota
	Write
)

func (k Kind) String() string {
	if k == Write {
		return "write"
	}
	return "read"
}

const (
	// priorityQueue holds writes, and calls without a deadline, which would otherwise
	// wait behind writes without end.
	priorityQueue = 0
	readQueue     = 1

	// HoldMargin reserves more than the p90 hold before a caller's deadline, so a caller
	// admitted late still has time if its statements run long.
	HoldMargin = 1.5
	// HoldSamples is how many of each kind's most recent holds the estimates cover.
	HoldSamples          = 256
	holdEstimateInterval = 50 * time.Millisecond
)

type (
	// Policy admits at most as many callers as the pool has connections, writes ahead of
	// reads, so the calls under way finish before new ones start. pgxpool alone serves
	// callers first come, first served, and on a short pool they all wait until their
	// deadlines pass.
	//
	// The policy refuses at once a caller that can't be admitted in time to finish before
	// its deadline, judged from recent hold times, and a queued caller gives up once too
	// little time is left.
	Policy struct {
		size  int
		clock clock.TimeSource
		holds [2]holdTimes // by Kind

		mu     sync.Mutex
		held   int
		queues [2]list.List // of *Waiter
	}

	// Admission is the policy's answer to a caller: a ticket when it admits the caller at
	// once, a waiter when it queues the caller, and neither when it refuses the caller.
	Admission struct {
		Ticket *Ticket
		Waiter *Waiter
	}

	// Waiter is a queued caller.
	Waiter struct {
		policy  *Policy
		kind    Kind
		queue   int
		elem    *list.Element
		giveUp  time.Time
		ready   chan struct{}
		granted bool
	}

	// Ticket is an admission. It holds a slot until it's released.
	Ticket struct {
		policy   *Policy
		kind     Kind
		admitted time.Time
		released atomic.Bool
	}
)

// NewPolicy returns a policy with size slots, reading time from timeSource.
func NewPolicy(size int, timeSource clock.TimeSource) *Policy {
	return &Policy{size: max(size, 1), clock: timeSource}
}

// Admit admits a caller of the kind at once while a slot is free and no caller is queued
// ahead of it. Otherwise it refuses a caller with a deadline that it can't admit in time to
// finish, and queues the rest. A caller without a deadline waits with the writes.
func (p *Policy) Admit(kind Kind, deadline time.Time, hasDeadline bool) Admission {
	queue := readQueue
	if kind == Write || !hasDeadline {
		queue = priorityQueue
	}

	p.mu.Lock()
	ahead := p.queues[priorityQueue].Len()
	if queue == readQueue {
		ahead += p.queues[readQueue].Len()
	}
	if p.held < p.size && ahead == 0 {
		p.held++
		p.mu.Unlock()
		return Admission{Ticket: p.ticket(kind)}
	}
	var giveUp time.Time
	if hasDeadline {
		p90, _ := p.Estimate(kind)
		reserve := time.Duration(HoldMargin * float64(p90))
		_, meanRead := p.Estimate(Read)
		_, meanWrite := p.Estimate(Write)
		// The callers ahead start first, and then a caller holding a slot has to finish.
		wait := time.Duration(float64(ahead+1) / float64(p.size) * float64(max(meanRead, meanWrite)))
		if deadline.Sub(p.clock.Now()) < wait+reserve {
			p.mu.Unlock()
			return Admission{}
		}
		giveUp = deadline.Add(-reserve)
	}
	w := &Waiter{policy: p, kind: kind, queue: queue, giveUp: giveUp, ready: make(chan struct{})}
	w.elem = p.queues[queue].PushBack(w)
	p.mu.Unlock()
	return Admission{Waiter: w}
}

// Withdraw takes a caller that stops waiting out of its queue. It returns true when a
// release had already handed the caller a slot, which the caller then holds.
func (p *Policy) Withdraw(w *Waiter) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w.granted {
		return true
	}
	p.queues[w.queue].Remove(w.elem)
	return false
}

// release hands the slot to the first queued caller, writes first, and returns it, or
// frees the slot when no caller is queued.
func (p *Policy) release() *Waiter {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.queues {
		if e := p.queues[i].Front(); e != nil {
			p.queues[i].Remove(e)
			w := e.Value.(*Waiter) //nolint:revive // type-safe: the queues hold only *Waiter
			w.granted = true
			close(w.ready)
			return w
		}
	}
	p.held--
	return nil
}

// Estimate returns the p90 and the mean of the kind's recent holds, or while the kind has
// none, the other kind's, so a kind that hasn't got through lately isn't admitted with no
// time reserved.
func (p *Policy) Estimate(kind Kind) (p90, mean time.Duration) {
	now := p.clock.Now()
	if p90, mean, ok := p.holds[kind].estimate(now); ok {
		return p90, mean
	}
	p90, mean, _ = p.holds[1-kind].estimate(now)
	return p90, mean
}

// RecordHold records a hold of the kind, as a released ticket does.
func (p *Policy) RecordHold(kind Kind, hold time.Duration) {
	p.holds[kind].record(hold)
}

// Holds returns how many recent holds of the kind the estimates cover.
func (p *Policy) Holds(kind Kind) int {
	return p.holds[kind].len()
}

// Held returns how many slots are taken.
func (p *Policy) Held() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.held
}

// Queued returns how many callers wait ahead of the reads, writes and callers without a
// deadline, and how many reads wait.
func (p *Policy) Queued() (priority, reads int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queues[priorityQueue].Len(), p.queues[readQueue].Len()
}

func (p *Policy) ticket(kind Kind) *Ticket {
	return &Ticket{policy: p, kind: kind, admitted: p.clock.Now()}
}

// Kind is the caller's kind.
func (w *Waiter) Kind() Kind { return w.kind }

// GiveUp is when the caller stops waiting and is refused: its deadline less the time
// reserved for its hold. It's zero for a caller without a deadline.
func (w *Waiter) GiveUp() time.Time { return w.giveUp }

// Ready is closed when a release hands the caller a slot.
func (w *Waiter) Ready() <-chan struct{} { return w.ready }

// Ticket returns the admission of a caller that a release handed a slot, and times its
// hold from now.
func (w *Waiter) Ticket() *Ticket { return w.policy.ticket(w.kind) }

// Release gives the slot back, and when the caller used the connection, records how long
// the caller held it. It returns the queued caller the slot went to, if any, for callers
// that don't wait on Ready.
func (t *Ticket) Release(used bool) *Waiter {
	if !t.released.CompareAndSwap(false, true) {
		return nil
	}
	if used {
		t.policy.holds[t.kind].record(t.policy.clock.Since(t.admitted))
	}
	return t.policy.release()
}

// holdTimes keeps the most recent hold times.
type holdTimes struct {
	mu       sync.Mutex
	ring     [HoldSamples]time.Duration
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
	h.next = (h.next + 1) % HoldSamples
	h.count = min(h.count+1, HoldSamples)
}

func (h *holdTimes) len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// estimate returns the p90 and the mean of the recent hold times, recomputed at most every
// holdEstimateInterval, and false when there are none.
func (h *holdTimes) estimate(now time.Time) (p90, mean time.Duration, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0, 0, false
	}
	if h.computed.IsZero() || now.Sub(h.computed) >= holdEstimateInterval {
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
