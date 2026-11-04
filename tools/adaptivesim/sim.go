// Package adaptivesim is a discrete-event simulator of the Temporal service's adaptive
// plane. It runs the signal plane, the adaptive controllers and their actuators closed-loop
// on simulated time, over a coarse model of history, its workers and its DSQL store. The
// controllers and the pool gate's admission policy are the real code; the service, the
// connections and the store are models. See README.md.
package adaptivesim

import (
	"hash/fnv"
	"math/rand/v2"
	"time"

	"go.temporal.io/server/common/clock"
)

type (
	// loop is the discrete-event loop. Events at the same time run in the order they were
	// scheduled.
	loop struct {
		now    time.Time
		seq    uint64
		events []*event // a binary min-heap by (at, seq)
	}

	event struct {
		at  time.Time
		seq uint64
		fn  func()
	}
)

func newLoop(start time.Time) *loop {
	return &loop{now: start}
}

// at schedules fn at t, or now if t has passed.
func (l *loop) at(t time.Time, fn func()) *event {
	if t.Before(l.now) {
		t = l.now
	}
	l.seq++
	e := &event{at: t, seq: l.seq, fn: fn}
	l.push(e)
	return e
}

func (l *loop) after(d time.Duration, fn func()) *event {
	return l.at(l.now.Add(d), fn)
}

// soon schedules fn now, after the events already scheduled for now, so a component hands
// work to another without running it inside its own call.
func (l *loop) soon(fn func()) {
	l.at(l.now, fn)
}

// cancel keeps a scheduled event from running. It's a no-op on nil or on an event that ran.
func (e *event) cancel() {
	if e != nil {
		e.fn = nil
	}
}

// run runs the events scheduled up to until, and leaves the clock at until.
func (l *loop) run(until time.Time) {
	for len(l.events) > 0 && !l.events[0].at.After(until) {
		e := l.pop()
		l.now = e.at
		if fn := e.fn; fn != nil {
			e.fn = nil
			fn()
		}
	}
	l.now = until
}

func (l *loop) less(i, j int) bool {
	a, b := l.events[i], l.events[j]
	if a.at.Equal(b.at) {
		return a.seq < b.seq
	}
	return a.at.Before(b.at)
}

func (l *loop) push(e *event) {
	l.events = append(l.events, e)
	for i := len(l.events) - 1; i > 0; {
		parent := (i - 1) / 2
		if !l.less(i, parent) {
			break
		}
		l.events[i], l.events[parent] = l.events[parent], l.events[i]
		i = parent
	}
}

func (l *loop) pop() *event {
	top := l.events[0]
	last := len(l.events) - 1
	l.events[0] = l.events[last]
	l.events[last] = nil
	l.events = l.events[:last]
	for i := 0; ; {
		smallest, left, right := i, 2*i+1, 2*i+2
		if left < last && l.less(left, smallest) {
			smallest = left
		}
		if right < last && l.less(right, smallest) {
			smallest = right
		}
		if smallest == i {
			break
		}
		l.events[i], l.events[smallest] = l.events[smallest], l.events[i]
		i = smallest
	}
	return top
}

// simClock is the loop's time as a clock.TimeSource, for the real code the simulation runs.
type simClock struct{ l *loop }

var _ clock.TimeSource = simClock{}

func (c simClock) Now() time.Time { return c.l.now }

func (c simClock) Since(t time.Time) time.Duration { return c.l.now.Sub(t) }

func (c simClock) AfterFunc(d time.Duration, f func()) clock.Timer {
	t := &simTimer{l: c.l, fn: f}
	t.Reset(d)
	return t
}

func (c simClock) NewTimer(d time.Duration) (<-chan time.Time, clock.Timer) {
	ch := make(chan time.Time, 1)
	t := &simTimer{l: c.l}
	t.fn = func() {
		select {
		case ch <- c.l.now:
		default:
		}
	}
	t.Reset(d)
	return ch, t
}

// simTimer is a timer on the loop.
type simTimer struct {
	l  *loop
	fn func()
	e  *event
}

func (t *simTimer) Reset(d time.Duration) bool {
	active := t.Stop()
	t.e = t.l.after(d, t.fn)
	return active
}

func (t *simTimer) Stop() bool {
	active := t.e != nil && t.e.fn != nil
	t.e.cancel()
	return active
}

// randoms returns a random stream that depends only on the seed and the stream's name, so
// adding draws to one part of the model doesn't move another's.
func randoms(seed uint64, stream string) *rand.Rand {
	h := fnv.New64a()
	_, _ = h.Write([]byte(stream))
	return rand.New(rand.NewPCG(seed, h.Sum64()))
}

// jittered spreads d by up to frac either way.
func jittered(r *rand.Rand, d time.Duration, frac float64) time.Duration {
	if frac <= 0 {
		return d
	}
	return time.Duration(float64(d) * (1 + frac*(2*r.Float64()-1)))
}

// between returns a duration drawn uniformly from [lo, hi].
func between(r *rand.Rand, lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(r.Int64N(int64(hi-lo)+1))
}

// fifo is a first-in, first-out queue.
type fifo[T any] struct {
	items []T
	head  int
}

func (q *fifo[T]) push(v T) { q.items = append(q.items, v) }

func (q *fifo[T]) len() int { return len(q.items) - q.head }

func (q *fifo[T]) pop() (T, bool) {
	var zero T
	if q.head == len(q.items) {
		return zero, false
	}
	v := q.items[q.head]
	q.items[q.head] = zero
	q.head++
	if q.head > 1024 && q.head*2 > len(q.items) {
		n := copy(q.items, q.items[q.head:])
		clear(q.items[n:])
		q.items = q.items[:n]
		q.head = 0
	}
	return v, true
}
