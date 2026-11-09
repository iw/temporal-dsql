package adaptivesim

import (
	"container/heap"
	"math/rand/v2"
	"time"

	"go.temporal.io/server/common/adaptive/taskrate"
	"go.temporal.io/server/common/signals"
	"golang.org/x/time/rate"
)

const (
	// schedulerBurstRatio is the burst of the scheduler's own limiter, as a share of its
	// rate: quotas' default for incoming requests.
	schedulerBurstRatio = 2
	// rescheduleRetry is how long the rescheduler waits, jittered by half either way, when
	// the scheduler refuses a task it resubmits.
	rescheduleRetry = 2 * time.Second
)

type (
	// schedulerLimiter is history's task scheduler rate limiter, on simulated time: the
	// scheduler's own limit, at the static persistence rate, which history falls back to
	// without a scheduler rate of its own, and the task rate controller's on top while
	// it's enabled, as taskrate's schedulerLimiter holds it. A task takes a token from
	// both, or from neither. Every task has the same priority, so one bucket stands for
	// the controller's buckets. While the controller's rate is below the static rate, the
	// tasks its bucket refuses go to the health windows, as taskrate records them, and the
	// last one's time gives the rescheduler its hint, as taskrate's SubmitRetry does.
	schedulerLimiter struct {
		original *rate.Limiter
		coupled  *rate.Limiter
		driven   *signals.Driven[float64]
		rate     float64 // the coupled bucket's
		static   float64
		settings func() taskrate.Settings
		health   *health
		heldAt   time.Time // when the coupled bucket last refused a task below the static rate
	}

	// rescheduler is history's task rescheduler: the tasks the scheduler didn't take wait
	// for their reschedule time, then go to the scheduler again in time order. When the
	// scheduler refuses one, the rescheduler tries again after 1–3 s, or after the
	// scheduler's hint, jittered by half either way, when it gives one.
	rescheduler struct {
		l      *loop
		rng    *rand.Rand
		submit func(*task) bool
		retry  func(now time.Time) (time.Duration, bool)
		tasks  rescheduled
		seq    int64
		gate   *event
		gateAt time.Time
	}

	rescheduled []rescheduledTask

	rescheduledTask struct {
		t   *task
		at  time.Time
		seq int64
	}
)

func newSchedulerLimiter(
	static float64,
	driven *signals.Driven[float64],
	settings func() taskrate.Settings,
	h *health,
	now time.Time,
) *schedulerLimiter {
	s := &schedulerLimiter{
		original: rate.NewLimiter(rate.Limit(static), max(1, int(schedulerBurstRatio*static))),
		coupled:  rate.NewLimiter(1, 1),
		driven:   driven,
		static:   static,
		settings: settings,
		health:   h,
	}
	// The limiters start full at the time the host starts.
	s.original.AllowN(now, 0)
	s.coupled.AllowN(now, 0)
	return s
}

// allow is TrySubmit's rate limit: whether the scheduler takes a task now.
func (s *schedulerLimiter) allow(now time.Time) bool {
	r, on := s.driven.Applying()
	if !on || r <= 0 {
		return s.original.AllowN(now, 1)
	}
	if r != s.rate {
		s.coupled.SetLimitAt(now, rate.Limit(r))
		s.coupled.SetBurstAt(now, max(1, int(2*r)))
		s.rate = r
	}
	coupled := s.coupled.ReserveN(now, 1)
	if !coupled.OK() || coupled.DelayFrom(now) > 0 {
		coupled.CancelAt(now)
		if r < s.static {
			s.health.recordRefusal(now, s.settings().Headroom)
			s.heldAt = now
		}
		return false
	}
	original := s.original.ReserveN(now, 1)
	if !original.OK() || original.DelayFrom(now) > 0 {
		original.CancelAt(now)
		coupled.CancelAt(now)
		return false
	}
	return true
}

// submitRetry is taskrate's SubmitRetry: CoupledRetry, when it's set and the coupled
// bucket refused a task at or after now, below the static rate.
func (s *schedulerLimiter) submitRetry(now time.Time) (time.Duration, bool) {
	retry := s.settings().CoupledRetry
	if retry <= 0 || s.heldAt.Before(now) {
		return 0, false
	}
	return retry, true
}

func newRescheduler(
	l *loop,
	seed uint64,
	name string,
	submit func(*task) bool,
	retry func(now time.Time) (time.Duration, bool),
) *rescheduler {
	return &rescheduler{l: l, rng: randoms(seed, name+"/rescheduler"), submit: submit, retry: retry}
}

// add holds a task until at.
func (r *rescheduler) add(t *task, at time.Time) {
	heap.Push(&r.tasks, rescheduledTask{t: t, at: at, seq: r.seq})
	r.seq++
	r.wake(at)
}

// len is how many tasks the rescheduler holds.
func (r *rescheduler) len() int {
	return len(r.tasks)
}

// wake runs the rescheduler at at, unless it already runs sooner, as its timer gate does.
func (r *rescheduler) wake(at time.Time) {
	if r.gate != nil && !r.gateAt.After(at) {
		return
	}
	r.gate.cancel()
	r.gate, r.gateAt = r.l.at(at, r.fire), at
}

// fire resubmits the tasks whose time has come, until the scheduler refuses one.
func (r *rescheduler) fire() {
	r.gate = nil
	now := r.l.now
	for len(r.tasks) > 0 {
		next := r.tasks[0]
		if now.Before(next.at) {
			r.wake(next.at)
			return
		}
		if !r.submit(next.t) {
			wait := rescheduleRetry
			if hint, ok := r.retry(now); ok {
				wait = hint
			}
			r.wake(now.Add(jittered(r.rng, wait, 0.5)))
			return
		}
		heap.Pop(&r.tasks)
	}
}

func (q rescheduled) Len() int { return len(q) }

func (q rescheduled) Less(i, j int) bool {
	if !q[i].at.Equal(q[j].at) {
		return q[i].at.Before(q[j].at)
	}
	return q[i].seq < q[j].seq
}

func (q rescheduled) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *rescheduled) Push(x any) { *q = append(*q, x.(rescheduledTask)) }

func (q *rescheduled) Pop() any {
	old := *q
	last := old[len(old)-1]
	*q = old[:len(old)-1]
	return last
}
