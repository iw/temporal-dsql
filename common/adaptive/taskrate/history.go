package taskrate

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/signals"
	"go.temporal.io/server/common/tasks"
)

// Name is the controller's name in its decisions.
const Name = "task_scheduler_rate"

type (
	// Settings is the controller's dynamic config. Bounds are shares of the host's static
	// persistence rate, which is also the scheduler's fallback rate.
	Settings struct {
		// Mode is "disabled", "shadow" or "enabled", and RolloutPercent the share of hosts
		// that apply an enabled Mode.
		Mode           string
		RolloutPercent int
		MinMultiplier  float64
		MaxStepFrac    float64
		Headroom       float64
		// MinSamples is the calls a step needs to measure the mix in full. A step with fewer
		// moves the mix by its calls' share of MinSamples, once a step has measured one.
		MinSamples int
		// Deadband keeps the mix of task attempts and calls that the rate was set from
		// while the measured mix stays within this share of it.
		Deadband float64
		// DecisionHold is how recent the capacity controller's decision must be for its
		// rate to count. Without one, the controller couples to the static rate.
		DecisionHold time.Duration
		// CoupledRetry, when set, is how soon history's rescheduler retries after the
		// controller's buckets refuse a task below the static rate, jittered by half either
		// way, in place of its 2s wait after any refusal. It's 200ms by default; at 0 it
		// waits 2s.
		CoupledRetry time.Duration
	}

	controller struct {
		settings func() Settings
		law      *Controller
	}

	// applier is the driven rate, as signals.Driven gives it.
	applier interface {
		Applying() (float64, bool)
	}

	// RefusalRecorder receives the tasks the scheduler's coupled limit holds back, as the
	// persistence health aggregator counts them for the capacity controller.
	RefusalRecorder interface {
		RecordCoupledRefusal(headroom float64)
	}

	// schedulerLimiter limits history's task scheduler to the controller's rate while it's
	// enabled, on top of the scheduler's own limiter. Otherwise it's the scheduler's own.
	// While the controller's rate is below the static rate, it records each task the
	// controller's buckets hold back, and when it last held one.
	schedulerLimiter struct {
		original quotas.RequestRateLimiter
		coupled  quotas.RequestRateLimiter // the buckets' priority cascade, then original
		driven   applier
		rate     atomic.Uint64 // the buckets' rate, as math.Float64bits
		static   func() float64
		settings func() Settings
		refusals RefusalRecorder
		heldAt   atomic.Int64 // when the buckets last held a task back below the static rate, in Unix ns

		mu      sync.Mutex // guards the buckets' updates
		buckets []*quotas.RateLimiterImpl
	}

	// heldBack is the buckets' priority cascade, which calls held for each task it holds
	// back. The multi limiter around it reserves on it whether the scheduler asks it to
	// allow a task or to reserve for one, so a reservation that isn't OK, or that would
	// wait, holds a task back.
	heldBack struct {
		quotas.RequestRateLimiter
		held func(now time.Time)
	}
)

// DefaultSettings leaves the controller off.
var DefaultSettings = Settings{
	Mode:           adaptive.ModeNameDisabled,
	RolloutPercent: 100,
	MinMultiplier:  0.02,
	MaxStepFrac:    0.25,
	Headroom:       0.9,
	MinSamples:     100,
	Deadband:       0.1,
	DecisionHold:   10 * time.Second,
	CoupledRetry:   200 * time.Millisecond,
}

// Config is the controller's dynamic config setting.
var Config = dynamicconfig.NewGlobalTypedSetting(
	"history.taskSchedulerRateController",
	DefaultSettings,
	`history.taskSchedulerRateController sets history's task scheduler rate from the persistence rate:
the capacity controller's while it's enabled, otherwise the static limit, times Headroom, divided by
the persistence calls each task attempt makes. It keeps the mix it was set from while the measured
mix stays within Deadband of it, and a step with fewer than MinSamples calls moves the mix by their
share of MinSamples. The rate stays between MinMultiplier of the static persistence rate
and the scheduler's fallback, that rate. It acts only while
history.taskSchedulerEnableRateLimiter is set and its shadow mode is off. Mode is disabled, shadow or
enabled. CoupledRetry, 200ms by default, is how soon history's rescheduler retries after the
controller's limit refuses a task below the static rate, in place of its 2s wait after any
refusal; 0 keeps the 2s wait.`,
)

// NewSchedulerLimiter couples limiter, history's task scheduler rate limiter, to
// persistence. persistenceRate is the host's static persistence rate. While the coupled
// rate is below it, refusals records the tasks the coupling holds back.
func NewSchedulerLimiter(
	limiter quotas.RequestRateLimiter,
	plane *signals.Plane,
	collection *dynamicconfig.Collection,
	persistenceRate func() float64,
	refusals RefusalRecorder,
	handler metrics.Handler,
) quotas.RequestRateLimiter {
	driven := DriveRate(plane, collection, persistenceRate, handler)
	return newSchedulerLimiter(limiter, driven, persistenceRate, Config.Get(collection), refusals)
}

// DriveRate drives the scheduler's rate from the plane's snapshots, for a limiter that
// reads it with Applying, as NewSchedulerLimiter's does. persistenceRate is the host's
// static persistence rate.
func DriveRate(
	plane *signals.Plane,
	collection *dynamicconfig.Collection,
	persistenceRate func() float64,
	handler metrics.Handler,
) *signals.Driven[float64] {
	ctrl := &controller{settings: Config.Get(collection), law: New(0)}
	return signals.DriveFunc(plane, persistenceRate, ctrl, adaptive.Frozen.Get(collection), adaptive.HostKey(), handler)
}

func newSchedulerLimiter(
	limiter quotas.RequestRateLimiter,
	driven applier,
	static func() float64,
	settings func() Settings,
	refusals RefusalRecorder,
) *schedulerLimiter {
	l := &schedulerLimiter{original: limiter, driven: driven, static: static, settings: settings, refusals: refusals}
	rateLimiters := make(map[int]quotas.RequestRateLimiter, len(tasks.PriorityName))
	for priority := range tasks.PriorityName {
		bucket := quotas.NewRateLimiter(1, 1)
		l.buckets = append(l.buckets, bucket)
		rateLimiters[int(priority)] = quotas.NewRequestRateLimiterAdapter(bucket)
	}
	l.coupled = quotas.NewMultiRequestRateLimiter(
		&heldBack{RequestRateLimiter: quotas.NewPriorityRateLimiter(requestPriority, rateLimiters), held: l.held},
		limiter,
	)
	return l
}

// requestPriority maps a task's request to its priority, as the scheduler's own limiter
// does: the request's caller type carries the task's priority.
func requestPriority(req quotas.Request) int {
	if priority, ok := tasks.CallerTypeToPriority[req.CallerType]; ok {
		return int(priority)
	}
	return int(tasks.PriorityPreemptable)
}

func (l *schedulerLimiter) Allow(now time.Time, request quotas.Request) bool {
	return l.limiter().Allow(now, request)
}

func (l *schedulerLimiter) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	return l.limiter().Reserve(now, request)
}

func (l *schedulerLimiter) Wait(ctx context.Context, request quotas.Request) error {
	return l.limiter().Wait(ctx, request)
}

func (l *schedulerLimiter) limiter() quotas.RequestRateLimiter {
	rate, on := l.driven.Applying()
	if !on || rate <= 0 {
		return l.original
	}
	if bits := math.Float64bits(rate); l.rate.Load() != bits {
		l.mu.Lock()
		for _, bucket := range l.buckets {
			bucket.SetRateBurst(rate, max(1, int(2*rate)))
		}
		l.rate.Store(bits)
		l.mu.Unlock()
	}
	return l.coupled
}

// held records a task the buckets held back, while their rate is below the static rate.
// At the static rate, the controller's ceiling, a higher persistence rate wouldn't let more
// tasks through, and the buckets refuse what the scheduler's own limit would.
func (l *schedulerLimiter) held(now time.Time) {
	if math.Float64frombits(l.rate.Load()) >= l.static() {
		return
	}
	l.heldAt.Store(now.UnixNano())
	if l.refusals != nil {
		l.refusals.RecordCoupledRefusal(l.settings().Headroom)
	}
}

// SubmitRetry returns CoupledRetry, and true, when it's set and the buckets held a task
// back at or after now, below the static rate. History's rescheduler asks after the
// scheduler refuses a task, so it retries a task the coupling refused while the buckets
// refill, instead of after its wait for a busy scheduler.
func (l *schedulerLimiter) SubmitRetry(now time.Time) (time.Duration, bool) {
	retry := l.settings().CoupledRetry
	if retry <= 0 || l.heldAt.Load() < now.UnixNano() {
		return 0, false
	}
	return retry, true
}

func (h *heldBack) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	r := h.RequestRateLimiter.Reserve(now, request)
	if !r.OK() || r.DelayFrom(now) > 0 {
		h.held(now)
	}
	return r
}

func (c *controller) Name() string {
	return Name
}

func (c *controller) Envelope(static float64) adaptive.Envelope[float64] {
	s := c.settings()
	return adaptive.Envelope[float64]{
		Mode:           s.Mode,
		RolloutPercent: s.RolloutPercent,
		Min:            s.MinMultiplier * static,
		Max:            static,
		MaxStepFrac:    s.MaxStepFrac,
	}
}

func (c *controller) Reset(rate float64) {
	c.law.Reset(rate)
}

func (c *controller) Step(snap *signals.Snapshot, env adaptive.Envelope[float64], static float64) (float64, adaptive.Reason, any) {
	s := c.settings()
	params := Params{Min: env.Min, Max: env.Max, Headroom: s.Headroom, MinSamples: s.MinSamples, Deadband: s.Deadband}
	in := Signals{
		PersistenceRate: persistenceRate(snap, static, s.DecisionHold),
		Attempts:        snap.Persistence.ScheduleSamples,
		Calls:           snap.Persistence.Samples + snap.Persistence.Rejected,
	}
	rate, reason := c.law.Step(params, in)
	return rate, adaptive.Reason(reason), Detail{Params: params, Signals: in, MaxStepFrac: env.MaxStepFrac}
}

// persistenceRate is the capacity controller's rate while it's enabled and its decision
// is recent, otherwise static.
func persistenceRate(snap *signals.Snapshot, static float64, hold time.Duration) float64 {
	if d, ok := snap.Decisions[capacity.Name]; ok && d.Mode == adaptive.ModeOn && snap.Time.Sub(d.Time) <= hold {
		return d.Applied
	}
	return static
}
