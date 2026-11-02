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
		MinSamples     int
		// DecisionHold is how recent the capacity controller's decision must be for its
		// rate to count. Without one, the controller couples to the static rate.
		DecisionHold time.Duration
	}

	controller struct {
		settings func() Settings
		law      *Controller
	}

	// stepDetail is a step as its decision's detail, so a trace can replay it.
	stepDetail struct {
		Params  Params
		Signals Signals
	}

	// applier is the driven rate, as signals.Driven gives it.
	applier interface {
		Applying() (float64, bool)
	}

	// schedulerLimiter limits history's task scheduler to the controller's rate while it's
	// enabled, on top of the scheduler's own limiter. Otherwise it's the scheduler's own.
	schedulerLimiter struct {
		original quotas.RequestRateLimiter
		coupled  quotas.RequestRateLimiter // the buckets' priority cascade, then original
		driven   applier
		rate     atomic.Uint64 // the buckets' rate, as math.Float64bits

		mu      sync.Mutex // guards the buckets' updates
		buckets []*quotas.RateLimiterImpl
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
	DecisionHold:   10 * time.Second,
}

// Config is the controller's dynamic config setting.
var Config = dynamicconfig.NewGlobalTypedSetting(
	"history.taskSchedulerRateController",
	DefaultSettings,
	`history.taskSchedulerRateController sets history's task scheduler rate from the persistence rate:
the capacity controller's while it's enabled, otherwise the static limit, times Headroom, divided by
the persistence calls each task attempt makes. The rate stays between MinMultiplier of the static
persistence rate and the scheduler's fallback, that rate. It acts only while
history.taskSchedulerEnableRateLimiter is set and its shadow mode is off. Mode is disabled, shadow or
enabled.`,
)

// NewSchedulerLimiter couples limiter, history's task scheduler rate limiter, to
// persistence. persistenceRate is the host's static persistence rate.
func NewSchedulerLimiter(
	limiter quotas.RequestRateLimiter,
	plane *signals.Plane,
	collection *dynamicconfig.Collection,
	persistenceRate func() float64,
	handler metrics.Handler,
) quotas.RequestRateLimiter {
	ctrl := &controller{settings: Config.Get(collection), law: New(0)}
	driven := signals.DriveFunc(plane, persistenceRate, ctrl, adaptive.Frozen.Get(collection), adaptive.HostKey(), handler)
	return newSchedulerLimiter(limiter, driven)
}

func newSchedulerLimiter(limiter quotas.RequestRateLimiter, driven applier) *schedulerLimiter {
	l := &schedulerLimiter{original: limiter, driven: driven}
	rateLimiters := make(map[int]quotas.RequestRateLimiter, len(tasks.PriorityName))
	for priority := range tasks.PriorityName {
		bucket := quotas.NewRateLimiter(1, 1)
		l.buckets = append(l.buckets, bucket)
		rateLimiters[int(priority)] = quotas.NewRequestRateLimiterAdapter(bucket)
	}
	l.coupled = quotas.NewMultiRequestRateLimiter(
		quotas.NewPriorityRateLimiter(requestPriority, rateLimiters),
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
	params := Params{Min: env.Min, Max: env.Max, Headroom: s.Headroom, MinSamples: s.MinSamples}
	in := Signals{
		PersistenceRate: persistenceRate(snap, static, s.DecisionHold),
		Attempts:        snap.Persistence.ScheduleSamples,
		Calls:           snap.Persistence.Samples + snap.Persistence.Rejected,
	}
	rate, reason := c.law.Step(params, in)
	return rate, adaptive.Reason(reason), stepDetail{Params: params, Signals: in}
}

// persistenceRate is the capacity controller's rate while it's enabled and its decision
// is recent, otherwise static.
func persistenceRate(snap *signals.Snapshot, static float64, hold time.Duration) float64 {
	if d, ok := snap.Decisions[capacity.Name]; ok && d.Mode == adaptive.ModeOn && snap.Time.Sub(d.Time) <= hold {
		return d.Applied
	}
	return static
}
