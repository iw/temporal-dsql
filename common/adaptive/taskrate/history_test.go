package taskrate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/signals"
	"go.temporal.io/server/common/tasks"
)

func TestPersistenceRateIsTheCapacityControllersWhileItsEnabled(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	decision := func(mode adaptive.Mode, age time.Duration) map[string]adaptive.Decision {
		return map[string]adaptive.Decision{capacity.Name: {Controller: capacity.Name, Time: now.Add(-age), Mode: mode, Applied: 2500}}
	}
	for name, tc := range map[string]struct {
		decisions map[string]adaptive.Decision
		want      float64
	}{
		"no capacity controller":     {want: 9000},
		"enabled":                    {decisions: decision(adaptive.ModeOn, time.Second), want: 2500},
		"in shadow":                  {decisions: decision(adaptive.ModeShadow, time.Second), want: 9000},
		"enabled, but long ago":      {decisions: decision(adaptive.ModeOn, time.Minute), want: 9000},
		"enabled, at the hold's end": {decisions: decision(adaptive.ModeOn, 10*time.Second), want: 2500},
	} {
		t.Run(name, func(t *testing.T) {
			snap := &signals.Snapshot{Time: now, Decisions: tc.decisions}
			require.InDelta(t, tc.want, persistenceRate(snap, 9000, DefaultSettings.DecisionHold), 1e-9)
		})
	}
}

func TestControllerStepsOnThePersistenceSnapshot(t *testing.T) {
	settings := DefaultSettings
	c := &controller{settings: func() Settings { return settings }, law: New(0)}
	require.Equal(t, Name, c.Name())

	env := c.Envelope(9000)
	require.Equal(t, adaptive.Envelope[float64]{Mode: adaptive.ModeNameDisabled, RolloutPercent: 100, Min: 180, Max: 9000, MaxStepFrac: 0.25}, env)

	c.Reset(9000)
	snap := &signals.Snapshot{
		Time:        time.Unix(1_700_000_000, 0),
		Persistence: persistence.HealthSnapshot{Samples: 900, Rejected: 100, ScheduleSamples: 200},
	}
	rate, reason, detail := c.Step(snap, env, 9000)
	require.InDelta(t, 1620, rate, 1e-9, "9000 x 0.9 x 200/1000")
	require.Equal(t, adaptive.Reason(ReasonCoupled), reason)
	require.Equal(t, stepDetail{
		Params:  Params{Min: 180, Max: 9000, Headroom: 0.9, MinSamples: 100},
		Signals: Signals{PersistenceRate: 9000, Attempts: 200, Calls: 1000},
	}, detail)
}

// fixedRate is a driven rate.
type fixedRate struct {
	rate float64
	on   bool
}

func (f *fixedRate) Applying() (float64, bool) { return f.rate, f.on }

// countingLimiter stands in for the scheduler's own limiter, and allows everything.
type countingLimiter struct {
	quotas.RequestRateLimiter
	calls int
}

func newCountingLimiter() *countingLimiter {
	return &countingLimiter{RequestRateLimiter: quotas.NoopRequestRateLimiter}
}

func (c *countingLimiter) Allow(now time.Time, request quotas.Request) bool {
	c.calls++
	return c.RequestRateLimiter.Allow(now, request)
}

func (c *countingLimiter) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	c.calls++
	return c.RequestRateLimiter.Reserve(now, request)
}

func taskRequest(priority tasks.Priority) quotas.Request {
	return quotas.NewRequest("TransferActivityTask", 1, "namespace", tasks.PriorityToCallerType[priority], 0, "")
}

func TestSchedulerLimiterIsTheSchedulersOwnWhileOff(t *testing.T) {
	original := newCountingLimiter()
	rate := &fixedRate{rate: 0.001}
	l := newSchedulerLimiter(original, rate)
	now := time.Now()
	for range 10 {
		require.True(t, l.Allow(now, taskRequest(tasks.PriorityLow)))
	}
	require.Equal(t, 10, original.calls)

	// Enabled at no rate, as with an unlimited persistence rate, it stays out of the way.
	rate.on, rate.rate = true, 0
	require.True(t, l.Allow(now, taskRequest(tasks.PriorityLow)))
}

func TestSchedulerLimiterHoldsTasksToTheControllersRate(t *testing.T) {
	original := newCountingLimiter()
	l := newSchedulerLimiter(original, &fixedRate{rate: 10, on: true})
	now := time.Now()

	// A bucket set to a new rate starts with one token and fills at the rate.
	allowed := 0
	for range 100 {
		if l.Allow(now, taskRequest(tasks.PriorityLow)) {
			allowed++
		}
	}
	require.Equal(t, 1, allowed)
	require.Equal(t, 1, original.calls, "admitted tasks still pass the scheduler's own limiter")

	// A high-priority task has its own bucket.
	require.True(t, l.Allow(now, taskRequest(tasks.PriorityHigh)))
}

func TestRequestPriorityIsTheTasks(t *testing.T) {
	require.Equal(t, int(tasks.PriorityHigh), requestPriority(taskRequest(tasks.PriorityHigh)))
	require.Equal(t, int(tasks.PriorityLow), requestPriority(taskRequest(tasks.PriorityLow)))
	require.Equal(t, int(tasks.PriorityPreemptable), requestPriority(quotas.NewRequest("api", 1, "namespace", headers.CallerTypeOperator, 0, "")))
}
