package taskrate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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
	require.Equal(t, Detail{
		Params:      Params{Min: 180, Max: 9000, Headroom: 0.9, MinSamples: 100, Deadband: 0.1},
		Signals:     Signals{PersistenceRate: 9000, Attempts: 200, Calls: 1000},
		MaxStepFrac: 0.25,
	}, detail)
}

func TestDriveRateStepsOnThePlane(t *testing.T) {
	settings := DefaultSettings
	settings.Mode = adaptive.ModeNameEnabled
	settings.MaxStepFrac = 0
	collection := dynamicconfig.NewCollection(dynamicconfig.StaticClient{Config.Key(): settings}, log.NewNoopLogger())
	timeSource := clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0))
	source := &snapshotSource{Snapshot: signals.Snapshot{
		Persistence: persistence.HealthSnapshot{Samples: 900, Rejected: 100, ScheduleSamples: 200},
	}}
	plane := signals.NewPlane(
		func() signals.Settings { return signals.Settings{Enabled: true, Interval: time.Second, Windows: 3} },
		source, nil, timeSource, signals.Header{}, log.NewNoopLogger(),
	)
	defer plane.Stop()
	driven := DriveRate(plane, collection, func() float64 { return 9000 }, metrics.NoopMetricsHandler)
	defer driven.Stop()

	_, on := driven.Applying()
	require.False(t, on, "the rate applies from the controller's first step")

	plane.RecordDecision(adaptive.Decision{Controller: capacity.Name, Time: timeSource.Now(), Mode: adaptive.ModeOn, Applied: 2000})
	plane.Tick(timeSource.Now())
	rate, on := driven.Applying()
	require.True(t, on)
	require.InDelta(t, 360, rate, 1e-9, "2000 x 0.9 x 200/1000")
}

// snapshotSource is a host whose signals are a fixed snapshot.
type snapshotSource struct {
	signals.Snapshot
}

func (s *snapshotSource) Read(time.Time, int) signals.Snapshot { return s.Snapshot }

func (s *snapshotSource) Idle() {}

// staticRate is a static persistence rate.
func staticRate(rate float64) func() float64 {
	return func() float64 { return rate }
}

func defaultSettings() Settings { return DefaultSettings }

// recordedRefusals stands in for the persistence health aggregator.
type recordedRefusals struct {
	headrooms []float64
}

func (r *recordedRefusals) RecordCoupledRefusal(headroom float64) {
	r.headrooms = append(r.headrooms, headroom)
}

// refusingLimiter stands in for a scheduler's own limiter that refuses every task.
type refusingLimiter struct {
	quotas.RequestRateLimiter
}

func (refusingLimiter) Allow(time.Time, quotas.Request) bool { return false }

func (refusingLimiter) Reserve(time.Time, quotas.Request) quotas.Reservation {
	return quotas.NewMultiReservation(false, nil)
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
	l := newSchedulerLimiter(original, rate, staticRate(9000), defaultSettings, nil)
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
	l := newSchedulerLimiter(original, &fixedRate{rate: 10, on: true}, staticRate(9000), defaultSettings, nil)
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

func TestSchedulerLimiterRecordsTheTasksItsBucketsHoldBack(t *testing.T) {
	refusals := &recordedRefusals{}
	settings := DefaultSettings
	settings.Headroom = 0.8
	l := newSchedulerLimiter(newCountingLimiter(), &fixedRate{rate: 10, on: true}, staticRate(100), func() Settings { return settings }, refusals)
	now := time.Now()

	allowed := 0
	for range 20 {
		if l.Allow(now, taskRequest(tasks.PriorityLow)) {
			allowed++
		}
	}
	require.Equal(t, 1, allowed)
	require.Len(t, refusals.headrooms, 19)
	require.InDelta(t, 0.8, refusals.headrooms[0], 1e-9, "the Headroom the rate coupled at")

	// The scheduler's Submit reserves, and waits for the reservation: a task that has to
	// wait is held back too.
	r := l.Reserve(now, taskRequest(tasks.PriorityLow))
	require.True(t, r.OK())
	require.Positive(t, r.DelayFrom(now))
	require.Len(t, refusals.headrooms, 20)
}

// reservingRefusals reserves for another task the first time it records a refusal, as
// another shard's submitter might while the refusal is made.
type reservingRefusals struct {
	l     *schedulerLimiter
	now   time.Time
	delay time.Duration
	done  bool
}

func (r *reservingRefusals) RecordCoupledRefusal(float64) {
	if r.done {
		return
	}
	r.done = true
	r.delay = r.l.Reserve(r.now, taskRequest(tasks.PriorityLow)).DelayFrom(r.now)
}

// A refused task reserves nothing, so a task that reserves meanwhile waits only for its
// own token. Had the refusal reserved and then canceled, its token would have stayed
// spent, and the other task would wait for both.
func TestSchedulerLimiterRefusesWithoutReserving(t *testing.T) {
	now := time.Now()
	refusals := &reservingRefusals{now: now}
	l := newSchedulerLimiter(newCountingLimiter(), &fixedRate{rate: 10, on: true}, staticRate(100), defaultSettings, refusals)
	refusals.l = l

	require.True(t, l.Allow(now, taskRequest(tasks.PriorityLow)))
	require.False(t, l.Allow(now, taskRequest(tasks.PriorityLow)))
	require.True(t, refusals.done)
	require.Equal(t, 100*time.Millisecond, refusals.delay, "the next token, at 10 a second")
}

func TestSchedulerLimiterRecordsOnlyItsBucketsRefusalsBelowTheStaticRate(t *testing.T) {
	now := time.Now()
	refuse := func(l *schedulerLimiter) {
		for range 20 {
			l.Allow(now, taskRequest(tasks.PriorityLow))
		}
	}
	for name, l := range map[string]func(*recordedRefusals) *schedulerLimiter{
		// At the static rate, the controller's ceiling, a higher persistence rate wouldn't
		// let more tasks through.
		"at the static rate": func(r *recordedRefusals) *schedulerLimiter {
			return newSchedulerLimiter(newCountingLimiter(), &fixedRate{rate: 10, on: true}, staticRate(10), defaultSettings, r)
		},
		"the scheduler's own limit": func(r *recordedRefusals) *schedulerLimiter {
			return newSchedulerLimiter(refusingLimiter{}, &fixedRate{rate: 1000, on: true}, staticRate(9000), defaultSettings, r)
		},
		"the controller off": func(r *recordedRefusals) *schedulerLimiter {
			return newSchedulerLimiter(refusingLimiter{}, &fixedRate{rate: 10}, staticRate(9000), defaultSettings, r)
		},
	} {
		t.Run(name, func(t *testing.T) {
			refusals := &recordedRefusals{}
			l := l(refusals)
			refuse(l)
			require.False(t, l.Allow(now, taskRequest(tasks.PriorityLow)))
			require.Empty(t, refusals.headrooms)
		})
	}
}

func TestSchedulerLimiterHintsARetryAfterItsBucketsRefuse(t *testing.T) {
	settings := DefaultSettings
	l := newSchedulerLimiter(newCountingLimiter(), &fixedRate{rate: 10, on: true}, staticRate(100), func() Settings { return settings }, nil)
	now := time.Now()

	// Nothing refused yet.
	_, ok := l.SubmitRetry(now)
	require.False(t, ok)

	require.True(t, l.Allow(now, taskRequest(tasks.PriorityLow)))
	require.False(t, l.Allow(now, taskRequest(tasks.PriorityLow)))
	retry, ok := l.SubmitRetry(now)
	require.True(t, ok)
	require.Equal(t, 200*time.Millisecond, retry)

	// A refusal before the reschedule that asks isn't its own.
	_, ok = l.SubmitRetry(now.Add(time.Millisecond))
	require.False(t, ok)

	// 0 turns it off.
	settings.CoupledRetry = 0
	_, ok = l.SubmitRetry(now)
	require.False(t, ok)
}

func TestSchedulerLimiterHintsNoRetryForOtherRefusals(t *testing.T) {
	get := func() Settings { return DefaultSettings }
	now := time.Now()
	for name, l := range map[string]*schedulerLimiter{
		"at the static rate":        newSchedulerLimiter(newCountingLimiter(), &fixedRate{rate: 10, on: true}, staticRate(10), get, nil),
		"the scheduler's own limit": newSchedulerLimiter(refusingLimiter{}, &fixedRate{rate: 1000, on: true}, staticRate(9000), get, nil),
		"the controller off":        newSchedulerLimiter(refusingLimiter{}, &fixedRate{rate: 10}, staticRate(9000), get, nil),
	} {
		t.Run(name, func(t *testing.T) {
			for range 20 {
				l.Allow(now, taskRequest(tasks.PriorityLow))
			}
			require.False(t, l.Allow(now, taskRequest(tasks.PriorityLow)))
			_, ok := l.SubmitRetry(now)
			require.False(t, ok)
		})
	}
}
