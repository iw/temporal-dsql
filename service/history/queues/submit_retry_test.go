package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	ctasks "go.temporal.io/server/common/tasks"
	"go.uber.org/mock/gomock"
)

// refusingScheduler refuses every task, and hints a retry when hinted is set.
type refusingScheduler struct {
	Scheduler
	retry  time.Duration
	hinted bool
	asked  []time.Time
}

func (s *refusingScheduler) TaskChannelKeyFn() TaskChannelKeyFn {
	return func(Executable) TaskChannelKey { return TaskChannelKey{} }
}

func (s *refusingScheduler) TrySubmit(Executable) bool { return false }

func (s *refusingScheduler) SubmitRetry(now time.Time) (time.Duration, bool) {
	s.asked = append(s.asked, now)
	return s.retry, s.hinted
}

func TestRescheduleRetriesAsTheSchedulerHints(t *testing.T) {
	for name, tc := range map[string]struct {
		hinted bool
		// the latest and earliest the rescheduler retries after the refusal
		before, after time.Duration
	}{
		"with a hint":    {hinted: true, before: 300 * time.Millisecond, after: 100 * time.Millisecond},
		"without a hint": {before: 3 * time.Second, after: time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			now := time.Now()
			timeSource := clock.NewEventTimeSource().Update(now)
			scheduler := &refusingScheduler{retry: 200 * time.Millisecond, hinted: tc.hinted}
			r := NewRescheduler(scheduler, timeSource, log.NewTestLogger(), metrics.NoopMetricsHandler)
			defer r.timerGate.Close()

			executable := NewMockExecutable(ctrl)
			executable.EXPECT().State().Return(ctasks.TaskStatePending)
			executable.EXPECT().SetScheduledTime(now)
			r.Add(executable, now)
			<-r.timerGate.FireCh() // the task is due at once

			r.reschedule()
			require.Equal(t, 1, r.Len())
			require.Equal(t, []time.Time{now}, scheduler.asked)
			require.False(t, r.timerGate.FireAfter(now.Add(tc.before)))
			require.True(t, r.timerGate.FireAfter(now.Add(tc.after-time.Nanosecond)))
		})
	}
}

// hintingLimiter refuses every request, and hints a retry.
type hintingLimiter struct {
	quotas.RequestRateLimiter
}

func (hintingLimiter) Allow(time.Time, quotas.Request) bool { return false }

func (hintingLimiter) SubmitRetry(time.Time) (time.Duration, bool) {
	return 200 * time.Millisecond, true
}

func TestRateLimitedSchedulerPassesItsLimitersHintWhileItThrottles(t *testing.T) {
	for name, tc := range map[string]struct {
		limiter         SchedulerRateLimiter
		enabled, shadow bool
		startupDelay    time.Duration
		wantRetry       time.Duration
		wantHint        bool
	}{
		"throttling":                {limiter: hintingLimiter{}, enabled: true, wantRetry: 200 * time.Millisecond, wantHint: true},
		"after a startup delay":     {limiter: hintingLimiter{}, enabled: true, startupDelay: time.Minute, wantRetry: 200 * time.Millisecond, wantHint: true},
		"disabled":                  {limiter: hintingLimiter{}},
		"in shadow mode":            {limiter: hintingLimiter{}, enabled: true, shadow: true},
		"a limiter that gives none": {limiter: quotas.NoopRequestRateLimiter, enabled: true},
	} {
		t.Run(name, func(t *testing.T) {
			s := NewRateLimitedScheduler(
				nil,
				RateLimitedSchedulerOptions{
					Enabled:          dynamicconfig.GetBoolPropertyFn(tc.enabled),
					EnableShadowMode: dynamicconfig.GetBoolPropertyFn(tc.shadow),
					StartupDelay:     dynamicconfig.GetDurationPropertyFn(tc.startupDelay),
				},
				"active",
				nil,
				tc.limiter,
				clock.NewRealTimeSource(),
				nil,
				log.NewTestLogger(),
				metrics.NoopMetricsHandler,
			)
			hinter, ok := s.(SubmitRetryHinter)
			require.True(t, ok)
			retry, hinted := hinter.SubmitRetry(time.Now())
			require.Equal(t, tc.wantHint, hinted)
			require.Equal(t, tc.wantRetry, retry)
		})
	}
}
