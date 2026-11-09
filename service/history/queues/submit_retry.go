package queues

import (
	"time"

	"go.temporal.io/server/common/backoff"
)

// SubmitRetryHinter is a Scheduler, or its rate limiter, that can say how soon to retry a
// task TrySubmit refused, when that's sooner than the rescheduler's wait after any
// refusal. The adaptive task rate's limiter gives one for the refusals of its coupling to
// persistence, whose buckets refill well within that wait.
type SubmitRetryHinter interface {
	// SubmitRetry returns how soon to retry after a TrySubmit at or after now was refused,
	// and false to wait as after any refusal.
	SubmitRetry(now time.Time) (time.Duration, bool)
}

// retryHinterOf is the scheduler's rate limiter as a SubmitRetryHinter, or nil if it gives
// no hints.
func retryHinterOf(limiter SchedulerRateLimiter) SubmitRetryHinter {
	if hinter, ok := limiter.(SubmitRetryHinter); ok {
		return hinter
	}
	return nil
}

// SubmitRetry passes on its rate limiter's hint while the limiter throttles: while it's
// enabled and out of shadow mode.
func (s *rateLimitedSchedulerImpl) SubmitRetry(now time.Time) (time.Duration, bool) {
	if s.retryHinter == nil || !s.options.Enabled() || s.options.EnableShadowMode() {
		return 0, false
	}
	return s.retryHinter.SubmitRetry(now)
}

// retryAfterRefusal is how long to wait after the scheduler refused a task in a reschedule
// at now: its hint, if it gives one, otherwise the wait for a busy scheduler. Both are
// jittered by half either way.
func (r *reschedulerImpl) retryAfterRefusal(now time.Time) time.Duration {
	if hinter, ok := r.scheduler.(SubmitRetryHinter); ok {
		if retry, ok := hinter.SubmitRetry(now); ok {
			return backoff.Jitter(retry, taskChanFullBackoffJitterCoefficient)
		}
	}
	return backoff.Jitter(taskChanFullBackoff, taskChanFullBackoffJitterCoefficient)
}
