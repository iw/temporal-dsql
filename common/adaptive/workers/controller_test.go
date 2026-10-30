package workers_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive/workers"
)

var params = workers.Params{
	Min:         256,
	Max:         1024,
	TargetDelay: 100 * time.Millisecond,
	GrowFrac:    0.25,
	ShrinkFrac:  0.05,
	IdleSteps:   3,
	MinSamples:  20,
	Cooldown:    3 * time.Second,
}

const static = 512

// stepper steps a controller once a second.
type stepper struct {
	ctrl *workers.Controller
	now  time.Time
}

func newStepper(count int) *stepper {
	return &stepper{ctrl: workers.New(count), now: time.Unix(1_700_000_000, 0)}
}

func (s *stepper) step(delay time.Duration, samples int, backingOff bool) (int, workers.Reason) {
	s.now = s.now.Add(time.Second)
	return s.ctrl.Step(params, static, workers.Signals{Now: s.now, Delay: delay, Samples: samples, BackingOff: backingOff})
}

func TestWorkersGrowWhileTasksWaitOncePerCooldown(t *testing.T) {
	s := newStepper(static)
	count, reason := s.step(300*time.Millisecond, 100, false)
	require.Equal(t, 640, count)
	require.Equal(t, workers.ReasonGrow, reason)

	for range 2 {
		count, reason = s.step(300*time.Millisecond, 100, false)
		require.Equal(t, 640, count)
		require.Equal(t, workers.ReasonCooldown, reason, "the delay still covers the pool before it grew")
	}
	count, reason = s.step(300*time.Millisecond, 100, false)
	require.Equal(t, 800, count)
	require.Equal(t, workers.ReasonGrow, reason)

	for range 20 {
		count, _ = s.step(300*time.Millisecond, 100, false)
	}
	require.Equal(t, params.Max, count)
}

func TestWorkersHoldWhilePersistenceBacksOff(t *testing.T) {
	s := newStepper(static)
	count, reason := s.step(300*time.Millisecond, 100, true)
	require.Equal(t, static, count)
	require.Equal(t, workers.ReasonBackoff, reason)

	// A pool that grew returns to its static size, but not below.
	s = newStepper(900)
	count, reason = s.step(300*time.Millisecond, 100, true)
	require.Equal(t, 675, count)
	require.Equal(t, workers.ReasonBackoff, reason)
	for range 10 {
		count, _ = s.step(300*time.Millisecond, 100, true)
	}
	require.Equal(t, static, count)
}

func TestWorkersShrinkSlowlyWhileIdle(t *testing.T) {
	s := newStepper(static)
	for range 2 {
		count, reason := s.step(time.Millisecond, 100, false)
		require.Equal(t, static, count)
		require.Equal(t, workers.ReasonIdle, reason)
	}
	count, reason := s.step(time.Millisecond, 100, false)
	require.Equal(t, 487, count, "5% after three idle steps")
	require.Equal(t, workers.ReasonShrink, reason)

	// Too few delays to read the pool as busy counts as idle too, down to the floor.
	for range 100 {
		count, _ = s.step(0, 3, false)
	}
	require.Equal(t, params.Min, count)
}

func TestWorkersHoldBetweenIdleAndTarget(t *testing.T) {
	s := newStepper(static)
	s.step(time.Millisecond, 100, false)
	s.step(time.Millisecond, 100, false)
	count, reason := s.step(50*time.Millisecond, 100, false)
	require.Equal(t, static, count)
	require.Equal(t, workers.ReasonSteady, reason)

	// The steady step restarted the idle run.
	for range 2 {
		_, reason = s.step(time.Millisecond, 100, false)
		require.Equal(t, workers.ReasonIdle, reason)
	}
}

func TestWorkersResetClearsTheLawsState(t *testing.T) {
	s := newStepper(static)
	s.step(300*time.Millisecond, 100, false)
	s.ctrl.Reset(300)
	count, reason := s.step(300*time.Millisecond, 100, false)
	require.Equal(t, 375, count, "no cooldown left from before the reset")
	require.Equal(t, workers.ReasonGrow, reason)
}
