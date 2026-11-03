package memory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/signals"
)

// fixedMultiplier is a driven multiplier.
type fixedMultiplier struct {
	multiplier float64
	on         bool
}

func (f *fixedMultiplier) Applying() (float64, bool) { return f.multiplier, f.on }

func count(n int) func() int { return func() int { return n } }

func TestPendingTaskLimitsScaleWhileEnabled(t *testing.T) {
	driven := &fixedMultiplier{multiplier: 0.25}
	limits := &QueueLimits{driven: driven, ctrl: newPendingController(func() Settings { return DefaultSettings })}
	maxCount, criticalCount := limits.Limits("transfer", count(10000), count(9000))
	require.Equal(t, 10000, maxCount())
	require.Equal(t, 9000, criticalCount())

	driven.on = true
	require.Equal(t, 2500, maxCount())
	require.Equal(t, 2250, criticalCount())

	// A scaled count rounds to the nearest task, and stays at least 1. A count of 0 or
	// less, which isn't a limit, stays.
	driven.multiplier = minCriticalCount / 9000.0
	require.Equal(t, minCriticalCount, criticalCount())
	driven.multiplier = 1e-6
	require.Equal(t, 1, maxCount())
	_, noLimit := limits.Limits("timer", count(10000), count(0))
	require.Equal(t, 0, noLimit())
}

func TestPendingMultiplierKeepsCriticalCountsAboveTheReadersFloor(t *testing.T) {
	ctrl := newPendingController(func() Settings { return DefaultSettings })
	limits := &QueueLimits{driven: &fixedMultiplier{}, ctrl: ctrl}
	require.InDelta(t, 0.05, ctrl.Envelope(1).Min, 1e-9, "MinMultiplier, with no queues yet")

	// The floor follows the smallest critical count, and ignores a count that isn't a
	// limit.
	limits.Limits("transfer", count(10000), count(9000))
	limits.Limits("outbound", count(10000), count(4500))
	limits.Limits("archival", count(10000), count(0))
	require.InDelta(t, minCriticalCount/4500.0, ctrl.Envelope(1).Min, 1e-9)

	// With a queue's critical count set below the floor, the queues keep their settings.
	limits.Limits("timer", count(1000), count(900))
	require.InDelta(t, 1.0, ctrl.Envelope(1).Min, 1e-9)
}

func TestPendingControllerStepsOnTheHostsQueues(t *testing.T) {
	settings := DefaultSettings
	ctrl := newPendingController(func() Settings { return settings })
	limits := &QueueLimits{driven: &fixedMultiplier{}, ctrl: ctrl}
	require.Equal(t, "memory_budget_pending_tasks", ctrl.Name())

	env := ctrl.Envelope(1)
	require.Equal(t, adaptive.Envelope[float64]{
		Mode:           adaptive.ModeNameDisabled,
		RolloutPercent: 100,
		Min:            0.05,
		Max:            1,
		MaxStepFrac:    0.25,
	}, env)

	snap := &signals.Snapshot{
		Time:    time.Unix(1_700_000_000, 0),
		Memory:  signals.Memory{GoLimit: 256 * mib},
		Runtime: signals.Runtime{HeapGoal: 100 * mib, HeapLive: 50 * mib},
	}

	// Until it can count the host's shards, the queues hold nothing it can budget for.
	limits.Limits("transfer", count(10000), count(9000))
	multiplier, reason, _ := ctrl.Step(snap, env, 1)
	require.InDelta(t, 1.0, multiplier, 1e-9)
	require.Equal(t, adaptive.Reason(ReasonSteady), reason)

	// Four shards' transfer and outbound queues may hold 60,000 tasks of 2 KiB, which is
	// 117 MiB, and 0.2 of 256 MiB holds 0.437 of that.
	limits.SetShardCount(count(4))
	limits.Limits("outbound", count(5000), count(4500))
	multiplier, reason, detail := ctrl.Step(snap, env, 1)
	require.InDelta(t, 0.2*256*mib/(2048*60000.0), multiplier, 1e-9)
	require.Equal(t, adaptive.Reason(ReasonBudget), reason)
	require.Equal(t, Detail[float64]{
		Params: Params[float64]{
			Min:          0.05,
			Max:          1,
			Share:        0.2,
			BytesPerUnit: 2048 * 60000,
			HighWater:    0.85,
			LowWater:     0.6,
			ShrinkFrac:   0.25,
			GrowFrac:     0.1,
			Interval:     time.Minute,
		},
		Signals:     Signals{Now: snap.Time, GoLimit: 256 * mib, HeapGoal: 100 * mib, HeapLive: 50 * mib},
		MaxStepFrac: 0.25,
		Shards:      4,
		Tasks:       60000,
	}, detail)
}
