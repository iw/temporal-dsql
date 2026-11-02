package workers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/signals"
)

func TestQueueControllerStepsOnItsQueuesDelay(t *testing.T) {
	settings := DefaultSettings
	settings.Mode = adaptive.ModeNameEnabled
	q := &queueController{queue: "transfer", settings: func() Settings { return settings }, law: New(0)}
	require.Equal(t, "worker_count_transfer", q.Name())

	env := q.Envelope(512)
	require.Equal(t, adaptive.Envelope[int]{Mode: adaptive.ModeNameEnabled, RolloutPercent: 100, Min: 256, Max: 1024, MaxStepFrac: 0.25}, env)

	q.Reset(512)
	snap := &signals.Snapshot{
		Time:        time.Unix(1_700_000_000, 0),
		Persistence: persistence.HealthSnapshot{Samples: 1000},
		Queues: map[string]signals.Queue{
			"transfer": {DelayP90: 300 * time.Millisecond, Samples: 100},
			"timer":    {DelayP90: time.Millisecond, Samples: 100},
		},
	}
	count, reason, detail := q.Step(snap, env, 512)
	require.Equal(t, 640, count)
	require.Equal(t, adaptive.Reason(ReasonGrow), reason)
	require.Equal(t, stepDetail{
		Params:  Params{Min: 256, Max: 1024, TargetDelay: settings.TargetDelay, GrowFrac: 0.25, ShrinkFrac: 0.05, IdleSteps: 30, MinSamples: 20, Cooldown: 3 * time.Second},
		Signals: Signals{Now: snap.Time, Delay: 300 * time.Millisecond, Samples: 100},
	}, detail)
}

func TestHistoryQueuesWithoutAPlaneLeaveTheSettingsAlone(t *testing.T) {
	var queues *HistoryQueues
	static := dynamicconfig.TypedSubscribable[int](func(func(int)) (int, func()) { return 512, func() {} })
	value, _ := queues.WorkerCount("transfer", static)(nil)
	require.Equal(t, 512, value)
	require.Nil(t, queues.ScheduleLatencyRecorder("transfer"))
	maxCount, criticalCount := queues.PendingTaskLimits("transfer", func() int { return 10000 }, func() int { return 9000 })
	require.Equal(t, 10000, maxCount())
	require.Equal(t, 9000, criticalCount())
}
