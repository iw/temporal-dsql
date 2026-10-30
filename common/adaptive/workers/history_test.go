package workers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/signals"
)

func TestPersistenceBackingOff(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	healthy := persistence.HealthSnapshot{Samples: 1000}
	capacityDecision := func(reason capacity.Reason, age time.Duration) map[string]adaptive.Decision {
		return map[string]adaptive.Decision{capacity.Name: {Controller: capacity.Name, Time: now.Add(-age), Reason: adaptive.Reason(reason)}}
	}
	for name, tc := range map[string]struct {
		persistence persistence.HealthSnapshot
		decisions   map[string]adaptive.Decision
		want        bool
	}{
		"healthy":                          {persistence: healthy},
		"capacity controller cut":          {persistence: healthy, decisions: capacityDecision(capacity.ReasonLatency, 5*time.Second), want: true},
		"capacity controller cooling down": {persistence: healthy, decisions: capacityDecision(capacity.ReasonCooldown, time.Second), want: true},
		"capacity controller cut long ago": {persistence: healthy, decisions: capacityDecision(capacity.ReasonErrors, time.Minute)},
		"capacity controller probing":      {persistence: healthy, decisions: capacityDecision(capacity.ReasonProbe, time.Second)},
		"errors":                           {persistence: persistence.HealthSnapshot{Samples: 1000, ErrorRatio: 0.02}, want: true},
		"throttled":                        {persistence: persistence.HealthSnapshot{Samples: 1000, Throttled: 20}, want: true},
		"limiter rejecting":                {persistence: persistence.HealthSnapshot{Samples: 1000, Rejected: 20}, want: true},
		"a few failures among few calls":   {persistence: persistence.HealthSnapshot{Samples: 5, ErrorRatio: 0.2}},
		"pool acquires waiting":            {persistence: persistence.HealthSnapshot{Samples: 1000, Queued: 300, Acquired: 1000}, want: true},
		"a few pool acquires waiting":      {persistence: persistence.HealthSnapshot{Samples: 1000, Queued: 100, Acquired: 1000}},
	} {
		t.Run(name, func(t *testing.T) {
			snap := &signals.Snapshot{Time: now, Persistence: tc.persistence, Decisions: tc.decisions}
			require.Equal(t, tc.want, persistenceBackingOff(snap, DefaultSettings))
		})
	}
}

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
}
