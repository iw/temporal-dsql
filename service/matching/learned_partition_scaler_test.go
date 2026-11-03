package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/partitions"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/signals"
	"google.golang.org/protobuf/types/known/anypb"
)

type (
	// fixedScaler is a scaler that always decides the same.
	fixedScaler struct {
		decision PartitionScalerDecision
		calls    int
	}

	decisionRecorder struct {
		decisions []adaptive.Decision
	}

	learnedScalerTest struct {
		scaler   *learnedScaler
		simple   *fixedScaler
		time     *clock.EventTimeSource
		settings partitions.Settings
		frozen   bool
		snapshot *signals.Snapshot
		sink     decisionRecorder
	}
)

func (f *fixedScaler) OnTasks(PartitionScalerInput) PartitionScalerDecision {
	f.calls++
	return f.decision
}

func (f *fixedScaler) Stop() {}

func (r *decisionRecorder) RecordDecision(d adaptive.Decision) {
	r.decisions = append(r.decisions, d)
}

func newLearnedScalerTest(mode string) *learnedScalerTest {
	lt := &learnedScalerTest{
		simple:   &fixedScaler{decision: PartitionScalerDecision{NewTarget: 0}},
		time:     clock.NewEventTimeSource(),
		settings: partitions.DefaultSettings,
	}
	lt.settings.Mode = mode
	lt.settings.Settle = 0
	lt.time.Update(time.Unix(1_700_000_000, 0))
	lt.snapshot = &signals.Snapshot{Time: lt.time.Now(), Persistence: persistence.HealthSnapshot{Samples: 1000}}
	factory := &learnedScalerFactory{
		frozen:     func() bool { return lt.frozen },
		latest:     func() *signals.Snapshot { return lt.snapshot },
		sink:       &lt.sink,
		handler:    metrics.NoopMetricsHandler,
		timeSource: lt.time,
	}
	lt.scaler = &learnedScaler{
		factory:  factory,
		simple:   lt.simple,
		settings: func() partitions.Settings { return lt.settings },
		simpleCfg: func() dynamicconfig.SimplePartitionScalerSettings {
			return dynamicconfig.SimplePartitionScalerSettings{BacklogCap: 100}
		},
		writePartitions: func() int { return 4 },
		key:             []byte("ns/tq/TASK_QUEUE_TYPE_ACTIVITY"),
		namespace:       "ns",
		taskQueue:       "tq",
		taskQueueType:   enumspb.TASK_QUEUE_TYPE_ACTIVITY.String(),
		law:             partitions.New(0),
	}
	return lt
}

// stats are the task queue's stats, described now.
func (lt *learnedScalerTest) stats(writePartitions int, addRate, dispatchRate float32) *PartitionScalerStats {
	return &PartitionScalerStats{
		Time:            lt.time.Now(),
		WritePartitions: writePartitions,
		Described:       writePartitions,
		AddRate:         addRate,
		DispatchRate:    dispatchRate,
	}
}

func (lt *learnedScalerTest) lastDecision(t *testing.T) adaptive.Decision {
	t.Helper()
	require.NotEmpty(t, lt.sink.decisions)
	return lt.sink.decisions[len(lt.sink.decisions)-1]
}

func TestLearnedScalerIsTheSimpleScalerWhileDisabled(t *testing.T) {
	lt := newLearnedScalerTest(adaptive.ModeNameDisabled)
	lt.simple.decision = PartitionScalerDecision{NewTarget: 6, BacklogCap: 10}

	in := PartitionScalerInput{NumTasks: 100, CurrentTarget: 4, Stats: lt.stats(4, 3000, 2000)}
	require.Equal(t, lt.simple.decision, lt.scaler.OnTasks(in))
	require.Equal(t, 1, lt.simple.calls)
	require.False(t, lt.scaler.WantsStats())
	require.Empty(t, lt.sink.decisions)
}

func TestLearnedScalerProposesInShadowWhileTheSimpleScalerApplies(t *testing.T) {
	lt := newLearnedScalerTest(adaptive.ModeNameShadow)
	lt.simple.decision = PartitionScalerDecision{NoChange: true}

	// It asks for stats once tasks arrive, and until StatsIdle after the last.
	require.False(t, lt.scaler.WantsStats())
	require.Equal(t, lt.simple.decision, lt.scaler.OnTasks(PartitionScalerInput{NumTasks: 100}))
	require.True(t, lt.scaler.WantsStats())

	// 3,600 tasks a second on 4 partitions at 1,000 each needs a fifth.
	lt.time.Advance(23 * time.Second)
	require.Equal(t, lt.simple.decision, lt.scaler.OnTasks(PartitionScalerInput{Stats: lt.stats(4, 3600, 3600)}))
	d := lt.lastDecision(t)
	require.Equal(t, partitions.Name, d.Controller)
	require.Equal(t, adaptive.ModeShadow, d.Mode)
	require.Equal(t, adaptive.Reason(partitions.ReasonRate), d.Reason)
	require.InDelta(t, 4, d.Static, 0)
	require.InDelta(t, 5, d.Proposed, 0)
	require.InDelta(t, 4, d.Applied, 0)
	detail, ok := d.Detail.(partitions.Detail)
	require.True(t, ok)
	require.Equal(t, "tq", detail.TaskQueue)
	require.Equal(t, "ns", detail.Namespace)
	require.Equal(t, enumspb.TASK_QUEUE_TYPE_ACTIVITY.String(), detail.TaskQueueType)
	require.False(t, detail.Held)
	require.True(t, detail.Started, "the law's first step")
	require.Zero(t, detail.Loaded)
	require.True(t, detail.Params.Shadow)
	require.InDelta(t, 1000, detail.Capacity, 0)

	// The same stats again aren't a new step.
	lt.scaler.OnTasks(PartitionScalerInput{Stats: lt.stats(4, 3600, 3600)})
	require.Len(t, lt.sink.decisions, 1)

	lt.time.Advance(lt.settings.StatsIdle)
	require.False(t, lt.scaler.WantsStats())
}

func TestLearnedScalerSizesThePartitionsWhileEnabled(t *testing.T) {
	lt := newLearnedScalerTest(adaptive.ModeNameEnabled)

	// Without stats there's nothing to step on.
	require.Equal(t, PartitionScalerDecision{NoChange: true}, lt.scaler.OnTasks(PartitionScalerInput{NumTasks: 100}))
	require.True(t, lt.scaler.WantsStats())

	// 4 partitions dispatched 6,000 tasks a second without falling behind, so each
	// dispatches 1,500 or more, and the add rate needs 5 of them.
	lt.time.Advance(23 * time.Second)
	decision := lt.scaler.OnTasks(PartitionScalerInput{Stats: lt.stats(4, 6000, 6000)})
	require.Equal(t, 5, decision.NewTarget)
	require.Equal(t, 100, decision.BacklogCap)
	capacity, ours := learnedCapacity(decision.PrivateState)
	require.True(t, ours)
	require.InDelta(t, 1500, capacity, 1e-9)
	d := lt.lastDecision(t)
	require.Equal(t, adaptive.ModeOn, d.Mode)
	require.InDelta(t, 5, d.Applied, 0)

	// Once it sizes the partitions, it wants stats even while no tasks arrive.
	lt.time.Advance(23 * time.Second)
	decision = lt.scaler.OnTasks(PartitionScalerInput{CurrentTarget: 5, PrivateState: decision.PrivateState, Stats: lt.stats(5, 6000, 6000)})
	require.Equal(t, 5, decision.NewTarget)
	lt.time.Advance(time.Hour)
	require.True(t, lt.scaler.WantsStats())

	// A task queue out of the rollout's cohort runs in shadow.
	lt = newLearnedScalerTest(adaptive.ModeNameEnabled)
	lt.settings.RolloutPercent = 0
	lt.simple.decision = PartitionScalerDecision{NewTarget: 3}
	require.Equal(t, lt.simple.decision, lt.scaler.OnTasks(PartitionScalerInput{NumTasks: 1, Stats: lt.stats(4, 6000, 6000)}))
	require.Equal(t, adaptive.ModeShadow, lt.lastDecision(t).Mode)
	require.InDelta(t, 3, lt.lastDecision(t).Static, 0)
}

func TestLearnedScalerSizesFromTheTargetWhilePartitionsDrain(t *testing.T) {
	lt := newLearnedScalerTest(adaptive.ModeNameEnabled)

	// After a step down from 13 to 11, tasks still go to 12 partitions until the 12th
	// drains. The controller holds the target it set.
	decision := lt.scaler.OnTasks(PartitionScalerInput{
		CurrentTarget:  11,
		ReadPartitions: 13,
		PrivateState:   learnedState(100),
		Stats:          lt.stats(12, 900, 900),
	})
	require.Equal(t, 11, decision.NewTarget)
	detail, ok := lt.lastDecision(t).Detail.(partitions.Detail)
	require.True(t, ok)
	require.Equal(t, 11, detail.Signals.Partitions)
	require.Equal(t, 12, detail.Signals.Described)
}

func TestLearnedScalerTakesItsCapacityFromItsPrivateState(t *testing.T) {
	lt := newLearnedScalerTest(adaptive.ModeNameShadow)

	// In shadow, the law steps without the private state's capacity.
	lt.scaler.OnTasks(PartitionScalerInput{CurrentTarget: 4, PrivateState: learnedState(500), Stats: lt.stats(4, 1800, 1800)})
	detail, ok := lt.lastDecision(t).Detail.(partitions.Detail)
	require.True(t, ok)
	require.True(t, detail.Started)
	require.Zero(t, detail.Loaded)

	// Enabled, it starts over from it: at 500 tasks a second a partition, 1,800 a second
	// needs 5 partitions.
	lt.settings.Mode = adaptive.ModeNameEnabled
	lt.time.Advance(time.Minute)
	decision := lt.scaler.OnTasks(PartitionScalerInput{
		CurrentTarget: 4,
		PrivateState:  learnedState(500),
		Stats:         lt.stats(4, 1800, 1800),
	})
	require.Equal(t, 5, decision.NewTarget)
	capacity, _ := learnedCapacity(decision.PrivateState)
	require.InDelta(t, 500, capacity, 1e-9)
	detail, ok = lt.lastDecision(t).Detail.(partitions.Detail)
	require.True(t, ok)
	require.True(t, detail.Started)
	require.InDelta(t, 500, detail.Loaded, 0, "the step reports the capacity the law started from")

	// Later steps don't.
	lt.time.Advance(time.Minute)
	lt.scaler.OnTasks(PartitionScalerInput{CurrentTarget: 5, PrivateState: decision.PrivateState, Stats: lt.stats(5, 1800, 1800)})
	detail, ok = lt.lastDecision(t).Detail.(partitions.Detail)
	require.True(t, ok)
	require.False(t, detail.Started)
	require.Zero(t, detail.Loaded)
}

func TestLearnedScalerHoldsWhileFrozenOrWithoutData(t *testing.T) {
	for name, tc := range map[string]struct {
		setUp  func(lt *learnedScalerTest)
		stats  func(lt *learnedScalerTest) *PartitionScalerStats
		reason adaptive.Reason
	}{
		"frozen": {
			setUp:  func(lt *learnedScalerTest) { lt.frozen = true },
			reason: adaptive.ReasonFrozen,
		},
		"no snapshot": {
			setUp:  func(lt *learnedScalerTest) { lt.snapshot = nil },
			reason: adaptive.Reason(partitions.ReasonNoData),
		},
		"no partition answered": {
			stats: func(lt *learnedScalerTest) *PartitionScalerStats {
				stats := lt.stats(4, 6000, 6000)
				stats.Described = 0
				return stats
			},
			reason: adaptive.Reason(partitions.ReasonNoData),
		},
	} {
		t.Run(name, func(t *testing.T) {
			lt := newLearnedScalerTest(adaptive.ModeNameEnabled)
			if tc.setUp != nil {
				tc.setUp(lt)
			}
			stats := lt.stats(4, 6000, 6000)
			if tc.stats != nil {
				stats = tc.stats(lt)
			}
			decision := lt.scaler.OnTasks(PartitionScalerInput{CurrentTarget: 4, PrivateState: learnedState(0), Stats: stats})
			require.Equal(t, PartitionScalerDecision{NoChange: true}, decision)
			require.Equal(t, tc.reason, lt.lastDecision(t).Reason)
			require.InDelta(t, 4, lt.lastDecision(t).Proposed, 0)
			detail, ok := lt.lastDecision(t).Detail.(partitions.Detail)
			require.True(t, ok)
			require.True(t, detail.Held)
		})
	}
}

func TestLearnedScalerHoldsItsPartitionsWhilePersistenceBacksOff(t *testing.T) {
	lt := newLearnedScalerTest(adaptive.ModeNameEnabled)
	lt.snapshot.Persistence.Throttled = 100

	decision := lt.scaler.OnTasks(PartitionScalerInput{CurrentTarget: 4, Stats: lt.stats(4, 3600, 3600)})
	require.Equal(t, 4, decision.NewTarget)
	d := lt.lastDecision(t)
	require.Equal(t, adaptive.Reason(partitions.ReasonBackoff), d.Reason)
	detail, ok := d.Detail.(partitions.Detail)
	require.True(t, ok)
	require.True(t, detail.Signals.BackingOff)
}

func TestLearnedScalerDrainsItsPartitionsBeforeManagedScalingTurnsOff(t *testing.T) {
	lt := newLearnedScalerTest(adaptive.ModeNameDisabled)
	ours := learnedState(500)
	simple, err := anypb.New(&persistencespb.SimplePartitionScalerState{AddTarget: 8})
	require.NoError(t, err)

	// The controller added partitions beyond dynamic config's 4, so managed scaling stays on
	// at 4 until they drain.
	decision := lt.scaler.OnTasks(PartitionScalerInput{CurrentTarget: 8, ReadPartitions: 8, PrivateState: ours})
	require.Equal(t, PartitionScalerDecision{NewTarget: 4, PrivateState: ours}, decision)
	decision = lt.scaler.OnTasks(PartitionScalerInput{CurrentTarget: 4, ReadPartitions: 6, PrivateState: ours})
	require.Equal(t, PartitionScalerDecision{NewTarget: 4, PrivateState: ours}, decision)

	// Once they've drained, managed scaling turns off.
	decision = lt.scaler.OnTasks(PartitionScalerInput{CurrentTarget: 4, ReadPartitions: 4, PrivateState: ours})
	require.Equal(t, PartitionScalerDecision{NewTarget: 0}, decision)

	// Partitions the simple scaler added are its own to drain, and a simple scaler that
	// sizes the partitions keeps them.
	decision = lt.scaler.OnTasks(PartitionScalerInput{CurrentTarget: 8, ReadPartitions: 8, PrivateState: simple})
	require.Equal(t, PartitionScalerDecision{NewTarget: 0}, decision)
	lt.simple.decision = PartitionScalerDecision{NewTarget: 6}
	decision = lt.scaler.OnTasks(PartitionScalerInput{CurrentTarget: 8, ReadPartitions: 8, PrivateState: ours})
	require.Equal(t, PartitionScalerDecision{NewTarget: 6}, decision)
}

func TestLearnedStateIsTheControllers(t *testing.T) {
	capacity, ours := learnedCapacity(learnedState(1234.5))
	require.True(t, ours)
	require.InDelta(t, 1234.5, capacity, 1e-9)

	simple, err := anypb.New(&persistencespb.SimplePartitionScalerState{AddTarget: 3})
	require.NoError(t, err)
	_, ours = learnedCapacity(simple)
	require.False(t, ours)
	_, ours = learnedCapacity(nil)
	require.False(t, ours)
}
