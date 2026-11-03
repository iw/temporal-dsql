package matching

import (
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/partitions"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/signals"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// planeActiveWindow is how long after a scaler last stepped in shadow or enabled mode the
// signal plane keeps running for it.
const planeActiveWindow = 2 * time.Minute

type (
	// learnedScalerFactory gives each root partition a scaler that learns its partitions'
	// capacity, around the simple scaler. While the controller is disabled for a task
	// queue, its scaler is the simple scaler. See docs/dsql/adaptive-behaviour.md,
	// Proposal 6.
	learnedScalerFactory struct {
		simple          *simplePartitionScalerFactory
		simpleCfg       scalerFactoryCfg
		settings        dynamicconfig.TypedPropertyFnWithTaskQueueFilter[partitions.Settings]
		writePartitions dynamicconfig.IntPropertyFnWithTaskQueueFilter
		frozen          dynamicconfig.BoolPropertyFn
		latest          func() *signals.Snapshot // the signal plane's latest snapshot
		sink            adaptive.DecisionSink
		handler         metrics.Handler
		timeSource      clock.TimeSource
		// active is when a scaler last stepped in shadow or enabled mode, in Unix
		// nanoseconds.
		active atomic.Int64
	}

	// learnedScaler is one task queue's scaler. The scale manager calls it from one
	// goroutine.
	learnedScaler struct {
		factory         *learnedScalerFactory
		simple          PartitionScaler
		settings        func() partitions.Settings
		simpleCfg       func() dynamicconfig.SimplePartitionScalerSettings
		writePartitions func() int
		key             []byte // the task queue, which places it in or out of the rollout
		namespace       string
		taskQueue       string
		taskQueueType   string
		law             *partitions.Controller
		loaded          bool      // whether the law has the private state's capacity
		loadedFrom      float64   // that capacity, until a step reports it
		reported        bool      // whether a step reported the law since it started
		lastStats       time.Time // the stats the law last stepped on
		lastTasks       time.Time // when tasks last arrived
		managed         bool      // whether the controller sized the partitions at the last call
	}
)

var _ PartitionStatsScaler = (*learnedScaler)(nil)

func learnedPartitionScalerFactoryProvider(
	dc *dynamicconfig.Collection,
	plane *signals.Plane,
	handler metrics.Handler,
	lc fx.Lifecycle,
) PartitionScalerFactory {
	f := newLearnedScalerFactory(dc, plane.Latest, plane, handler, clock.NewRealTimeSource())
	cancel := plane.Subscribe(func(*signals.Snapshot) {}, f.planeActive)
	lc.Append(fx.StopHook(cancel))
	return f
}

func newLearnedScalerFactory(
	dc *dynamicconfig.Collection,
	latest func() *signals.Snapshot,
	sink adaptive.DecisionSink,
	handler metrics.Handler,
	timeSource clock.TimeSource,
) *learnedScalerFactory {
	simpleCfg := dynamicconfig.MatchingPartitionScaler.Get(dc)
	return &learnedScalerFactory{
		simple:          newSimplePartitionScalerFactory(simpleCfg),
		simpleCfg:       simpleCfg,
		settings:        partitions.Config.Get(dc),
		writePartitions: dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(dc),
		frozen:          adaptive.Frozen.Get(dc),
		latest:          latest,
		sink:            sink,
		handler:         handler,
		timeSource:      timeSource,
	}
}

func (f *learnedScalerFactory) New(nsName namespace.Name, tqName string, tqType enumspb.TaskQueueType) PartitionScaler {
	ns := nsName.String()
	return &learnedScaler{
		factory: f,
		simple:  f.simple.New(nsName, tqName, tqType),
		settings: func() partitions.Settings {
			return f.settings(ns, tqName, tqType)
		},
		simpleCfg: func() dynamicconfig.SimplePartitionScalerSettings {
			return f.simpleCfg(ns, tqName, tqType)
		},
		writePartitions: func() int {
			return max(1, f.writePartitions(ns, tqName, tqType))
		},
		key:           []byte(ns + "/" + tqName + "/" + tqType.String()),
		namespace:     ns,
		taskQueue:     tqName,
		taskQueueType: tqType.String(),
		law:           partitions.New(0),
	}
}

// planeActive keeps the signal plane running while scalers step in shadow or enabled mode.
func (f *learnedScalerFactory) planeActive() bool {
	return f.timeSource.Now().UnixNano()-f.active.Load() < int64(planeActiveWindow)
}

func (l *learnedScaler) mode() (partitions.Settings, adaptive.Mode) {
	s := l.settings()
	return s, adaptive.HostMode(s.Mode, s.RolloutPercent, l.key)
}

// WantsStats asks for the task queue's stats while the controller runs, unless no tasks
// arrived in the last StatsIdle and the controller doesn't size the partitions.
func (l *learnedScaler) WantsStats() bool {
	s, mode := l.mode()
	switch {
	case mode == adaptive.ModeOff:
		return false
	case mode == adaptive.ModeOn && l.managed:
		return true
	}
	return !l.lastTasks.IsZero() && l.factory.timeSource.Since(l.lastTasks) < s.StatsIdle
}

func (l *learnedScaler) OnTasks(in PartitionScalerInput) PartitionScalerDecision {
	now := l.factory.timeSource.Now()
	if in.NumTasks > 0 {
		l.lastTasks = now
	}
	// The simple scaler steps on every call, so its rates are current whenever it applies.
	static := l.simple.OnTasks(in)
	s, mode := l.mode()
	l.managed = mode == adaptive.ModeOn && in.CurrentTarget > 0
	if mode == adaptive.ModeOff {
		l.lastStats = time.Time{}
		return l.drainFirst(in, static)
	}
	l.factory.active.Store(now.UnixNano())
	if mode == adaptive.ModeOn && !l.loaded {
		if capacity, ok := learnedCapacity(in.PrivateState); ok {
			l.law = partitions.New(capacity)
			l.loadedFrom, l.reported = capacity, false
		}
		l.loaded = true
	}

	if in.Stats == nil || !in.Stats.Time.After(l.lastStats) {
		if mode == adaptive.ModeOn {
			return PartitionScalerDecision{NoChange: true}
		}
		return l.drainFirst(in, static)
	}
	l.lastStats = in.Stats.Time
	target, held := l.step(s, mode, in, static)
	if mode != adaptive.ModeOn {
		return l.drainFirst(in, static)
	}
	if held {
		return PartitionScalerDecision{NoChange: true}
	}
	return PartitionScalerDecision{
		NewTarget:    target,
		BacklogCap:   int(l.simpleCfg().BacklogCap),
		PrivateState: learnedState(l.law.Learned()),
	}
}

func (l *learnedScaler) Stop() {
	l.simple.Stop()
}

// step runs the law on the task queue's stats and reports its decision. A frozen
// controller, or one without the data to step, holds the partitions.
func (l *learnedScaler) step(
	s partitions.Settings,
	mode adaptive.Mode,
	in PartitionScalerInput,
	static PartitionScalerDecision,
) (target int, held bool) {
	stats := in.Stats
	snap := l.factory.latest()
	params := s.Params(mode == adaptive.ModeShadow)
	// While managed scaling is on, the partitions are its target. Tasks still go to more
	// partitions while ones above the target drain, but the target is what changes.
	count := stats.WritePartitions
	if in.CurrentTarget > 0 {
		count = in.CurrentTarget
	}
	sig := partitions.Signals{
		Now:          stats.Time,
		Partitions:   count,
		Described:    stats.Described,
		AddRate:      float64(stats.AddRate),
		DispatchRate: float64(stats.DispatchRate),
		RateLimited:  stats.RateLimited,
		BackingOff:   snap != nil && snap.PersistenceBackingOff(s.Backoff()),
	}
	var reason adaptive.Reason
	switch {
	case l.factory.frozen():
		target, reason, held = count, adaptive.ReasonFrozen, true
	case snap == nil || stats.Described == 0:
		target, reason, held = count, adaptive.Reason(partitions.ReasonNoData), true
	default:
		var r partitions.Reason
		target, r = l.law.Step(params, sig)
		reason = adaptive.Reason(r)
	}

	staticTarget := l.staticTarget(in, static)
	applied := staticTarget
	if mode == adaptive.ModeOn {
		applied = target
	}
	adaptive.Report(l.factory.handler, l.factory.sink, adaptive.Decision{
		Controller: partitions.Name,
		Time:       stats.Time,
		Mode:       mode,
		Reason:     reason,
		Static:     float64(staticTarget),
		Proposed:   float64(target),
		Applied:    float64(applied),
		Detail: partitions.Detail{
			Namespace:     l.namespace,
			TaskQueue:     l.taskQueue,
			TaskQueueType: l.taskQueueType,
			Params:        params,
			Signals:       sig,
			Capacity:      l.law.Capacity(params),
			Started:       !l.reported,
			Loaded:        l.loadedFrom,
			Held:          held,
			BacklogCount:  stats.BacklogCount,
			BacklogAge:    stats.BacklogAge,
		},
	})
	l.loadedFrom, l.reported = 0, true
	return target, held
}

// staticTarget is the partitions the task queue would have without the controller: the
// simple scaler's target, or dynamic config's.
func (l *learnedScaler) staticTarget(in PartitionScalerInput, d PartitionScalerDecision) int {
	switch {
	case !d.NoChange && d.NewTarget > 0:
		return d.NewTarget
	case d.NoChange && in.CurrentTarget > 0:
		return in.CurrentTarget
	default:
		return l.writePartitions()
	}
}

// drainFirst passes on the decision that applies, unless it turns managed scaling off
// while partitions the controller added beyond dynamic config's still drain. Turning it off
// would leave their tasks unpolled, so managed scaling stays on at dynamic config's count,
// and turns off once they've drained.
func (l *learnedScaler) drainFirst(in PartitionScalerInput, d PartitionScalerDecision) PartitionScalerDecision {
	if d.NoChange || d.NewTarget != 0 || in.CurrentTarget == 0 {
		return d
	}
	if _, ours := learnedCapacity(in.PrivateState); !ours {
		return d
	}
	static := l.writePartitions()
	if in.ReadPartitions <= static {
		return d
	}
	return PartitionScalerDecision{NewTarget: static, PrivateState: in.PrivateState}
}

// learnedState is the controller's private state: the capacity it learned.
func learnedState(capacity float64) *anypb.Any {
	state, err := structpb.NewStruct(map[string]any{"scaler": partitions.Name, "capacity": capacity})
	if err != nil {
		return nil
	}
	packed, _ := anypb.New(state) // ignore error, just use nil
	return packed
}

// learnedCapacity reads the capacity from the controller's private state, and whether the
// state is the controller's.
func learnedCapacity(state *anypb.Any) (float64, bool) {
	var s structpb.Struct
	if state == nil || state.UnmarshalTo(&s) != nil || s.GetFields()["scaler"].GetStringValue() != partitions.Name {
		return 0, false
	}
	return s.GetFields()["capacity"].GetNumberValue(), true
}
