package memory

import (
	"maps"
	"math"
	"slices"
	"sync"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/signals"
)

// PendingTasks names the tasks history's queues hold, in their controller's name.
const PendingTasks = "pending_tasks"

// minCriticalCount is the least the multiplier takes a queue's critical count to. A
// queue's readers after its first stop loading at 0.8 of the critical count, or less,
// but at no fewer than 1000 tasks. At 1000 / 0.8 that floor starts to lift them, and at
// 1000 or below they'd load past the critical count, so the mitigator would unload
// their tasks over and over.
const minCriticalCount = 1250

type (
	// QueueLimits scales the pending-task limits of history's queues by one multiplier,
	// so that the tasks every queue on the host may hold, at BytesPerTask each, fit in
	// PendingTaskShare of the memory limit. Each shard has its own queues, so what they
	// may hold grows with the host's shards.
	QueueLimits struct {
		driven applier
		ctrl   *pendingController
	}

	// applier is the driven multiplier, as signals.Driven gives it.
	applier interface {
		Applying() (float64, bool)
	}

	pendingController struct {
		settings func() Settings
		law      *Controller[float64]

		mu     sync.Mutex // guards the fields below
		shards func() int
		queues map[string]queueCounts
	}

	// queueCounts are a queue's pending task count settings.
	queueCounts struct {
		maxCount, criticalCount dynamicconfig.IntPropertyFn
	}

	// pendingDetail is a step as its decision's detail, so a trace can replay it.
	pendingDetail struct {
		Params  Params[float64]
		Signals Signals
		// Shards is the host's shards, and Tasks what their queues may hold at their
		// settings.
		Shards, Tasks int
	}
)

// NewQueueLimits returns the pending-task limits of history's queues, driven from the
// memory budget. While the controller is disabled, each queue keeps its settings.
func NewQueueLimits(plane *signals.Plane, collection *dynamicconfig.Collection, handler metrics.Handler) *QueueLimits {
	ctrl := newPendingController(Config.Get(collection))
	driven := signals.DriveFunc(plane, func() float64 { return 1 }, ctrl, adaptive.Frozen.Get(collection), adaptive.HostKey(), handler)
	return &QueueLimits{driven: driven, ctrl: ctrl}
}

func newPendingController(settings func() Settings) *pendingController {
	return &pendingController{
		settings: settings,
		law:      New(1.0),
		queues:   make(map[string]queueCounts),
	}
}

// SetShardCount sets how the controller counts the host's shards. Until it's set, the
// queues keep their settings.
func (l *QueueLimits) SetShardCount(shards func() int) {
	l.ctrl.mu.Lock()
	defer l.ctrl.mu.Unlock()
	l.ctrl.shards = shards
}

// Limits returns a queue's max and critical pending task counts, scaled by the
// multiplier while the controller is enabled. maxCount is the most each shard's queue
// may hold, which the budget counts.
func (l *QueueLimits) Limits(queue string, maxCount, criticalCount dynamicconfig.IntPropertyFn) (scaledMax, scaledCritical dynamicconfig.IntPropertyFn) {
	l.ctrl.mu.Lock()
	l.ctrl.queues[queue] = queueCounts{maxCount: maxCount, criticalCount: criticalCount}
	l.ctrl.mu.Unlock()
	return l.scaled(maxCount), l.scaled(criticalCount)
}

// scaled is a pending task count setting times the multiplier. A count of 0 or less
// isn't a limit, so it stays.
func (l *QueueLimits) scaled(setting dynamicconfig.IntPropertyFn) dynamicconfig.IntPropertyFn {
	return func() int {
		count := setting()
		if multiplier, on := l.driven.Applying(); on && count > 0 {
			return max(1, int(math.Round(multiplier*float64(count))))
		}
		return count
	}
}

func (c *pendingController) Name() string {
	return "memory_budget_" + PendingTasks
}

// Envelope keeps the multiplier where no queue's critical count is below
// minCriticalCount. If one is set below it, the queues keep their settings.
func (c *pendingController) Envelope(static float64) adaptive.Envelope[float64] {
	s := c.settings()
	return adaptive.Envelope[float64]{
		Mode:           s.Mode,
		RolloutPercent: s.RolloutPercent,
		Min:            min(1, max(s.MinMultiplier, c.criticalFloor())) * static,
		Max:            static,
		MaxStepFrac:    s.MaxStepFrac,
	}
}

func (c *pendingController) Reset(multiplier float64) {
	c.law.Reset(multiplier)
}

func (c *pendingController) Step(snap *signals.Snapshot, env adaptive.Envelope[float64], _ float64) (float64, adaptive.Reason, any) {
	s := c.settings()
	shards, tasks := c.held()
	params := Params[float64]{
		Min:          env.Min,
		Max:          env.Max,
		Share:        s.PendingTaskShare,
		BytesPerUnit: float64(s.BytesPerTask) * float64(tasks),
		HighWater:    s.HighWater,
		LowWater:     s.LowWater,
		ShrinkFrac:   s.ShrinkFrac,
		GrowFrac:     s.GrowFrac,
		Interval:     s.Interval,
	}
	in := memorySignals(snap)
	multiplier, reason := c.law.Step(params, in)
	return multiplier, adaptive.Reason(reason), pendingDetail{Params: params, Signals: in, Shards: shards, Tasks: tasks}
}

// held is the host's shards, and the tasks their queues may hold at their settings.
func (c *pendingController) held() (shards, tasks int) {
	c.mu.Lock()
	countShards := c.shards
	c.mu.Unlock()
	if countShards == nil {
		return 0, 0
	}
	shards = countShards()
	for _, counts := range c.counts() {
		tasks += max(0, counts.maxCount())
	}
	return shards, shards * tasks
}

// criticalFloor is the least multiplier that keeps every queue's critical count at
// minCriticalCount.
func (c *pendingController) criticalFloor() float64 {
	floor := 0.0
	for _, counts := range c.counts() {
		if critical := counts.criticalCount(); critical > 0 {
			floor = max(floor, minCriticalCount/float64(critical))
		}
	}
	return floor
}

func (c *pendingController) counts() []queueCounts {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Collect(maps.Values(c.queues))
}
