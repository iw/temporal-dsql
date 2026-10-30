package workers

import (
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/signals"
)

type (
	// Settings is the dynamic config of history's worker-count controllers, which every
	// queue shares. Bounds are multiples of each queue's own worker count setting.
	Settings struct {
		// Mode is "disabled", "shadow" or "enabled", and RolloutPercent the share of hosts
		// that apply an enabled Mode.
		Mode           string
		RolloutPercent int
		MinMultiplier  float64
		MaxMultiplier  float64
		TargetDelay    time.Duration
		GrowFrac       float64
		ShrinkFrac     float64
		IdleSteps      int
		MinSamples     int
		Cooldown       time.Duration
		// Persistence is backing off while the capacity controller found the store
		// overloaded in the last BackoffHold, while errors, throttled calls and the
		// persistence rate limiter's rejections pass MaxErrorRatio of calls, or while
		// more than MaxQueuedRatio of the store's connection acquires waited.
		BackoffHold    time.Duration
		MaxErrorRatio  float64
		MaxQueuedRatio float64
	}

	// HistoryQueues gives history's task queues their queueing delay recorders and their
	// worker counts.
	HistoryQueues struct {
		plane    *signals.Plane
		settings func() Settings
		frozen   func() bool
		hostKey  []byte
		metrics  metrics.Handler
		health   signals.ScheduleLatencyRecorder
	}

	queueController struct {
		queue    string
		settings func() Settings
		law      *Controller
	}

	// stepDetail is a step as its decision's detail, so a trace can replay it.
	stepDetail struct {
		Params  Params
		Signals Signals
	}
)

// DefaultSettings leaves the controllers off.
var DefaultSettings = Settings{
	Mode:           adaptive.ModeNameDisabled,
	RolloutPercent: 100,
	MinMultiplier:  0.5,
	MaxMultiplier:  2,
	TargetDelay:    100 * time.Millisecond,
	GrowFrac:       0.25,
	ShrinkFrac:     0.05,
	IdleSteps:      30,
	MinSamples:     20,
	Cooldown:       3 * time.Second,
	BackoffHold:    10 * time.Second,
	MaxErrorRatio:  0.01,
	MaxQueuedRatio: 0.25,
}

// Config is the controllers' dynamic config setting.
var Config = dynamicconfig.NewGlobalTypedSetting(
	"history.workerCountController",
	DefaultSettings,
	`history.workerCountController sizes the worker pools of history's transfer, timer, visibility
and archival queues from their queueing delay, within MinMultiplier and MaxMultiplier of each
queue's worker count setting. A pool grows while its p90 delay passes TargetDelay, unless
persistence is backing off, and shrinks after IdleSteps idle steps. The controllers step on the
signal plane's snapshots. Mode is disabled, shadow or enabled.`,
)

func NewHistoryQueues(
	plane *signals.Plane,
	collection *dynamicconfig.Collection,
	health persistence.HealthSignalAggregator,
	handler metrics.Handler,
) *HistoryQueues {
	return &HistoryQueues{
		plane:    plane,
		settings: Config.Get(collection),
		frozen:   adaptive.Frozen.Get(collection),
		hostKey:  adaptive.HostKey(),
		metrics:  handler,
		health:   health,
	}
}

// ScheduleLatencyRecorder returns the recorder for a queue's task attempts, which passes
// every delay on to the persistence health aggregator too.
func (h *HistoryQueues) ScheduleLatencyRecorder(queue string) signals.ScheduleLatencyRecorder {
	if h == nil {
		return nil
	}
	return h.plane.QueueRecorder(queue, h.health)
}

// WorkerCount returns a queue's worker count, driven by its controller from the queue's
// worker count setting.
func (h *HistoryQueues) WorkerCount(queue string, static dynamicconfig.TypedSubscribable[int]) dynamicconfig.TypedSubscribable[int] {
	if h == nil {
		return static
	}
	ctrl := &queueController{queue: queue, settings: h.settings, law: New(0)}
	return signals.Drive(h.plane, static, ctrl, h.frozen, h.hostKey, h.metrics).Subscribable()
}

func (q *queueController) Name() string {
	return "worker_count_" + q.queue
}

func (q *queueController) Envelope(static int) adaptive.Envelope[int] {
	s := q.settings()
	return adaptive.Envelope[int]{
		Mode:           s.Mode,
		RolloutPercent: s.RolloutPercent,
		Min:            max(1, int(s.MinMultiplier*float64(static))),
		Max:            max(1, int(s.MaxMultiplier*float64(static))),
		MaxStepFrac:    s.GrowFrac,
	}
}

func (q *queueController) Reset(count int) {
	q.law.Reset(count)
}

func (q *queueController) Step(snap *signals.Snapshot, env adaptive.Envelope[int], static int) (int, adaptive.Reason, any) {
	s := q.settings()
	params := Params{
		Min:         env.Min,
		Max:         env.Max,
		TargetDelay: s.TargetDelay,
		GrowFrac:    s.GrowFrac,
		ShrinkFrac:  s.ShrinkFrac,
		IdleSteps:   s.IdleSteps,
		MinSamples:  s.MinSamples,
		Cooldown:    s.Cooldown,
	}
	queue := snap.Queues[q.queue]
	in := Signals{Now: snap.Time, Delay: queue.DelayP90, Samples: queue.Samples, BackingOff: persistenceBackingOff(snap, s)}
	count, reason := q.law.Step(params, static, in)
	return count, adaptive.Reason(reason), stepDetail{Params: params, Signals: in}
}

// persistenceBackingOff reads whether persistence is backing off from the snapshot: the
// capacity controller's latest decision, when it's recent, and persistence health.
func persistenceBackingOff(snap *signals.Snapshot, s Settings) bool {
	if d, ok := snap.Decisions[capacity.Name]; ok && snap.Time.Sub(d.Time) <= s.BackoffHold && capacity.Reason(d.Reason).Overloaded() {
		return true
	}
	h := snap.Persistence
	if calls := h.Samples + h.Rejected; calls >= max(1, s.MinSamples) {
		failed := h.ErrorRatio*float64(h.Samples) + float64(h.Throttled+h.Rejected)
		if failed/float64(calls) > s.MaxErrorRatio {
			return true
		}
	}
	return h.Acquired > 0 && float64(h.Queued)/float64(h.Acquired) > s.MaxQueuedRatio
}
