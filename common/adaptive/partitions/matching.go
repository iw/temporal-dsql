package partitions

import (
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/signals"
)

// Name is the controller's name in its decisions and their metric.
const Name = "partition_scaler"

// Settings is the controller's dynamic config, for each task queue.
type Settings struct {
	// Mode is "disabled", "shadow" or "enabled". RolloutPercent is the share of task
	// queues, picked by name, that apply an enabled Mode; the others run it in shadow.
	Mode           string
	RolloutPercent int
	MinPartitions  int
	MaxPartitions  int
	MaxStepFrac    float64
	// InitialCapacity is the tasks a second a partition is taken to dispatch until the
	// controller measures it.
	InitialCapacity float64
	Headroom        float64
	MinRate         float64
	BehindRatio     float64
	TrialWindow     time.Duration
	MinGain         float64
	NoGainHold      time.Duration
	DownHold        time.Duration
	Settle          time.Duration
	// Persistence is backing off while the capacity controller found the store overloaded
	// in the last BackoffHold, while errors, throttled calls and the persistence rate
	// limiter's rejections pass MaxErrorRatio of at least MinSamples calls, or while more
	// than MaxQueuedRatio of the store's connection acquires waited.
	BackoffHold    time.Duration
	MaxErrorRatio  float64
	MaxQueuedRatio float64
	MinSamples     int
	// StatsIdle is how long after a task queue's last task its partitions stop being
	// described for the controller, unless the controller sizes them.
	StatsIdle time.Duration
}

// DefaultSettings leaves the controller off.
var DefaultSettings = Settings{
	Mode:            adaptive.ModeNameDisabled,
	RolloutPercent:  100,
	MinPartitions:   1,
	MaxPartitions:   64,
	MaxStepFrac:     0.25,
	InitialCapacity: 1000,
	Headroom:        0.2,
	MinRate:         50,
	BehindRatio:     0.1,
	TrialWindow:     2 * time.Minute,
	MinGain:         0.5,
	NoGainHold:      10 * time.Minute,
	DownHold:        5 * time.Minute,
	Settle:          30 * time.Second,
	BackoffHold:     10 * time.Second,
	MaxErrorRatio:   0.01,
	MaxQueuedRatio:  0.25,
	MinSamples:      20,
	StatsIdle:       5 * time.Minute,
}

// Config is the controller's dynamic config setting.
var Config = dynamicconfig.NewTaskQueueTypedSetting(
	"matching.partitionScalerController",
	DefaultSettings,
	`matching.partitionScalerController sizes a task queue's partitions from the tasks a second
each partition dispatches, which it learns, starting from InitialCapacity. The partitions grow to
dispatch the add rate with Headroom to spare, and shrink after DownHold while they'd keep twice
that. They grow again only Settle after they, or the partitions described, last changed. When
the task queue falls behind, adding tasks BehindRatio faster than it dispatches them, the
controller adds partitions as a trial and judges it after TrialWindow: a trial that raised
dispatch measures the capacity, and one that didn't is undone, with no trials for NoGainHold.
The partitions stay within MinPartitions and MaxPartitions, and change by at most MaxStepFrac.
While it's enabled, the controller replaces matching.partitionScaler, and it keeps a partition
it added until its tasks drain. Mode is disabled, shadow or enabled.`,
)

// Params are the law's parameters from the settings. A controller in shadow runs no
// trials. A task queue keeps at least one partition.
func (s Settings) Params(shadow bool) Params {
	return Params{
		Min:         max(1, s.MinPartitions),
		Max:         max(1, s.MaxPartitions),
		MaxStepFrac: s.MaxStepFrac,
		Capacity:    s.InitialCapacity,
		Headroom:    s.Headroom,
		MinRate:     s.MinRate,
		BehindRatio: s.BehindRatio,
		TrialWindow: s.TrialWindow,
		MinGain:     s.MinGain,
		NoGainHold:  s.NoGainHold,
		DownHold:    s.DownHold,
		Settle:      s.Settle,
		Shadow:      shadow,
	}
}

// Backoff is when the controller takes persistence to be backing off.
func (s Settings) Backoff() signals.Backoff {
	return signals.Backoff{
		Hold:           s.BackoffHold,
		MaxErrorRatio:  s.MaxErrorRatio,
		MaxQueuedRatio: s.MaxQueuedRatio,
		MinSamples:     s.MinSamples,
	}
}
