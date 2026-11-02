package memory

import (
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/signals"
)

// HistoryCache and EventsCache name history's host-level caches, in their controllers'
// names.
const (
	HistoryCache = "history_cache"
	EventsCache  = "events_cache"
)

type (
	// Settings is the dynamic config of history's cache controllers. Each cache's size
	// setting is its cap, and MinMultiplier of it its floor.
	Settings struct {
		// Mode is "disabled", "shadow" or "enabled", and RolloutPercent the share of hosts
		// that apply an enabled Mode.
		Mode           string
		RolloutPercent int
		// HistoryCacheShare and EventsCacheShare are each cache's share of the memory limit.
		HistoryCacheShare float64
		EventsCacheShare  float64
		MinMultiplier     float64
		MaxStepFrac       float64
		HighWater         float64
		LowWater          float64
		ShrinkFrac        float64
		GrowFrac          float64
		Interval          time.Duration
	}

	cacheController struct {
		cache    string
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
	Mode:              adaptive.ModeNameDisabled,
	RolloutPercent:    100,
	HistoryCacheShare: 0.3,
	EventsCacheShare:  0.1,
	MinMultiplier:     0.05,
	MaxStepFrac:       0.25,
	HighWater:         0.85,
	LowWater:          0.6,
	ShrinkFrac:        0.25,
	GrowFrac:          0.1,
	Interval:          time.Minute,
}

// Config is the controllers' dynamic config setting.
var Config = dynamicconfig.NewGlobalTypedSetting(
	"history.memoryBudgetController",
	DefaultSettings,
	`history.memoryBudgetController sizes history's host-level caches from the process's memory
limit, the smaller of its cgroup's limit and GOMEMLIMIT. Each cache's budget is its share of the
limit, capped at its size setting. While the heap's pressure on the limit passes HighWater, a
cache shrinks by ShrinkFrac at most once per Interval, down to MinMultiplier of its setting; below
LowWater it grows back towards its budget. Under GOMEMLIMIT the pressure is the live heap's share
of the heap goal, and under a cgroup limit alone the heap goal's share of the limit. It sizes the history cache while
history.cacheSizeBasedLimit is set, and the events cache while history.enableHostLevelEventsCache
is; a cache limited by entry count keeps its setting. Mode is disabled, shadow or enabled.`,
)

// DriveCache sizes c, a cache created at size bytes, from its share of the memory budget.
// While the controller is disabled, the cache keeps that size.
func DriveCache(
	plane *signals.Plane,
	collection *dynamicconfig.Collection,
	name string,
	size int,
	c cache.Resizable,
	handler metrics.Handler,
) {
	ctrl := &cacheController{cache: name, settings: Config.Get(collection), law: New(size)}
	driven := signals.DriveFunc(plane, func() int { return size }, ctrl, adaptive.Frozen.Get(collection), adaptive.HostKey(), handler)
	_, _ = driven.Subscribable()(c.SetMaxSize)
}

func (c *cacheController) Name() string {
	return "memory_budget_" + c.cache
}

func (c *cacheController) Envelope(static int) adaptive.Envelope[int] {
	s := c.settings()
	return adaptive.Envelope[int]{
		Mode:           s.Mode,
		RolloutPercent: s.RolloutPercent,
		Min:            max(1, int(s.MinMultiplier*float64(static))),
		Max:            static,
		MaxStepFrac:    s.MaxStepFrac,
	}
}

func (c *cacheController) Reset(size int) {
	c.law.Reset(size)
}

func (c *cacheController) Step(snap *signals.Snapshot, env adaptive.Envelope[int], _ int) (int, adaptive.Reason, any) {
	s := c.settings()
	params := Params{
		Min:        env.Min,
		Max:        env.Max,
		Share:      c.share(s),
		HighWater:  s.HighWater,
		LowWater:   s.LowWater,
		ShrinkFrac: s.ShrinkFrac,
		GrowFrac:   s.GrowFrac,
		Interval:   s.Interval,
	}
	in := Signals{
		Now:         snap.Time,
		CgroupLimit: snap.Memory.Limit,
		GoLimit:     snap.Memory.GoLimit,
		HeapGoal:    snap.Runtime.HeapGoal,
		HeapLive:    snap.Runtime.HeapLive,
	}
	size, reason := c.law.Step(params, in)
	return size, adaptive.Reason(reason), stepDetail{Params: params, Signals: in}
}

func (c *cacheController) share(s Settings) float64 {
	if c.cache == EventsCache {
		return s.EventsCacheShare
	}
	return s.HistoryCacheShare
}
