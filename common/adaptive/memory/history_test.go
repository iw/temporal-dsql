package memory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/signals"
)

const mib = 1 << 20

func TestCacheControllerStepsOnTheHostsMemory(t *testing.T) {
	settings := DefaultSettings
	c := &cacheController{cache: EventsCache, settings: func() Settings { return settings }, law: New(256 * mib)}
	require.Equal(t, "memory_budget_events_cache", c.Name())

	env := c.Envelope(256 * mib)
	require.Equal(t, adaptive.Envelope[int]{
		Mode:           adaptive.ModeNameDisabled,
		RolloutPercent: 100,
		Min:            int(settings.MinMultiplier * float64(256*mib)),
		Max:            256 * mib,
		MaxStepFrac:    0.25,
	}, env)

	snap := &signals.Snapshot{
		Time:    time.Unix(1_700_000_000, 0),
		Memory:  signals.Memory{Limit: 1024 * mib, GoLimit: 900 * mib},
		Runtime: signals.Runtime{HeapGoal: 400 * mib, HeapLive: 200 * mib},
	}
	size, reason, detail := c.Step(snap, env, 256*mib)
	require.Equal(t, int(settings.EventsCacheShare*float64(900*mib)), size, "the events cache's share of GOMEMLIMIT")
	require.Equal(t, adaptive.Reason(ReasonBudget), reason)
	require.Equal(t, stepDetail{
		Params: Params[int]{
			Min:          env.Min,
			Max:          env.Max,
			Share:        0.1,
			BytesPerUnit: 1,
			HighWater:    0.85,
			LowWater:     0.6,
			ShrinkFrac:   0.25,
			GrowFrac:     0.1,
			Interval:     time.Minute,
		},
		Signals: Signals{Now: snap.Time, CgroupLimit: 1024 * mib, GoLimit: 900 * mib, HeapGoal: 400 * mib, HeapLive: 200 * mib},
	}, detail)

	history := &cacheController{cache: HistoryCache, settings: func() Settings { return settings }, law: New(1000 * mib)}
	require.InDelta(t, 0.3, history.share(settings), 1e-9)
}
