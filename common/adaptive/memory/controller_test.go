package memory_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive/memory"
)

const mib = 1 << 20

var (
	start  = time.Unix(1_700_000_000, 0)
	params = memory.Params{
		Min:        50 * mib,
		Max:        1000 * mib,
		Share:      0.3,
		HighWater:  0.85,
		LowWater:   0.6,
		ShrinkFrac: 0.25,
		GrowFrac:   0.1,
		Interval:   time.Minute,
	}
	// budget is the cache's share of a 1 GiB limit.
	budget = int(params.Share * float64(1024*mib))
)

// cgroup is a host with a 1 GiB cgroup limit and no GOMEMLIMIT, whose heap goal is twice
// its live heap.
func cgroup(at time.Duration, heapLive uint64) memory.Signals {
	return memory.Signals{Now: start.Add(at), CgroupLimit: 1024 * mib, HeapGoal: 2 * heapLive, HeapLive: heapLive}
}

func TestLimitIsTheSmallerOfTheCgroupsAndGOMEMLIMIT(t *testing.T) {
	for name, tc := range map[string]struct {
		s    memory.Signals
		want uint64
	}{
		"neither":         {want: 0},
		"cgroup only":     {s: memory.Signals{CgroupLimit: 1024 * mib}, want: 1024 * mib},
		"GOMEMLIMIT only": {s: memory.Signals{GoLimit: 900 * mib}, want: 900 * mib},
		"GOMEMLIMIT less": {s: memory.Signals{CgroupLimit: 1024 * mib, GoLimit: 900 * mib}, want: 900 * mib},
		"cgroup less":     {s: memory.Signals{CgroupLimit: 512 * mib, GoLimit: 900 * mib}, want: 512 * mib},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.s.Limit())
		})
	}
}

func TestPressureFollowsTheLimitThatBinds(t *testing.T) {
	for name, tc := range map[string]struct {
		s    memory.Signals
		want float64
	}{
		"no limit":                  {s: memory.Signals{HeapGoal: 80 * mib, HeapLive: 40 * mib}, want: 0},
		"cgroup: the goal's share":  {s: memory.Signals{CgroupLimit: 1024 * mib, HeapGoal: 512 * mib, HeapLive: 256 * mib}, want: 0.5},
		"cgroup below GOMEMLIMIT":   {s: memory.Signals{CgroupLimit: 512 * mib, GoLimit: 900 * mib, HeapGoal: 384 * mib, HeapLive: 192 * mib}, want: 0.75},
		"GOMEMLIMIT, with room":     {s: memory.Signals{GoLimit: 256 * mib, HeapGoal: 80 * mib, HeapLive: 40 * mib}, want: 0.5},
		"GOMEMLIMIT, back to back":  {s: memory.Signals{GoLimit: 72 * mib, HeapGoal: 40 * mib, HeapLive: 38 * mib}, want: 0.95},
		"GOGC off, goal at limit":   {s: memory.Signals{GoLimit: 1024 * mib, HeapGoal: 1000 * mib, HeapLive: 200 * mib}, want: 0.2},
		"GOMEMLIMIT, no collection": {s: memory.Signals{GoLimit: 1024 * mib}, want: 0},
	} {
		t.Run(name, func(t *testing.T) {
			require.InDelta(t, tc.want, tc.s.Pressure(), 1e-9)
		})
	}
}

func TestCacheMovesToItsBudget(t *testing.T) {
	c := memory.New(1000 * mib)

	// The cache's budget is 0.3 of the limit, below its 1000 MiB setting.
	size, reason := c.Step(params, cgroup(0, 200*mib))
	require.Equal(t, budget, size)
	require.Equal(t, memory.ReasonBudget, reason)

	// Between the waters, it holds.
	size, reason = c.Step(params, cgroup(time.Second, 400*mib))
	require.Equal(t, budget, size)
	require.Equal(t, memory.ReasonSteady, reason)
}

func TestCacheShrinksUnderPressureAtMostOncePerInterval(t *testing.T) {
	c := memory.New(300 * mib)

	// A heap goal of 1000 MiB is 0.98 of the limit.
	size, reason := c.Step(params, cgroup(0, 500*mib))
	require.Equal(t, 225*mib, size)
	require.Equal(t, memory.ReasonPressure, reason)

	size, reason = c.Step(params, cgroup(30*time.Second, 500*mib))
	require.Equal(t, 225*mib, size)
	require.Equal(t, memory.ReasonCooldown, reason)

	size, reason = c.Step(params, cgroup(time.Minute, 500*mib))
	require.Equal(t, 225*mib-225*mib/4, size)
	require.Equal(t, memory.ReasonPressure, reason)
}

func TestCacheShrinksWhileCollectionsRunBackToBack(t *testing.T) {
	// Under GOMEMLIMIT, the goal sits just above the live heap.
	c := memory.New(70 * mib)
	size, reason := c.Step(params, memory.Signals{Now: start, GoLimit: 256 * mib, HeapGoal: 100 * mib, HeapLive: 95 * mib})
	require.Equal(t, 70*mib-70*mib/4, size)
	require.Equal(t, memory.ReasonPressure, reason)

	// With GOGC off the goal sits at the limit whatever the heap holds, so the live heap
	// decides.
	size, reason = memory.New(200*mib).Step(params, memory.Signals{Now: start, GoLimit: 1024 * mib, HeapGoal: 1000 * mib, HeapLive: 200 * mib})
	require.Equal(t, 220*mib, size)
	require.Equal(t, memory.ReasonHeadroom, reason)
}

func TestCacheGrowsBackTowardsItsBudget(t *testing.T) {
	c := memory.New(200 * mib)

	size, reason := c.Step(params, cgroup(0, 200*mib))
	require.Equal(t, 220*mib, size)
	require.Equal(t, memory.ReasonHeadroom, reason)

	size, reason = c.Step(params, cgroup(30*time.Second, 200*mib))
	require.Equal(t, 220*mib, size)
	require.Equal(t, memory.ReasonCooldown, reason)

	// It grows no further than its budget.
	for i := 1; i <= 10; i++ {
		size, _ = c.Step(params, cgroup(time.Duration(i)*time.Minute, 200*mib))
	}
	require.Equal(t, budget, size)
}

func TestCacheHoldsWithoutAMemoryLimit(t *testing.T) {
	c := memory.New(300 * mib)
	size, reason := c.Step(params, memory.Signals{Now: start, HeapGoal: 900 * mib, HeapLive: 450 * mib})
	require.Equal(t, 300*mib, size)
	require.Equal(t, memory.ReasonNoData, reason)
}

func TestCacheStaysWithinItsBounds(t *testing.T) {
	c := memory.New(60 * mib)
	size, reason := c.Step(params, cgroup(0, 1000*mib))
	require.Equal(t, params.Min, size)
	require.Equal(t, memory.ReasonPressure, reason)

	// A small limit's budget is still at least Min.
	size, reason = memory.New(100*mib).Step(params, memory.Signals{Now: start, CgroupLimit: 64 * mib, HeapGoal: 20 * mib, HeapLive: 10 * mib})
	require.Equal(t, params.Min, size)
	require.Equal(t, memory.ReasonBudget, reason)
}
