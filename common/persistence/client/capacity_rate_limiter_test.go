package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
)

// fakeHealthSignals serves a fixed snapshot; the capacity limiter reads nothing else.
type fakeHealthSignals struct {
	persistence.HealthSignalAggregator
	snapshot persistence.HealthSnapshot
}

func (f *fakeHealthSignals) RecentSnapshot(int) persistence.HealthSnapshot {
	return f.snapshot
}

// countingLimiter stands in for the per-priority health limiters.
type countingLimiter struct {
	quotas.RequestRateLimiter
	allow bool
	calls int
}

func (c *countingLimiter) Allow(time.Time, quotas.Request) bool {
	c.calls++
	return c.allow
}

type capacityLimiterFixture struct {
	limiter *capacityAwareRateLimiter
	legacy  *countingLimiter
	signals *fakeHealthSignals
	params  dynamicconfig.DynamicRateLimitingParams
	now     time.Time
	metrics *metricstest.Capture
}

const testHostRPS = 1000.0

func newCapacityLimiterFixture(t *testing.T, mode string) *capacityLimiterFixture {
	t.Helper()
	handler := metricstest.NewCaptureHandler()
	f := &capacityLimiterFixture{
		legacy:  &countingLimiter{allow: true},
		signals: &fakeHealthSignals{snapshot: healthySnapshot()},
		params:  dynamicconfig.DefaultDynamicRateLimitingParams,
		now:     time.Unix(1_700_000_000, 0),
		metrics: handler.StartCapture(),
	}
	t.Cleanup(func() { handler.StopCapture(f.metrics) })
	f.params.Enabled = true
	f.params.Capacity.Mode = mode
	f.limiter = newCapacityAwareRateLimiter(
		f.legacy,
		func() float64 { return testHostRPS },
		RequestPriorityFn,
		func() float64 { return 0.2 },
		func() float64 { return 1 },
		f.signals,
		func() dynamicconfig.DynamicRateLimitingParams { return f.params },
		handler,
		log.NewNoopLogger(),
	)
	return f
}

// decisions lists the reasons the limiter recorded, in order.
func (f *capacityLimiterFixture) decisions() []string {
	var reasons []string
	for _, recording := range f.metrics.Snapshot()["persistence_capacity_decisions"] {
		reasons = append(reasons, recording.Tags["reason"])
	}
	return reasons
}

func healthySnapshot() persistence.HealthSnapshot {
	return persistence.HealthSnapshot{P99: 10 * time.Millisecond, Samples: 1000}
}

func apiRequest() quotas.Request {
	return quotas.NewRequest("UpdateWorkflowExecution", 1, "namespace", headers.CallerTypeAPI, 0, "")
}

// allowAfter advances the clock by d, which steps the controller once d reaches
// the step interval, and decides one API request.
func (f *capacityLimiterFixture) allowAfter(d time.Duration) bool {
	f.now = f.now.Add(d)
	return f.limiter.Allow(f.now, apiRequest())
}

func (f *capacityLimiterFixture) bucketRPS(callerType string) float64 {
	return f.limiter.buckets[CallerTypeDefaultPriority[callerType]].Rate()
}

func TestCapacityLimiterDisabledDelegatesToLegacy(t *testing.T) {
	for name, configure := range map[string]func(*dynamicconfig.DynamicRateLimitingParams){
		"default params": func(p *dynamicconfig.DynamicRateLimitingParams) {
			*p = dynamicconfig.DefaultDynamicRateLimitingParams
		},
		"mode disabled":           func(p *dynamicconfig.DynamicRateLimitingParams) { p.Capacity.Mode = capacityModeDisabled },
		"enabled without Enabled": func(p *dynamicconfig.DynamicRateLimitingParams) { p.Enabled = false },
		"unknown mode":            func(p *dynamicconfig.DynamicRateLimitingParams) { p.Capacity.Mode = "on" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newCapacityLimiterFixture(t, capacityModeEnabled)
			configure(&f.params)
			f.signals.snapshot = persistence.HealthSnapshot{P99: time.Second, ErrorRatio: 0.5, Samples: 1000}

			for i := range 10 {
				f.legacy.allow = i%2 == 0
				require.Equal(t, f.legacy.allow, f.allowAfter(time.Second))
			}
			require.Equal(t, 10, f.legacy.calls)
			require.Equal(t, capacityModeDisabled, f.limiter.mode)
			require.InDelta(t, testHostRPS, f.bucketRPS(headers.CallerTypeAPI), 0.001)
		})
	}
}

func TestCapacityLimiterEnabledDrivesSharedBuckets(t *testing.T) {
	f := newCapacityLimiterFixture(t, capacityModeEnabled)
	require.True(t, f.allowAfter(0)) // learns a 10ms baseline
	require.Zero(t, f.legacy.calls)

	f.signals.snapshot = persistence.HealthSnapshot{P99: 200 * time.Millisecond, Throughput: 600, Samples: 1000}
	f.allowAfter(time.Second)

	// Cut to 0.9 x delivered throughput; the operator bucket keeps its ratio.
	require.InDelta(t, 540, f.bucketRPS(headers.CallerTypeAPI), 0.001)
	require.InDelta(t, 108, f.bucketRPS(headers.CallerTypeOperator), 0.001)
	require.Zero(t, f.legacy.calls)
}

func TestCapacityLimiterShadowKeepsLegacyInCharge(t *testing.T) {
	f := newCapacityLimiterFixture(t, capacityModeShadow)
	f.allowAfter(0)
	f.signals.snapshot = persistence.HealthSnapshot{P99: 200 * time.Millisecond, Throughput: 600, Samples: 1000}
	f.allowAfter(time.Second)

	require.InDelta(t, 540, f.limiter.ctrl.Rate(), 0.001)
	require.InDelta(t, testHostRPS, f.bucketRPS(headers.CallerTypeAPI), 0.001)
	require.Equal(t, 2, f.legacy.calls)
}

func TestCapacityLimiterProbesOnlyWhileBinding(t *testing.T) {
	f := newCapacityLimiterFixture(t, capacityModeEnabled)
	f.allowAfter(0)
	f.allowAfter(time.Second)
	require.InDelta(t, testHostRPS, f.bucketRPS(headers.CallerTypeAPI), 0.001, "idle: no probe")

	// Exhaust the API bucket's burst so a request is denied: the limit is binding.
	denied := false
	for range 2 * int(testHostRPS) {
		if !f.limiter.Allow(f.now, apiRequest()) {
			denied = true
			break
		}
	}
	require.True(t, denied)
	f.allowAfter(time.Second)
	require.InDelta(t, 1.05*testHostRPS, f.bucketRPS(headers.CallerTypeAPI), 0.001)
}

func TestCapacityLimiterReturnsToLegacyWhenDisabled(t *testing.T) {
	f := newCapacityLimiterFixture(t, capacityModeEnabled)
	f.allowAfter(0)
	require.Zero(t, f.legacy.calls)

	f.params.Capacity.Mode = capacityModeDisabled
	f.legacy.allow = false
	require.False(t, f.allowAfter(time.Second))
	require.Equal(t, 1, f.legacy.calls)
}

func TestCapacityCeiling(t *testing.T) {
	hostRateFn := func() float64 { return testHostRPS }
	for name, tc := range map[string]struct {
		enabled       bool
		mode          string
		maxMultiplier float64
		want          float64
	}{
		"default":                  {mode: capacityModeDisabled, maxMultiplier: 3, want: testHostRPS},
		"shadow":                   {enabled: true, mode: capacityModeShadow, maxMultiplier: 3, want: testHostRPS},
		"enabled":                  {enabled: true, mode: capacityModeEnabled, maxMultiplier: 3, want: 3 * testHostRPS},
		"enabled, no headroom":     {enabled: true, mode: capacityModeEnabled, maxMultiplier: 0.5, want: testHostRPS},
		"capacity without Enabled": {mode: capacityModeEnabled, maxMultiplier: 3, want: testHostRPS},
	} {
		t.Run(name, func(t *testing.T) {
			params := dynamicconfig.DefaultDynamicRateLimitingParams
			params.Enabled = tc.enabled
			params.Capacity.Mode = tc.mode
			params.Capacity.MaxMultiplier = tc.maxMultiplier
			ceiling := capacityCeilingFn(hostRateFn, func() dynamicconfig.DynamicRateLimitingParams { return params })
			require.InDelta(t, tc.want, ceiling(), 0.001)
		})
	}
}

func TestCapacityLimiterActsOnStoreFacts(t *testing.T) {
	f := newCapacityLimiterFixture(t, capacityModeEnabled)
	f.signals.snapshot.StoreReportsFacts = true
	f.allowAfter(0) // learns a 10ms baseline

	// 5% of calls throttled by the store, while latency stays at the baseline.
	f.signals.snapshot = persistence.HealthSnapshot{
		P99: 10 * time.Millisecond, Samples: 1000, Throughput: 600, StoreReportsFacts: true, Throttled: 50,
	}
	f.allowAfter(time.Second)

	require.InDelta(t, 540, f.bucketRPS(headers.CallerTypeAPI), 0.001)
	require.Equal(t, []string{"idle", "throttled"}, f.decisions())
}

func TestCapacityLimiterReadsQueuedFactsAsAShareOfAcquires(t *testing.T) {
	f := newCapacityLimiterFixture(t, capacityModeEnabled)
	f.signals.snapshot.StoreReportsFacts = true
	f.allowAfter(0)

	// 400 queued facts against 1000 calls, but the calls took 4000 connections, so a
	// tenth of the acquires waited.
	f.signals.snapshot = persistence.HealthSnapshot{
		P99: 10 * time.Millisecond, Samples: 1000, Throughput: 600, StoreReportsFacts: true,
		Queued: 400, Acquired: 4000,
	}
	f.allowAfter(time.Second)
	require.InDelta(t, testHostRPS, f.bucketRPS(headers.CallerTypeAPI), 0.001)

	// Three tenths of the acquires waited.
	f.signals.snapshot.Queued = 1200
	f.allowAfter(time.Second)

	require.InDelta(t, 540, f.bucketRPS(headers.CallerTypeAPI), 0.001)
	require.Equal(t, []string{"idle", "idle", "queued"}, f.decisions())
}

func TestCapacityLimiterRunsLatencyInShadowForStoresWithFacts(t *testing.T) {
	f := newCapacityLimiterFixture(t, capacityModeEnabled)
	f.signals.snapshot.StoreReportsFacts = true
	f.allowAfter(0)

	f.signals.snapshot = persistence.HealthSnapshot{
		P99: 200 * time.Millisecond, Samples: 1000, Throughput: 600, StoreReportsFacts: true,
	}
	f.allowAfter(time.Second)

	require.InDelta(t, testHostRPS, f.bucketRPS(headers.CallerTypeAPI), 0.001)
	require.Equal(t, []string{"idle", "idle", capacityReasonLatencyShadow}, f.decisions())
}

func TestCapacityLimiterRecordsNoDecisionsWhenDisabled(t *testing.T) {
	f := newCapacityLimiterFixture(t, capacityModeDisabled)
	f.allowAfter(0)
	f.allowAfter(time.Second)
	require.Empty(t, f.decisions())
}
