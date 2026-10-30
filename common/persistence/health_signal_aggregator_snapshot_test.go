package persistence

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestRecentSnapshotCountsStoreFacts(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	t.Cleanup(func() { handler.StopCapture(capture) })
	agg := NewHealthSignalAggregator(
		true,
		func() bool { return true },
		time.Minute,
		1000,
		handler,
		log.NewNoopLogger(),
		time.Second,
		10,
	)
	t.Cleanup(agg.emitMetricsTimer.Stop)

	agg.ReportStoreFact(StoreFactThrottled, 2)
	agg.ReportStoreFact(StoreFactContention, 1)
	agg.ReportStoreFact(StoreFactQueued, 5)
	agg.ReportStoreFact(StoreFactAcquired, 20)
	agg.ReportStoreFact(StoreFactLimit, 0) // nothing to count
	agg.ReportStoreFact(numStoreFacts, 1)  // not a fact
	agg.Record(CallerSegmentMissing, 10*time.Millisecond, nil)

	snap := agg.RecentSnapshot(3)
	require.Equal(t, 2, snap.Throttled)
	require.Equal(t, 1, snap.Contention)
	require.Zero(t, snap.Limit)
	require.Equal(t, 5, snap.Queued)
	require.Equal(t, 20, snap.Acquired)
	require.Equal(t, 1, snap.Samples)

	recordings := capture.Snapshot()["persistence_store_facts"]
	require.Len(t, recordings, 4)
	require.Equal(t, int64(2), recordings[0].Value)
	require.Equal(t, "throttled", recordings[0].Tags["fact"])
	require.Equal(t, "queued", recordings[2].Tags["fact"])
	require.Equal(t, "acquired", recordings[3].Tags["fact"])
}

func TestRecentSnapshotDeclaredFactsAndThrottledThroughput(t *testing.T) {
	agg := NewHealthSignalAggregator(
		true,
		func() bool { return true },
		time.Minute,
		1000,
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
		time.Second,
		10,
	)
	t.Cleanup(agg.emitMetricsTimer.Stop)
	require.False(t, agg.RecentSnapshot(3).StoreReportsFacts)

	agg.DeclareStoreFacts()
	for range 10 {
		agg.Record(CallerSegmentMissing, 10*time.Millisecond, nil)
	}
	// Throttled calls come back as ResourceExhausted, which the store never handled.
	for range 4 {
		agg.Record(CallerSegmentMissing, 0, &serviceerror.ResourceExhausted{Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED})
		agg.ReportStoreFact(StoreFactThrottled, 1)
	}

	snap := agg.RecentSnapshot(3)
	require.True(t, snap.StoreReportsFacts)
	require.Equal(t, 14, snap.Samples)
	require.Equal(t, 4, snap.Throttled)
	require.InDelta(t, 10.0, snap.Throughput*snap.Span.Seconds(), 1e-6)
}

func TestRecentSnapshotLeavesOutLimiterRejections(t *testing.T) {
	agg := NewHealthSignalAggregator(
		true,
		func() bool { return true },
		time.Minute,
		1000,
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
		time.Second,
		10,
	)
	t.Cleanup(agg.emitMetricsTimer.Stop)

	for range 100 {
		agg.Record(CallerSegmentMissing, 20*time.Millisecond, nil)
	}
	for _, rejection := range []error{
		ErrPersistenceSystemLimitExceeded,
		ErrPersistenceNamespaceLimitExceeded,
		ErrPersistenceNamespaceShardLimitExceeded,
	} {
		for range 100 {
			agg.Record(CallerSegmentMissing, 0, rejection)
		}
	}

	snap := agg.RecentSnapshot(3)
	require.Equal(t, 100, snap.Samples)
	require.InDelta(t, float64(20*time.Millisecond), float64(snap.P99), float64(time.Millisecond))
	require.Zero(t, snap.ErrorRatio)

	// The per-priority health limiters still see every call.
	require.InDelta(t, 5.0, agg.AverageLatency(), 0.001)
	require.Zero(t, agg.ErrorRatio())
}

func TestIsPersistenceLimitRejection(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"nil":                 {err: nil},
		"system limit":        {err: ErrPersistenceSystemLimitExceeded, want: true},
		"wrapped limit":       {err: fmt.Errorf("wrapped: %w", ErrPersistenceNamespaceLimitExceeded), want: true},
		"store overloaded":    {err: &serviceerror.ResourceExhausted{Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED}},
		"persistence timeout": {err: &TimeoutError{Msg: "timeout"}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, isPersistenceLimitRejection(tc.err))
		})
	}
}
