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
)

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
