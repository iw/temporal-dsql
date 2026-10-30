package persistence

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

// wrappedTimeout is how SQL stores return a query that its deadline interrupted.
var wrappedTimeout = serviceerror.NewUnavailable("GetWorkflowExecution: failed to get timer info. Error: timeout: context deadline exceeded")

func newTimeoutTestAggregator(t *testing.T) *healthSignalAggregatorImpl {
	t.Helper()
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
	return agg
}

// expiredContext is a call's context whose deadline has passed.
func expiredContext(t *testing.T) context.Context {
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return ctx
}

func TestRecordCallCountsTimeoutsTheStoreWrapped(t *testing.T) {
	agg := newTimeoutTestAggregator(t)
	for range 10 {
		agg.RecordCall(t.Context(), CallerSegmentMissing, 10*time.Millisecond, nil)
	}
	for range 5 {
		agg.RecordCall(expiredContext(t), CallerSegmentMissing, 2*time.Second, wrappedTimeout)
	}
	for range 5 { // failed while its deadline was still ahead
		agg.RecordCall(t.Context(), CallerSegmentMissing, 10*time.Millisecond, wrappedTimeout)
	}
	agg.RecordCall(expiredContext(t), CallerSegmentMissing, time.Second, nil) // succeeded late

	snap := agg.RecentSnapshot(3)
	require.Equal(t, 21, snap.Samples)
	require.InDelta(t, 5.0/21, snap.ErrorRatio, 1e-9)
	// The per-priority health limiters and the deep health check read the error itself, as before.
	require.Zero(t, agg.ErrorRatio())
}

func TestRecordCallLeavesLimiterRejectionsOut(t *testing.T) {
	agg := newTimeoutTestAggregator(t)
	agg.RecordCall(expiredContext(t), CallerSegmentMissing, 0, ErrPersistenceSystemLimitExceeded)
	require.Zero(t, agg.RecentSnapshot(3).Samples)
}

func TestMetricsClientRecordsTimeoutsByTheCallsDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	executionManager := NewMockExecutionManager(ctrl)
	agg := newTimeoutTestAggregator(t)
	client := NewExecutionPersistenceMetricsClient(
		executionManager,
		metrics.NoopMetricsHandler,
		agg,
		log.NewNoopLogger(),
		func() bool { return false },
	)

	ctx := expiredContext(t)
	request := &GetWorkflowExecutionRequest{ShardID: 1, NamespaceID: "namespace-id", WorkflowID: "workflow-id", RunID: "run-id"}
	executionManager.EXPECT().GetWorkflowExecution(ctx, request).Return(nil, wrappedTimeout)

	_, err := client.GetWorkflowExecution(ctx, request)
	require.ErrorIs(t, err, wrappedTimeout)
	snap := agg.RecentSnapshot(3)
	require.Equal(t, 1, snap.Samples)
	require.InDelta(t, 1.0, snap.ErrorRatio, 1e-9)
}
