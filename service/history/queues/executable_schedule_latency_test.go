package queues_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type recordedScheduleLatencies []time.Duration

func (r *recordedScheduleLatencies) RecordScheduleLatency(latency time.Duration) {
	*r = append(*r, latency)
}

func TestExecutableFactoryRecordsScheduleLatency(t *testing.T) {
	ctrl := gomock.NewController(t)
	executor := queues.NewMockExecutor(ctrl)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {ShardCount: 1},
	}).AnyTimes()
	timeSource := clock.NewEventTimeSource()
	var recorded recordedScheduleLatencies

	factory := queues.NewExecutableFactory(
		executor,
		queues.NewMockScheduler(ctrl),
		queues.NewMockRescheduler(ctrl),
		queues.NewNoopPriorityAssigner(),
		timeSource,
		namespaceRegistry,
		clusterMetadata,
		chasm.NewRegistry(log.NewTestLogger()),
		queues.GetTaskTypeTagValue,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		telemetry.NoopTracer,
		nil,
		func() bool { return false },
		func() int { return math.MaxInt },
		func() bool { return false },
		func() string { return "" },
		&recorded,
	)
	executable := factory.NewExecutable(
		tasks.NewFakeTask(
			definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID),
			tasks.CategoryTransfer,
			timeSource.Now(),
		),
		queues.DefaultReaderId,
	)

	now := time.Now()
	timeSource.Update(now)
	executable.SetScheduledTime(now)
	timeSource.Update(now.Add(100 * time.Millisecond))
	executor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{ExecutedAsActive: true})
	require.NoError(t, executable.Execute())

	require.Equal(t, recordedScheduleLatencies{100 * time.Millisecond}, recorded)
}
