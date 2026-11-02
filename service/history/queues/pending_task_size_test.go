package queues_test

import (
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/adaptive/memory"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type meterProvider struct{ meter metric.Meter }

func (p *meterProvider) GetMeter() metric.Meter { return p.meter }
func (p *meterProvider) Stop(log.Logger)        {}

// The memory budget counts BytesPerTask for each task the queues may hold. A pending
// task is the task loaded from persistence, its executable, with metrics handlers tagged
// for it, and its entry in its slice's tracker. OpenTelemetry's handler costs the most.
func TestPendingTaskFitsTheMemoryBudgetsBytesPerTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {ShardCount: 1},
	}).AnyTimes()
	meter := sdkmetrics.NewMeterProvider(sdkmetrics.WithReader(sdkmetrics.NewManualReader())).Meter("test")
	handler, err := metrics.NewOtelMetricsHandler(log.NewNoopLogger(), &meterProvider{meter: meter}, metrics.ClientConfig{}, false)
	require.NoError(t, err)
	factory := queues.NewExecutableFactory(
		queues.NewMockExecutor(ctrl),
		queues.NewMockScheduler(ctrl),
		queues.NewMockRescheduler(ctrl),
		queues.NewNoopPriorityAssigner(),
		clock.NewRealTimeSource(),
		namespaceRegistry,
		clusterMetadata,
		chasm.NewRegistry(log.NewTestLogger()),
		queues.GetTaskTypeTagValue,
		log.NewNoopLogger(),
		handler,
		telemetry.NoopTracer,
		nil,
		func() bool { return false },
		func() int { return math.MaxInt },
		func() bool { return false },
		func() string { return "" },
		nil,
	)

	workflowKey := definition.NewWorkflowKey(uuid.NewString(), "order-processing-"+uuid.NewString(), uuid.NewString())
	serializer := serialization.NewSerializer()
	for name, task := range map[string]tasks.Task{
		"transfer": &tasks.ActivityTask{
			WorkflowKey:         workflowKey,
			VisibilityTimestamp: time.Now(),
			TaskQueue:           "order-processing-task-queue",
			ScheduledEventID:    5,
		},
		"timer": &tasks.UserTimerTask{
			WorkflowKey:         workflowKey,
			VisibilityTimestamp: time.Now(),
			EventID:             5,
		},
	} {
		t.Run(name, func(t *testing.T) {
			blob, err := serializer.SerializeTask(task)
			require.NoError(t, err)
			const n = 10000
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			pending := make(map[tasks.Key]queues.Executable)
			for i := range n {
				loaded, err := serializer.DeserializeTask(task.GetCategory(), blob)
				require.NoError(t, err)
				loaded.SetTaskID(int64(i))
				pending[loaded.GetKey()] = factory.NewExecutable(loaded, queues.DefaultReaderId)
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			runtime.KeepAlive(pending)

			bytesPerTask := float64(after.HeapAlloc-before.HeapAlloc) / n
			t.Logf("a pending %s task holds %.0f bytes", name, bytesPerTask)
			require.Less(t, bytesPerTask, float64(memory.DefaultSettings.BytesPerTask))
		})
	}
}
