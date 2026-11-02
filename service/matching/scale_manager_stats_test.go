package matching

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// statsScaler wants stats while wants is set, and records what it's called with.
type statsScaler struct {
	wants  atomic.Bool
	mu     sync.Mutex
	inputs []PartitionScalerInput
}

func (s *statsScaler) WantsStats() bool { return s.wants.Load() }

func (s *statsScaler) OnTasks(in PartitionScalerInput) PartitionScalerDecision {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inputs = append(s.inputs, in)
	return PartitionScalerDecision{NoChange: true}
}

func (s *statsScaler) Stop() {}

func (s *statsScaler) calls() []PartitionScalerInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]PartitionScalerInput(nil), s.inputs...)
}

// statsDescribeResponse is a partition with an unversioned queue and a versioned one.
func statsDescribeResponse(addRate, dispatchRate float32, backlog int64, age time.Duration) *matchingservice.DescribeTaskQueuePartitionResponse {
	return &matchingservice.DescribeTaskQueuePartitionResponse{
		VersionsInfoInternal: map[string]*taskqueuespb.TaskQueueVersionInfoInternal{
			"": {PhysicalTaskQueueInfo: &taskqueuespb.PhysicalTaskQueueInfo{
				TaskQueueStats: &taskqueuepb.TaskQueueStats{
					TasksAddRate:            addRate,
					TasksDispatchRate:       dispatchRate,
					ApproximateBacklogCount: backlog,
					ApproximateBacklogAge:   durationpb.New(age),
				},
				InternalTaskQueueStatus: []*taskqueuespb.InternalTaskQueueStatus{{ApproximateBacklogCount: backlog}},
			}},
			"build-1": {PhysicalTaskQueueInfo: &taskqueuespb.PhysicalTaskQueueInfo{
				TaskQueueStats: &taskqueuepb.TaskQueueStats{TasksAddRate: 10, TasksDispatchRate: 10},
			}},
		},
	}
}

func newStatsTestScaleManager(
	t *testing.T,
	scaler PartitionScaler,
	matching matchingservice.MatchingServiceClient,
	writePartitions int,
	timeSource clock.TimeSource,
	settings dynamicconfig.PartitionScaleManagerSettings,
) *scaleManager {
	ctrl := gomock.NewController(t)
	userData := NewMockuserDataManager(ctrl)
	userData.EXPECT().GetUserData().Return(nil, nil, nil).AnyTimes()
	userData.EXPECT().SetPartitionScale(gomock.Any()).AnyTimes()
	return newScaleManager(
		context.Background(),
		tqid.MustNormalPartitionFromRpcName("tq", "ns-id", enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		userData,
		matching,
		scaler,
		timeSource,
		dynamicconfig.GetTypedPropertyFn(settings),
		dynamicconfig.GetIntPropertyFn(writePartitions),
		func() bool { return false },
	)
}

var statsTestSettings = dynamicconfig.PartitionScaleManagerSettings{
	MaxRate:            10,
	ShrinkRatio:        1,
	ShrinkDelta:        100,
	BatchSize:          1,
	BackgroundInterval: time.Hour,
	DrainBufferTime:    time.Hour,
}

func TestScaleManagerDescribesUnmanagedPartitionsForAStatsScaler(t *testing.T) {
	ctrl := gomock.NewController(t)
	matching := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	scaler := &statsScaler{}
	scaler.wants.Store(true)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(1_700_000_000, 0))
	sm := newStatsTestScaleManager(t, scaler, matching, 3, timeSource, statsTestSettings)

	// Managed scaling is off, so the partitions are dynamic config's 3, described for their
	// stats alone. Partition 2 isn't loaded.
	matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			req *matchingservice.DescribeTaskQueuePartitionRequest,
			_ ...grpc.CallOption,
		) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
			require.True(t, req.GetReportStats())
			require.False(t, req.GetReportInternalTaskQueueStatus())
			require.True(t, req.GetOnlyIfLoaded())
			switch req.GetTaskQueuePartition().GetNormalPartitionId() {
			case 0:
				return statsDescribeResponse(100, 80, 50, time.Second), nil
			case 1:
				return statsDescribeResponse(200, 150, 70, 3*time.Second), nil
			default:
				return nil, serviceerror.NewFailedPrecondition("partition was not loaded")
			}
		}).Times(3)

	sm.updateBacklogAndDrainState(context.Background())
	sm.callScaler()

	calls := scaler.calls()
	require.Len(t, calls, 1)
	require.Equal(t, &PartitionScalerStats{
		Time:            timeSource.Now(),
		WritePartitions: 3,
		Described:       2,
		AddRate:         320,
		DispatchRate:    250,
		BacklogCount:    120,
		BacklogAge:      3 * time.Second,
	}, calls[0].Stats)
	require.Zero(t, calls[0].ReadPartitions)
}

func TestScaleManagerDescribesManagedPartitionsWithStatsForAStatsScaler(t *testing.T) {
	ctrl := gomock.NewController(t)
	matching := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	scaleDB := NewMockscaleDB(ctrl)
	scaler := &statsScaler{}
	scaler.wants.Store(true)
	timeSource := clock.NewEventTimeSource()
	sm := newStatsTestScaleManager(t, scaler, matching, 4, timeSource, statsTestSettings)
	sm.scaleDB = scaleDB
	sm.setState(&persistencespb.PartitionScaleState{Target: 2, BacklogState: bitSet(nil).set(0).set(1)}, statsTestSettings)

	matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			req *matchingservice.DescribeTaskQueuePartitionRequest,
			_ ...grpc.CallOption,
		) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
			require.True(t, req.GetReportStats())
			require.True(t, req.GetReportInternalTaskQueueStatus())
			return statsDescribeResponse(100, 100, 5, time.Second), nil
		}).Times(2)
	scaleDB.EXPECT().UpdateScaleState(gomock.Any(), false).Return(nil).AnyTimes()

	sm.updateBacklogAndDrainState(context.Background())
	sm.callScaler()

	calls := scaler.calls()
	require.Len(t, calls, 1)
	require.Equal(t, 2, calls[0].ReadPartitions)
	require.NotNil(t, calls[0].Stats)
	require.Equal(t, 2, calls[0].Stats.WritePartitions)
	require.Equal(t, 2, calls[0].Stats.Described)
	require.InDelta(t, 220, calls[0].Stats.AddRate, 1e-3)
}

func TestScaleManagerLeavesStatsOutForOtherScalers(t *testing.T) {
	ctrl := gomock.NewController(t)
	matching := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	scaleDB := NewMockscaleDB(ctrl)
	scaler := &statsScaler{}
	timeSource := clock.NewEventTimeSource()
	sm := newStatsTestScaleManager(t, scaler, matching, 4, timeSource, statsTestSettings)
	sm.scaleDB = scaleDB

	// Unmanaged, nothing is described, and stats from when the scaler wanted them go.
	sm.stats = &PartitionScalerStats{Described: 1}
	sm.updateBacklogAndDrainState(context.Background())
	require.Nil(t, sm.stats)

	// Managed, the partitions are described without their stats.
	sm.setState(&persistencespb.PartitionScaleState{Target: 2}, statsTestSettings)
	matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			req *matchingservice.DescribeTaskQueuePartitionRequest,
			_ ...grpc.CallOption,
		) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
			require.False(t, req.GetReportStats())
			return statsDescribeResponse(100, 100, 5, time.Second), nil
		}).Times(2)
	scaleDB.EXPECT().UpdateScaleState(gomock.Any(), false).Return(nil).AnyTimes()
	sm.updateBacklogAndDrainState(context.Background())
	sm.callScaler()

	calls := scaler.calls()
	require.Len(t, calls, 1)
	require.Nil(t, calls[0].Stats)
}

func TestScaleManagerDescribesBeforeCallingAStatsScalerOnItsTimer(t *testing.T) {
	ctrl := gomock.NewController(t)
	matching := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	scaler := &statsScaler{}
	scaler.wants.Store(true)
	timeSource := clock.NewEventTimeSource()
	settings := statsTestSettings
	settings.BackgroundInterval = time.Minute
	sm := newStatsTestScaleManager(t, scaler, matching, 1, timeSource, settings)
	matching.EXPECT().DescribeTaskQueuePartition(gomock.Any(), gomock.Any()).
		Return(statsDescribeResponse(100, 100, 0, 0), nil).AnyTimes()

	sm.Start(nil, NewMockscaleDB(ctrl))
	defer func() {
		sm.Stop()
		<-sm.background.Done()
	}()
	await.RequireTrue(t, func() bool { return timeSource.NumTimers() >= 1 }, time.Second, time.Millisecond)
	timeSource.Advance(2*settings.BackgroundInterval + time.Millisecond)

	// The first call on the timer already has the stats it described.
	await.RequireTrue(t, func() bool { return len(scaler.calls()) >= 1 }, time.Second, time.Millisecond)
	require.NotNil(t, scaler.calls()[0].Stats)
	require.Equal(t, 1, scaler.calls()[0].Stats.Described)
}
