package matching

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
)

// wantsStats reports whether the scaler reads the task queue's stats.
func (sm *scaleManager) wantsStats() bool {
	s, ok := sm.partitionScaler.(PartitionStatsScaler)
	return ok && s.WantsStats()
}

// newStats starts the stats of one round of describing the partitions, if the scaler
// wants them.
func (sm *scaleManager) newStats(wanted bool, write int32) *PartitionScalerStats {
	if !wanted {
		return nil
	}
	return &PartitionScalerStats{Time: sm.timeSource.Now(), WritePartitions: int(write)}
}

// updateUnmanagedStats describes the partitions dynamic config gives, while managed
// scaling is off, for their stats alone.
// Called from backgroundWork only.
func (sm *scaleManager) updateUnmanagedStats(ctx context.Context, versions []string) {
	write := int32(max(1, sm.getWritePartitions()))
	stats := sm.newStats(true, write)
	for id := range write {
		callCtx, cancel := context.WithTimeout(ctx, ioTimeout)
		req := sm.describeRequest(id, versions)
		req.ReportStats = true
		req.ReportInternalTaskQueueStatus = false
		res, err := sm.matchingClient.DescribeTaskQueuePartition(callCtx, req)
		cancel()
		if err == nil {
			stats.add(res)
		}
	}
	sm.stats = stats
}

// add adds a partition's rates and backlog, over its physical queues. Stats that aren't
// wanted are nil.
func (s *PartitionScalerStats) add(res *matchingservice.DescribeTaskQueuePartitionResponse) {
	if s == nil {
		return
	}
	s.Described++
	for _, v := range res.GetVersionsInfoInternal() {
		queue := v.GetPhysicalTaskQueueInfo().GetTaskQueueStats()
		s.AddRate += queue.GetTasksAddRate()
		s.DispatchRate += queue.GetTasksDispatchRate()
		s.BacklogCount += queue.GetApproximateBacklogCount()
		s.BacklogAge = max(s.BacklogAge, queue.GetApproximateBacklogAge().AsDuration())
		s.RateLimited = s.RateLimited || queue.GetRateLimitingActive()
	}
}
