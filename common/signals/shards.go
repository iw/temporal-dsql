package signals

import (
	"time"

	"go.temporal.io/server/common/stats"
)

type (
	// ShardWrites is how long history's shard operations waited for their shard's I/O
	// semaphore, which lets history.shardIOConcurrency of a shard's writes reach persistence
	// at once. No controller reads it yet; traces record it.
	ShardWrites struct {
		WaitP90 time.Duration
		Samples int
	}

	// ShardWaitRecorder receives how long each of history's shard operations waited for its
	// shard's I/O semaphore.
	ShardWaitRecorder interface {
		RecordShardWait(wait time.Duration)
	}

	// shardWaits keeps the host's recent shard waits in 1s windows. wait is nil if the
	// windows couldn't be made.
	shardWaits struct {
		wait stats.TimeWindowedStats
	}
)

var _ ShardWaitRecorder = (*Plane)(nil)

func newShardWaits() *shardWaits {
	wait, err := stats.NewWindowedTDigest(stats.WindowConfig{WindowSize: queueWindowSize, WindowCount: queueWindowCount})
	if err != nil {
		return &shardWaits{}
	}
	return &shardWaits{wait: wait}
}

// RecordShardWait keeps how long one of history's shard operations waited for its shard's
// I/O semaphore, while the plane runs.
func (p *Plane) RecordShardWait(wait time.Duration) {
	if p.running.Load() && p.shards.wait != nil {
		p.shards.wait.Record(float64(wait)/float64(time.Millisecond), p.timeSource.Now())
	}
}

// snapshot returns the p90 wait over the newest windows.
func (s *shardWaits) snapshot(windows int, now time.Time) ShardWrites {
	recent, ok := s.wait.(stats.RecentQuantiler)
	if !ok {
		return ShardWrites{}
	}
	p90ms, samples := recent.QuantileRecent(0.90, min(windows, queueWindowCount), now)
	return ShardWrites{WaitP90: millis(p90ms), Samples: int(samples)}
}
