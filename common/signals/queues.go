package signals

import (
	"sync"
	"time"

	"go.temporal.io/server/common/stats"
)

const (
	queueWindowSize  = time.Second
	queueWindowCount = 10
)

type (
	// Queue is the queueing delay of one of the host's task queues: how long its tasks
	// waited in the queue's scheduler before a worker took them.
	Queue struct {
		DelayP90 time.Duration
		Samples  int
	}

	// ScheduleLatencyRecorder receives how long each task attempt waited in its
	// scheduler, as history's executables report it.
	ScheduleLatencyRecorder interface {
		RecordScheduleLatency(latency time.Duration)
	}

	// queueDelays keeps each queue's recent delays in 1s windows.
	queueDelays struct {
		mu      sync.Mutex
		byQueue map[string]stats.TimeWindowedStats
	}

	queueRecorder struct {
		plane *Plane
		delay stats.TimeWindowedStats
		next  ScheduleLatencyRecorder
	}
)

// QueueRecorder returns the recorder for one of the host's task queues. While the plane
// runs, it keeps the queue's delays for the plane's snapshots. It passes every delay on
// to next, if there is one.
func (p *Plane) QueueRecorder(queue string, next ScheduleLatencyRecorder) ScheduleLatencyRecorder {
	p.queues.mu.Lock()
	defer p.queues.mu.Unlock()
	delay, ok := p.queues.byQueue[queue]
	if !ok {
		var err error
		delay, err = stats.NewWindowedTDigest(stats.WindowConfig{WindowSize: queueWindowSize, WindowCount: queueWindowCount})
		if err != nil {
			return next
		}
		p.queues.byQueue[queue] = delay
	}
	return &queueRecorder{plane: p, delay: delay, next: next}
}

func (r *queueRecorder) RecordScheduleLatency(latency time.Duration) {
	if r.plane.running.Load() {
		r.delay.Record(float64(latency)/float64(time.Millisecond), r.plane.timeSource.Now())
	}
	if r.next != nil {
		r.next.RecordScheduleLatency(latency)
	}
}

// snapshot returns each queue's p90 delay over the newest windows.
func (q *queueDelays) snapshot(windows int, now time.Time) map[string]Queue {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.byQueue) == 0 {
		return nil
	}
	queues := make(map[string]Queue, len(q.byQueue))
	for name, delay := range q.byQueue {
		var queue Queue
		if recent, ok := delay.(stats.RecentQuantiler); ok {
			p90ms, samples := recent.QuantileRecent(0.90, min(windows, queueWindowCount), now)
			queue = Queue{DelayP90: time.Duration(p90ms * float64(time.Millisecond)), Samples: int(samples)}
		}
		queues[name] = queue
	}
	return queues
}
