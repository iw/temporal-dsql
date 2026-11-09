package adaptivesim

import (
	"time"

	"go.temporal.io/server/common/signals"
)

// History's task queues in the model, named as their worker count controllers name them.
const (
	transferQueue = "transfer"
	timerQueue    = "timer"
)

var queueNames = []string{transferQueue, timerQueue}

const (
	// queueWindow and queueWindows are the windows the plane keeps each queue's delays in.
	queueWindow  = time.Second
	queueWindows = 10
)

// taskQueue is one of history's task queues: each shard's reader, which loads the tasks
// the shard writes while its pending tasks are under the limit; a scheduler whose
// workers take its tasks first in, first out; the delays its tasks waited for a worker;
// and its rescheduler.
type taskQueue struct {
	h           *history
	name        string
	maxPending  func() int // a shard's pending-task limit; 0 sets none
	loaded      []int
	stored      []fifo[*task]
	workers     int
	busy        int
	waiting     fifo[*task]
	delays      windowedSamples
	rescheduler *rescheduler
}

func newTaskQueue(h *history, name string, seed uint64, host string) *taskQueue {
	q := &taskQueue{
		h:          h,
		name:       name,
		maxPending: func() int { return h.cfg.MaxPendingTasks },
		loaded:     make([]int, h.cfg.Shards),
		stored:     make([]fifo[*task], h.cfg.Shards),
		workers:    h.cfg.Workers,
		delays:     windowedSamples{size: queueWindow, windows: make([]sampleWindow, queueWindows)},
	}
	q.rescheduler = newRescheduler(h.l, seed, host+"/"+name, h.trySubmit, h.submitRetry)
	return q
}

// load is a task the shard wrote. The shard's reader loads it in turn, after the tasks
// the shard wrote before it.
func (q *taskQueue) load(t *task) {
	q.stored[t.shard].push(t)
	q.h.stored++
	q.fill(t.shard)
}

// unload ends a completed task's time in memory, which lets the shard's reader load more.
func (q *taskQueue) unload(shard int) {
	q.loaded[shard]--
	q.h.loaded--
	q.fill(shard)
}

// fill is the shard's reader: it loads the shard's tasks, oldest first, and submits them
// while the shard's pending tasks are under the limit, and leaves the rest in the store.
func (q *taskQueue) fill(shard int) {
	limit := q.maxPending()
	for q.stored[shard].len() > 0 && (limit <= 0 || q.loaded[shard] < limit) {
		t, _ := q.stored[shard].pop()
		q.h.stored--
		q.loaded[shard]++
		q.h.loaded++
		q.h.submit(t)
	}
}

// add gives a task to a free worker, or queues it.
func (q *taskQueue) add(t *task) {
	if q.busy < q.workers {
		q.busy++
		q.h.l.soon(func() { q.h.run(t) })
		return
	}
	q.waiting.push(t)
}

// free ends an attempt. Its worker takes the next task, unless the pool has shrunk below
// the workers it has, when it stops.
func (q *taskQueue) free() {
	if q.busy <= q.workers {
		if t, ok := q.waiting.pop(); ok {
			q.h.l.soon(func() { q.h.run(t) })
			return
		}
	}
	q.busy--
}

// setWorkers resizes the scheduler's pool, as its worker count's subscription does: new
// workers take waiting tasks at once, and workers over the count stop once their task
// ends.
func (q *taskQueue) setWorkers(n int) {
	q.workers = n
	for q.busy < q.workers {
		t, ok := q.waiting.pop()
		if !ok {
			return
		}
		q.busy++
		q.h.l.soon(func() { q.h.run(t) })
	}
}

// signal is the queue's delays as the plane reads them: the p90 over the newest windows.
func (q *taskQueue) signal(now time.Time, windows int) signals.Queue {
	p90, samples := q.delays.quantileRecent(0.90, min(windows, queueWindows), now)
	return signals.Queue{DelayP90: time.Duration(p90 * float64(time.Millisecond)), Samples: samples}
}
