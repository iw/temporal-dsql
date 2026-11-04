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

// taskQueue is one of history's task queues: a scheduler whose workers take its tasks
// first in, first out, the delays its tasks waited for a worker, and its rescheduler.
type taskQueue struct {
	h           *history
	name        string
	workers     int
	busy        int
	waiting     fifo[*task]
	delays      windowedSamples
	rescheduler *rescheduler
}

func newTaskQueue(h *history, name string, seed uint64, host string) *taskQueue {
	q := &taskQueue{
		h:       h,
		name:    name,
		workers: h.cfg.Workers,
		delays:  windowedSamples{size: queueWindow, windows: make([]sampleWindow, queueWindows)},
	}
	q.rescheduler = newRescheduler(h.l, seed, host+"/"+name, h.trySubmit)
	return q
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
