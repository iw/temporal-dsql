package adaptivesim

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

type (
	// worker is the SDK worker of the matching model. Its workflow and activity pollers
	// wait on matching's task queues. A poller that takes a task is held until history has
	// recorded the task as started, which is one of history's writes; the worker then
	// processes the task and responds, without a poller.
	worker struct {
		l     *loop
		cfg   Matching
		m     *matchingHost
		steps []stepKind
	}

	// flow is a workflow of the matching model, on the history host that owns its shard.
	// Its start is a write, and each of its steps a workflow task, an activity or a timer,
	// in the order steps gives them.
	flow struct {
		id      int64
		shard   int
		host    *process
		started time.Time
		step    int
	}

	stepKind int
)

const (
	workflowStep stepKind = iota
	activityStep
	timerStep
)

func newWorker(l *loop, cfg Matching, m *matchingHost) *worker {
	w := &worker{l: l, cfg: cfg, m: m, steps: cfg.steps()}
	pollers := map[enumspb.TaskQueueType]int{
		enumspb.TASK_QUEUE_TYPE_WORKFLOW: cfg.WorkflowPollers,
		enumspb.TASK_QUEUE_TYPE_ACTIVITY: cfg.ActivityPollers,
	}
	for _, q := range m.queues {
		q.dispatch = func(t *matchingTask) {
			l.soon(func() { w.record(q, t) })
		}
		for range pollers[q.kind] {
			q.poll()
		}
	}
	return w
}

// steps are a workflow's steps after its start: a workflow task, then an activity and a
// workflow task for each of Activities, then, with a Timer, the timer and a last
// workflow task.
func (m Matching) steps() []stepKind {
	steps := []stepKind{workflowStep}
	for range m.Activities {
		steps = append(steps, activityStep, workflowStep)
	}
	if m.Timer > 0 {
		steps = append(steps, timerStep, workflowStep)
	}
	return steps
}

// start starts a workflow: history writes it, which adds a transfer task for its first
// workflow task.
func (w *worker) start(f *flow) {
	stats := &f.host.workers.stats
	if f.started.IsZero() {
		f.started = w.l.now
		stats.Started++
	}
	h := f.host.history
	h.write(f.id, f.shard, w.l.now.Add(h.cfg.Deadline), func(fl failure) {
		if fl != ok {
			stats.StartFailures++
			w.l.after(w.cfg.RetryDelay, func() { w.start(f) })
			return
		}
		h.cache.put(f.id)
		w.schedule(f)
	})
}

// schedule starts the workflow's step. A workflow task or an activity is a transfer task,
// which adds it to its task queue in matching. A timer is a timer task when it fires,
// which writes and schedules the workflow task after it.
func (w *worker) schedule(f *flow) {
	h := f.host.history
	switch kind := w.steps[f.step]; kind {
	case timerStep:
		w.l.after(w.cfg.Timer, func() {
			h.arrive(&task{wf: f.id, shard: f.shard, queue: h.queue(timerQueue), fired: func() {
				f.step++
				w.schedule(f)
			}})
		})
	default:
		q := w.m.queue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		if kind == activityStep {
			q = w.m.queue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
		}
		h.arrive(&task{wf: f.id, shard: f.shard, queue: h.queue(transferQueue), push: func() {
			q.add(&matchingTask{flow: f})
		}})
	}
}

// record is a poller holding the task while history records it as started. The poller
// then polls again, and the worker processes the task and responds. A task that history
// can't record goes back to matching after RetryDelay.
func (w *worker) record(q *matchingQueue, t *matchingTask) {
	f := t.flow
	h := f.host.history
	held := w.l.now
	h.execute(f.id, f.shard, w.l.now.Add(h.cfg.Deadline), func(fl failure) {
		s := secondOf(&q.result.PerSecond, w.l.now)
		s.Polls++
		s.Held += w.l.now.Sub(held)
		q.poll()
		if fl != ok {
			f.host.workers.stats.ResponseFailures++
			w.l.after(w.cfg.RetryDelay, func() { q.add(t) })
			return
		}
		process := w.cfg.Process
		if q.kind == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			process = w.cfg.Activity
		}
		w.l.after(process, func() { w.respond(f) })
	})
}

// respond is the worker's response to the step's task, which history writes. The next step
// follows; a response that fails schedules the step again after RetryDelay, standing in
// for the task's timeout.
func (w *worker) respond(f *flow) {
	h := f.host.history
	h.execute(f.id, f.shard, w.l.now.Add(h.cfg.Deadline), func(fl failure) {
		stats := &f.host.workers.stats
		if fl != ok {
			stats.ResponseFailures++
			w.l.after(w.cfg.RetryDelay, func() { w.schedule(f) })
			return
		}
		f.step++
		if f.step < len(w.steps) {
			w.schedule(f)
			return
		}
		f.host.workers.completed(f.started)
	})
}
