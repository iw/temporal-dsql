package adaptivesim

import (
	"slices"
	"time"
)

// op is a persistence call history makes.
type op int

const (
	// readOp is GetWorkflowExecution.
	readOp op = iota
	// writeOp is UpdateWorkflowExecution, or CreateWorkflowExecution for a start.
	writeOp
)

type (
	// client is history's persistence client: each call passes the persistence rate
	// limiter, the health signal aggregator records each attempt, and a call that returns
	// Unavailable is retried once, after RetryDelay.
	client struct {
		l          *loop
		limiter    *capacityLimiter
		health     *health
		store      *store
		retryDelay time.Duration
		reads      CallStats
		writes     CallStats
	}

	// CallStats counts a kind of persistence call's attempts and how they ended.
	CallStats struct {
		Tried, OK, Unavailable, Exhausted, Limited int
	}
)

func (c *client) call(o op, deadline time.Time, done func(failure)) {
	c.attempt(o, deadline, 1, done)
}

func (c *client) attempt(o op, deadline time.Time, n int, done func(failure)) {
	start := c.l.now
	if !start.Before(deadline) {
		done(timedOut)
		return
	}
	stats, run := &c.reads, c.store.readWorkflow
	if o == writeOp {
		stats, run = &c.writes, c.store.writeWorkflow
	}
	if !c.limiter.allow(start) {
		stats.Limited++
		c.health.recordCall(start, 0, limited, false)
		c.l.soon(func() { done(limited) })
		return
	}
	stats.Tried++
	run(deadline, func(f failure) {
		now := c.l.now
		c.health.recordCall(now, now.Sub(start), f, f != ok && !now.Before(deadline))
		switch f {
		case ok:
			stats.OK++
		case exhausted:
			stats.Exhausted++
		default:
			stats.Unavailable++
		}
		if f == unavailable && n == 1 {
			c.l.after(c.retryDelay, func() { c.attempt(o, deadline, n+1, done) })
			return
		}
		done(f)
	})
}

type (
	// history is a history host's task processing: its task queues, each with a scheduler
	// of its own workers and a rescheduler for the tasks the scheduler refuses; the
	// schedulers' rate limiter, while it's on; shards whose writes run one at a time; and
	// a cache of the workflows whose state it has read.
	history struct {
		l         *loop
		cfg       History
		client    *client
		health    *health
		scheduler *schedulerLimiter // nil while the scheduler's rate limiter is off
		queues    []*taskQueue
		// recording reports whether the plane keeps the queues' delays: it does once it
		// runs, as its queue recorders do.
		recording func() bool
		shards    []shard
		shardLog  ShardStats
		cache     *workflowCache
		// loaded is the tasks the queues hold in memory, and stored those they left in the
		// store while their shards' queues were at their pending-task limits.
		loaded, stored int
		stats          TaskStats
		lat            []time.Duration
		// started is when the host started, and reloadTotal how many tasks it reloads.
		started     time.Time
		reloadTotal int
	}

	// task is one of history's tasks, for a workflow of its own, in one of the queues. An
	// attempt reads the workflow's state if it isn't cached, then writes, except a
	// transfer task of the matching model's, which pushes a task to matching instead.
	task struct {
		wf               int64
		shard            int
		queue            *taskQueue
		arrived, queued  time.Time
		attempts, denied int
		reload           bool
		// push adds the workflow's task to matching, in place of the write. fired follows
		// a timer task's write.
		push, fired func()
	}

	shard struct {
		held    bool
		waiters fifo[*shardWaiter]
		live    int
	}

	shardWaiter struct {
		grant  func()
		expire *event
		gone   bool
	}

	// ShardStats are the host's shards' writes, each second after the run's start.
	ShardStats struct {
		Shards    int
		PerSecond []ShardSecond
	}

	// ShardSecond is the writes that took their shard's lock in a second, and how long
	// they waited for it, and the time the shards were held for writes during the second.
	ShardSecond struct {
		Writes       int
		Waited, Busy time.Duration
	}

	// TaskStats counts a host's tasks: those that arrived and completed, their attempts,
	// and how long they took from arrival to completion.
	TaskStats struct {
		Arrived, Completed, Attempts, Failed int
		// Throttled counts the submissions the scheduler's rate limiter refused.
		Throttled int
		// The reload's tasks, and when the last of them completed, after the host started.
		ReloadArrived, ReloadCompleted int
		ReloadCleared                  time.Duration
		// Backlog is the tasks that had arrived and not completed when the load ended.
		Backlog  int
		P50, P99 time.Duration
	}
)

func newHistory(l *loop, cfg History, c *client, h *health, seed uint64, name string) *history {
	hs := &history{
		l:         l,
		cfg:       cfg,
		client:    c,
		health:    h,
		recording: func() bool { return false },
		shards:    make([]shard, cfg.Shards),
		shardLog:  ShardStats{Shards: cfg.Shards},
		cache:     newWorkflowCache(cfg.CacheMaxBytes, cfg.CacheEntryBytes),
	}
	for _, queue := range queueNames {
		hs.queues = append(hs.queues, newTaskQueue(hs, queue, seed, name))
	}
	return hs
}

// arrive adds a task.
func (h *history) arrive(t *task) {
	t.arrived = h.l.now
	h.stats.Arrived++
	if t.reload {
		h.stats.ReloadArrived++
	}
	t.queue.load(t)
}

// submit gives a task to the scheduler, as the queue reader does. A task the scheduler
// refuses waits in the rescheduler for its executable's reschedule backoff,
// UnavailableBackoff at its attempt.
func (h *history) submit(t *task) {
	if !h.trySubmit(t) {
		t.queue.rescheduler.add(t, h.l.now.Add(h.cfg.UnavailableBackoff.delay(t.attempts)))
	}
}

// trySubmit is the scheduler's TrySubmit: the rate limiter refuses the task, or one of
// its queue's workers runs it once one is free.
func (h *history) trySubmit(t *task) bool {
	if h.scheduler != nil && !h.scheduler.allow(h.l.now) {
		h.stats.Throttled++
		return false
	}
	t.queued = h.l.now
	t.queue.add(t)
	return true
}

// submitRetry is the scheduler's hint to the rescheduler after a refusal, while its rate
// limiter is on.
func (h *history) submitRetry(now time.Time) (time.Duration, bool) {
	if h.scheduler == nil {
		return 0, false
	}
	return h.scheduler.submitRetry(now)
}

// run is a task attempt: read the workflow's state if it isn't cached, then write, within
// the attempt's deadline.
func (h *history) run(t *task) {
	now := h.l.now
	wait := now.Sub(t.queued)
	h.health.recordSchedule(now, wait)
	if h.recording() {
		t.queue.delays.record(float64(wait)/float64(time.Millisecond), now)
	}
	t.attempts++
	h.stats.Attempts++
	done := func(f failure) {
		if f == ok {
			h.complete(t)
		} else {
			h.fail(t, f)
		}
		t.queue.free()
	}
	deadline := now.Add(h.cfg.Deadline)
	if t.push != nil {
		h.read(t.wf, deadline, func(f failure) {
			if f == ok {
				t.push()
			}
			done(f)
		})
		return
	}
	h.execute(t.wf, t.shard, deadline, func(f failure) {
		if f == ok && t.fired != nil {
			t.fired()
		}
		done(f)
	})
}

// read reads the workflow's state, unless it's cached.
func (h *history) read(wf int64, deadline time.Time, done func(failure)) {
	if h.cache.get(wf) {
		done(ok)
		return
	}
	h.client.call(readOp, deadline, func(f failure) {
		if f == ok {
			h.cache.put(wf)
		}
		done(f)
	})
}

// execute reads the workflow's state if it isn't cached, then writes it under its shard's
// lock. A failure clears the workflow from the cache.
func (h *history) execute(wf int64, shard int, deadline time.Time, done func(failure)) {
	h.read(wf, deadline, func(f failure) {
		if f != ok {
			done(f)
			return
		}
		h.write(wf, shard, deadline, func(f failure) {
			if f != ok {
				h.cache.remove(wf)
			}
			done(f)
		})
	})
}

// queue is the task queue of the name.
func (h *history) queue(name string) *taskQueue {
	for _, q := range h.queues {
		if q.name == name {
			return q
		}
	}
	return nil
}

func (h *history) write(wf int64, shard int, deadline time.Time, done func(failure)) {
	asked := h.l.now
	h.lock(shard, deadline, func(locked bool) {
		if !locked {
			done(timedOut)
			return
		}
		took := h.l.now
		s := secondOf(&h.shardLog.PerSecond, took)
		s.Writes++
		s.Waited += took.Sub(asked)
		h.client.call(writeOp, deadline, func(f failure) {
			spread(&h.shardLog.PerSecond, took, h.l.now, func(s *ShardSecond, d time.Duration) { s.Busy += d })
			h.unlock(shard)
			done(f)
		})
	})
}

func (h *history) complete(t *task) {
	now := h.l.now
	t.queue.unload(t.shard)
	h.stats.Completed++
	h.lat = append(h.lat, now.Sub(t.arrived))
	if t.reload {
		h.stats.ReloadCompleted++
		if h.stats.ReloadCompleted == h.reloadTotal {
			h.stats.ReloadCleared = now.Sub(h.started)
		}
	}
}

// fail resubmits a task as history's task executor does: at once after Unavailable until
// its ImmediateAttempts attempt and then with backoff, and at once after its first
// ResourceExhausted and then with backoff.
func (h *history) fail(t *task, f failure) {
	h.stats.Failed++
	var delay time.Duration
	switch f {
	case exhausted, limited:
		if t.denied > 0 {
			delay = h.cfg.ExhaustedBackoff.delay(t.denied - 1)
		}
		t.denied++
	default:
		if t.attempts >= h.cfg.ImmediateAttempts {
			delay = h.cfg.UnavailableBackoff.delay(t.attempts - h.cfg.ImmediateAttempts)
		}
	}
	if delay == 0 {
		h.submit(t)
		return
	}
	h.l.after(delay, func() { h.submit(t) })
}

// lock takes a shard's write lock, first in, first out, or gives up at the deadline.
func (h *history) lock(i int, deadline time.Time, granted func(bool)) {
	s := &h.shards[i]
	if !s.held && s.live == 0 {
		s.held = true
		granted(true)
		return
	}
	w := &shardWaiter{grant: func() { granted(true) }}
	w.expire = h.l.at(deadline, func() {
		w.gone = true
		s.live--
		granted(false)
	})
	s.live++
	s.waiters.push(w)
}

func (h *history) unlock(i int) {
	s := &h.shards[i]
	for {
		w, ok := s.waiters.pop()
		if !ok {
			s.held = false
			return
		}
		if w.gone {
			continue
		}
		s.live--
		w.expire.cancel()
		h.l.soon(w.grant)
		return
	}
}

// result is the host's task stats, with the latency percentiles.
func (h *history) result() TaskStats {
	stats := h.stats
	stats.P50, stats.P99 = percentile(h.lat, 0.5), percentile(h.lat, 0.99)
	return stats
}

func percentile(values []time.Duration, q float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	i := int(q*float64(len(sorted))+0.5) - 1
	return sorted[min(max(i, 0), len(sorted)-1)]
}

type (
	// workers are a host's SDK workers: Pollers tasks held at once, each until history has
	// written the worker's response.
	workers struct {
		l       *loop
		cfg     Workers
		history *history
		free    int
		ready   fifo[*workflow]
		stats   WorkflowStats
		lat     []time.Duration
	}

	// workflow is a workflow of the coarse model: a start, then Steps worker tasks, each
	// dispatched to a poller and answered with a write.
	workflow struct {
		id      int64
		shard   int
		started time.Time
		step    int
	}

	// WorkflowStats counts a host's workflows.
	WorkflowStats struct {
		Started, Completed int
		// StartFailures and ResponseFailures count the starts and the workers' calls that
		// failed, and were tried again after RetryDelay: their responses, and with
		// matching, the tasks history couldn't record as started.
		StartFailures, ResponseFailures int
		// P50, P99 and Max are the workflows' times from start to completion.
		P50, P99, Max time.Duration
		// CompletedPerSecond is the workflows that completed in each second after the run's
		// start.
		CompletedPerSecond []int
	}
)

func newWorkers(l *loop, cfg Workers, h *history) *workers {
	return &workers{l: l, cfg: cfg, history: h, free: cfg.Pollers}
}

// start starts a workflow: a write of its own, as a frontend's StartWorkflowExecution
// reaches history.
func (w *workers) start(wf *workflow) {
	if wf.started.IsZero() {
		wf.started = w.l.now
		w.stats.Started++
	}
	w.history.write(wf.id, wf.shard, w.l.now.Add(w.history.cfg.Deadline), func(f failure) {
		if f != ok {
			w.stats.StartFailures++
			w.l.after(w.cfg.RetryDelay, func() { w.start(wf) })
			return
		}
		w.history.cache.put(wf.id)
		w.dispatch(wf)
	})
}

func (w *workers) dispatch(wf *workflow) {
	if w.free > 0 {
		w.free--
		w.poll(wf)
		return
	}
	w.ready.push(wf)
}

// poll is a poller holding a workflow's task while the worker processes it and history
// writes its response.
func (w *workers) poll(wf *workflow) {
	w.l.after(w.cfg.Process, func() {
		w.history.execute(wf.id, wf.shard, w.l.now.Add(w.history.cfg.Deadline), func(f failure) {
			w.release()
			if f != ok {
				w.stats.ResponseFailures++
				w.l.after(w.cfg.RetryDelay, func() { w.dispatch(wf) })
				return
			}
			wf.step++
			if wf.step < w.cfg.Steps {
				w.dispatch(wf)
				return
			}
			w.completed(wf.started)
		})
	})
}

func (w *workers) release() {
	if next, ok := w.ready.pop(); ok {
		w.l.soon(func() { w.poll(next) })
		return
	}
	w.free++
}

// completed counts a workflow that completed, and its time from its start.
func (w *workers) completed(started time.Time) {
	w.stats.Completed++
	*secondOf(&w.stats.CompletedPerSecond, w.l.now)++
	w.lat = append(w.lat, w.l.now.Sub(started))
}

func (w *workers) result() WorkflowStats {
	stats := w.stats
	stats.P50, stats.P99, stats.Max = percentile(w.lat, 0.5), percentile(w.lat, 0.99), percentile(w.lat, 1)
	return stats
}
