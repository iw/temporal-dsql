package adaptivesim

import (
	"math/rand/v2"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/partitions"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/signals"
)

const (
	// matchingService is the service a matching host's trace names.
	matchingService = "matching"
	// workflowNamespace and workerTaskQueue name the task queues the workflows' tasks go
	// through.
	workflowNamespace = "default"
	workerTaskQueue   = "adaptivesim"
	// trackerBucket and trackerInterval are matching's task trackers' buckets and span.
	trackerBucket   = 5 * time.Second
	trackerInterval = 30 * time.Second
)

type (
	// matchingHost is the matching service, on one host: a workflow and an activity task
	// queue, each with its partitions and its scale manager, and a signal plane of its own
	// that the partition scaler reads and records its decisions on.
	matchingHost struct {
		l      *loop
		cfg    Matching
		name   string
		pid    int
		plane  *plane
		queues []*matchingQueue

		scaleManager    dynamicconfig.TypedPropertyFnWithTaskQueueFilter[dynamicconfig.PartitionScaleManagerSettings]
		scalerSettings  dynamicconfig.TypedPropertyFnWithTaskQueueFilter[partitions.Settings]
		writePartitions dynamicconfig.IntPropertyFnWithTaskQueueFilter
		readPartitions  dynamicconfig.IntPropertyFnWithTaskQueueFilter
		frozen          dynamicconfig.BoolPropertyFn
	}

	// matchingQueue is a task queue's root partition: its partitions, the pollers waiting
	// on them, and the scale manager that sizes them with its scaler.
	matchingQueue struct {
		m          *matchingHost
		kind       enumspb.TaskQueueType
		partitions []*partition
		rng        *rand.Rand
		// dispatch hands a task to a poller.
		dispatch func(*matchingTask)
		// idle is the pollers waiting for a task, and backlog the tasks waiting for a
		// poller.
		idle, backlog int
		next          int // the read partition pollers take a task from next
		wake          *event
		wakeAt        time.Time

		// The scale manager's state.
		state        scaleState
		scaler       *scaler
		stats        *scalerStats // the partitions' stats as last described
		batch        int          // tasks added since the scaler was last called
		nextDecision time.Time

		result QueueResult
	}

	// partition is one of a task queue's partitions: its backlog, and the tasks it took and
	// dispatched, which matching's task trackers count. With a dispatch limit, it
	// dispatches its next task no sooner than nextFree.
	partition struct {
		backlog    fifo[*matchingTask]
		added      *taskTracker
		dispatched *taskTracker
		nextFree   time.Time
		slot       time.Duration // what each dispatch takes at the partition's limit, or 0 for none
	}

	// matchingTask is one of a workflow's tasks in matching.
	matchingTask struct {
		flow  *flow
		added time.Time
	}

	// scaleState is the task queue's partition scale state: the managed target, or 0 while
	// dynamic config sizes the partitions; when the target last changed; the partitions
	// that may have a backlog; and the capacity the scaler keeps in it.
	scaleState struct {
		target   int
		version  time.Time
		backlog  []bool
		capacity float64
		private  bool
	}

	// QueueResult is what one of matching's task queues did.
	QueueResult struct {
		Name                  string
		Added, Dispatched     int
		PeakBacklog           int
		PeakWait              time.Duration
		Read, Write, MostRead int
		// PerSecond is the task queue each second after the run's start.
		PerSecond []QueueSecond
	}

	// QueueSecond is a task queue's polls that ended in a second, having held their poller
	// while history recorded their task as started, how long they held it, and the
	// backlog at the second's end.
	QueueSecond struct {
		Polls   int
		Held    time.Duration
		Backlog int
	}

	// MatchingResult is what the matching host did.
	MatchingResult struct {
		Name      string
		PID       int
		Queues    []QueueResult
		Decisions Decisions
	}
)

func newMatchingHost(w *world, pid int) *matchingHost {
	l := w.l
	m := &matchingHost{l: l, cfg: w.sc.Matching, name: "matching-1", pid: pid}
	collection := newMatchingCollection(w.sc.Matching)
	m.scaleManager = dynamicconfig.MatchingPartitionScaleManager.Get(collection)
	m.scalerSettings = partitions.Config.Get(collection)
	m.writePartitions = dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(collection)
	m.readPartitions = dynamicconfig.MatchingNumTaskqueueReadPartitions.Get(collection)
	m.frozen = adaptive.Frozen.Get(collection)
	header := signals.Header{Service: matchingService, Host: m.name, PID: pid}
	m.plane = newPlane(l, w.sc.Signals, emptySource{}, header, w.opts.TraceDir, w.log)
	for _, kind := range []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY} {
		q := &matchingQueue{m: m, kind: kind, rng: randoms(w.sc.Seed, "matching/"+kind.String())}
		q.scaler = newScaler(m, q)
		q.result.Name = q.key()
		m.queues = append(m.queues, q)
	}
	return m
}

// newMatchingCollection is the matching host's dynamic config: the partition scaler's
// settings and the partitions, each its default where the scenario leaves it zero.
func newMatchingCollection(m Matching) *dynamicconfig.Collection {
	values := dynamicconfig.StaticClient{}
	if m.Scaler != (partitions.Settings{}) {
		values[partitions.Config.Key()] = m.Scaler
	}
	if m.Partitions > 0 {
		values[dynamicconfig.MatchingNumTaskqueueWritePartitions.Key()] = m.Partitions
		values[dynamicconfig.MatchingNumTaskqueueReadPartitions.Key()] = m.Partitions
	}
	return dynamicconfig.NewCollection(values, log.NewNoopLogger())
}

// start starts the plane and each task queue's scale manager.
func (m *matchingHost) start() {
	m.plane.start()
	for _, q := range m.queues {
		read, _ := q.counts()
		q.partition(read - 1)
		q.result.MostRead = read
		q.background()
		q.sample()
	}
}

// sample keeps the backlog at the end of each second.
func (q *matchingQueue) sample() {
	q.m.l.after(time.Second, func() {
		secondOf(&q.result.PerSecond, q.m.l.now.Add(-time.Nanosecond)).Backlog = q.backlog
		q.sample()
	})
}

// queue is the task queue of a kind.
func (m *matchingHost) queue(kind enumspb.TaskQueueType) *matchingQueue {
	for _, q := range m.queues {
		if q.kind == kind {
			return q
		}
	}
	return nil
}

func (m *matchingHost) result() *MatchingResult {
	r := &MatchingResult{Name: m.name, PID: m.pid, Decisions: m.plane.decisions}
	for _, q := range m.queues {
		res := q.result
		res.Read, res.Write = q.counts()
		r.Queues = append(r.Queues, res)
	}
	return r
}

// key names the task queue as the partition scaler's decisions do.
func (q *matchingQueue) key() string {
	return workflowNamespace + "/" + workerTaskQueue + "/" + q.kind.String()
}

func (q *matchingQueue) managerSettings() dynamicconfig.PartitionScaleManagerSettings {
	return q.m.scaleManager(workflowNamespace, workerTaskQueue, q.kind)
}

func (q *matchingQueue) staticWritePartitions() int {
	return max(1, q.m.writePartitions(workflowNamespace, workerTaskQueue, q.kind))
}

// counts are the partitions pollers poll and tasks are added to: dynamic config's while
// the scale manager has no state, and otherwise the state's, which keeps partitions
// above the target until they drain.
func (q *matchingQueue) counts() (read, write int) {
	read = q.state.readCount()
	if read == 0 {
		return max(1, q.m.readPartitions(workflowNamespace, workerTaskQueue, q.kind)), q.staticWritePartitions()
	}
	settings := q.managerSettings()
	shrink := max(1, min(int(float32(read)*settings.ShrinkRatio), int(settings.ShrinkDelta)))
	return read, max(q.state.target, read-shrink)
}

// partition is partition id, which matching loads, with new task trackers, the first time
// it's used.
func (q *matchingQueue) partition(id int) *partition {
	for len(q.partitions) <= id {
		p := &partition{added: newTaskTracker(q.m.l.now), dispatched: newTaskTracker(q.m.l.now)}
		if rate := q.m.cfg.PartitionRate; rate > 0 {
			p.slot = time.Duration(float64(time.Second) / rate)
		}
		q.partitions = append(q.partitions, p)
	}
	return q.partitions[id]
}

// add adds a task to one of the write partitions, picked at random as matching's client
// picks it.
func (q *matchingQueue) add(t *matchingTask) {
	now := q.m.l.now
	_, write := q.counts()
	p := q.partition(q.rng.IntN(write))
	t.added = now
	p.backlog.push(t)
	p.added.inc(now)
	q.backlog++
	q.batch++
	q.result.Added++
	q.result.PeakBacklog = max(q.result.PeakBacklog, q.backlog)
	q.match()
}

// poll adds a poller that waits for a task.
func (q *matchingQueue) poll() {
	q.idle++
	q.match()
}

// match gives waiting pollers the backlog's tasks, taking the read partitions in turn, as
// pollers spread across them. A partition at its dispatch limit waits for its next slot.
func (q *matchingQueue) match() {
	now := q.m.l.now
	read, _ := q.counts()
	for q.idle > 0 && q.backlog > 0 {
		p := q.nextPartition(now, read)
		if p == nil {
			q.wakeForSlot(now, read)
			return
		}
		t, _ := p.backlog.pop()
		q.backlog--
		q.idle--
		p.dispatched.inc(now)
		if p.slot > 0 {
			p.nextFree = now.Add(p.slot)
		}
		q.result.Dispatched++
		q.result.PeakWait = max(q.result.PeakWait, now.Sub(t.added))
		q.dispatch(t)
	}
}

func (q *matchingQueue) nextPartition(now time.Time, read int) *partition {
	for i := range read {
		id := (q.next + i) % read
		p := q.partition(id)
		if p.backlog.len() > 0 && !p.nextFree.After(now) {
			q.next = (id + 1) % read
			return p
		}
	}
	return nil
}

// wakeForSlot matches again when the first read partition with a backlog can dispatch.
func (q *matchingQueue) wakeForSlot(now time.Time, read int) {
	var at time.Time
	for id := range read {
		p := q.partition(id)
		if p.backlog.len() > 0 && p.nextFree.After(now) && (at.IsZero() || p.nextFree.Before(at)) {
			at = p.nextFree
		}
	}
	if at.IsZero() || q.wake != nil && q.wake.fn != nil && !q.wakeAt.After(at) {
		return
	}
	q.wake.cancel()
	q.wakeAt = at
	q.wake = q.m.l.at(at, q.match)
}

// background is the scale manager's background work, every BackgroundInterval with 5%
// jitter: when the scaler wants the partitions' stats, it describes them and then calls
// the scaler; otherwise it calls the scaler and then describes them, to drain.
func (q *matchingQueue) background() {
	q.m.l.after(jittered(q.rng, q.managerSettings().BackgroundInterval, 0.05), func() {
		if q.scaler.wantsStats() {
			q.describe()
			q.callScaler()
		} else {
			q.callScaler()
			q.describe()
		}
		q.background()
	})
}

// describe describes the read partitions, as the scale manager does: their stats, if the
// scaler wants them, and from DrainBufferTime after the target last changed, each
// partition above the target that has drained, which stops being read.
func (q *matchingQueue) describe() {
	now := q.m.l.now
	read := q.state.readCount()
	wants := q.scaler.wantsStats()
	if read == 0 {
		q.stats = nil
		if !wants {
			return
		}
		// Managed scaling is off: the partitions dynamic config gives, for their stats.
		write := q.staticWritePartitions()
		q.stats = &scalerStats{time: now, writePartitions: write}
		for id := range write {
			q.stats.add(q.partition(id), now)
		}
		return
	}
	target := q.state.target
	checkDrain := target > 0 && now.Sub(q.state.version) >= q.managerSettings().DrainBufferTime
	_, write := q.counts()
	var stats *scalerStats
	if wants {
		stats = &scalerStats{time: now, writePartitions: write}
	}
	var drained []int
	for id := range read {
		p := q.partition(id)
		stats.add(p, now)
		// A partition knows it drains once tasks no longer go to it.
		if checkDrain && id >= target && id >= write && q.state.has(id) && p.backlog.len() == 0 {
			drained = append(drained, id)
		}
	}
	q.stats = stats
	for _, id := range drained {
		q.state.backlog[id] = false
	}
}

// callScaler calls the scaler with the tasks added since the last call, and sets the
// target it decides, outside the cooldown after the last change.
func (q *matchingQueue) callScaler() {
	now := q.m.l.now
	if !q.nextDecision.IsZero() && now.Before(q.nextDecision) {
		return
	}
	tasks := q.batch
	q.batch = 0
	d := q.scaler.onTasks(scalerInput{
		numTasks:        tasks,
		currentTarget:   q.state.target,
		readPartitions:  q.state.readCount(),
		privateCapacity: q.state.capacity,
		private:         q.state.private,
		stats:           q.stats,
	})
	cleanup := d.newTarget == 0 && (len(q.state.backlog) > 0 || q.state.private)
	if d.noChange || d.newTarget == q.state.target && !cleanup {
		return
	}
	prev := q.state.target
	q.state.target = d.newTarget
	q.state.version = now
	q.state.capacity, q.state.private = d.capacity, d.private
	if d.newTarget == 0 {
		// Back to dynamic config's partitions.
		q.state = scaleState{version: now}
	}
	mayHaveBacklog := d.newTarget
	if prev == 0 && d.newTarget > 0 {
		// Managed scaling starts with dynamic config's partitions, which may have a
		// backlog.
		mayHaveBacklog = max(mayHaveBacklog, q.staticWritePartitions())
	}
	for id := range mayHaveBacklog {
		q.state.set(id)
	}
	read, _ := q.counts()
	q.partition(read - 1)
	q.result.MostRead = max(q.result.MostRead, read)
	q.nextDecision = now.Add(time.Duration(float32(time.Second) / q.managerSettings().MaxRate))
	q.match()
}

func (s *scaleState) readCount() int {
	n := 0
	for id, set := range s.backlog {
		if set {
			n = id + 1
		}
	}
	return max(s.target, n)
}

func (s *scaleState) has(id int) bool {
	return id < len(s.backlog) && s.backlog[id]
}

func (s *scaleState) set(id int) {
	for len(s.backlog) <= id {
		s.backlog = append(s.backlog, false)
	}
	s.backlog[id] = true
}

// taskTracker is matching's task tracker: tasks counted in 5 s buckets, and their rate
// over the last 30 s, or since it started.
type taskTracker struct {
	start, bucketStart time.Time
	buckets            []int
	pos                int
}

func newTaskTracker(now time.Time) *taskTracker {
	return &taskTracker{start: now, bucketStart: now, buckets: make([]int, int(trackerInterval/trackerBucket)+1)}
}

func (t *taskTracker) advance(now time.Time) {
	n := int(now.Sub(t.bucketStart) / trackerBucket)
	for range min(n, len(t.buckets)) {
		t.pos = (t.pos + 1) % len(t.buckets)
		t.buckets[t.pos] = 0
	}
	t.bucketStart = t.bucketStart.Add(time.Duration(n) * trackerBucket)
}

func (t *taskTracker) inc(now time.Time) {
	t.advance(now)
	t.buckets[t.pos]++
}

func (t *taskTracker) rate(now time.Time) float32 {
	t.advance(now)
	total := 0
	for _, n := range t.buckets {
		total += n
	}
	elapsed := min(now.Sub(t.bucketStart)+trackerInterval, now.Sub(t.start))
	if elapsed <= 0 {
		return 0
	}
	return float32(total) / float32(elapsed.Seconds())
}

type (
	// scalerStats are the partitions' stats as the scale manager sums them for the scaler.
	scalerStats struct {
		time                  time.Time
		writePartitions       int
		described             int
		addRate, dispatchRate float32
		backlogCount          int64
		backlogAge            time.Duration
		rateLimited           bool
	}

	scalerInput struct {
		numTasks, currentTarget, readPartitions int
		privateCapacity                         float64
		private                                 bool
		stats                                   *scalerStats
	}

	scalerDecision struct {
		noChange  bool
		newTarget int
		capacity  float64
		private   bool
	}

	// scaler is matching's learned partition scaler for one task queue, around the real
	// partitions.Controller, with matching.partitionScaler, the simple scaler it falls
	// back to, at its default, disabled: while the controller doesn't apply its target,
	// dynamic config sizes the partitions.
	scaler struct {
		m          *matchingHost
		q          *matchingQueue
		key        []byte
		law        *partitions.Controller
		loaded     bool
		loadedFrom float64
		reported   bool
		lastStats  time.Time
		lastTasks  time.Time
		managed    bool
	}
)

// add adds a partition's stats. Stats the scaler doesn't want are nil.
func (s *scalerStats) add(p *partition, now time.Time) {
	if s == nil {
		return
	}
	s.described++
	s.addRate += p.added.rate(now)
	s.dispatchRate += p.dispatched.rate(now)
	s.backlogCount += int64(p.backlog.len())
	if t, ok := p.backlog.peek(); ok {
		s.backlogAge = max(s.backlogAge, now.Sub(t.added))
	}
}

func newScaler(m *matchingHost, q *matchingQueue) *scaler {
	return &scaler{
		m:   m,
		q:   q,
		key: []byte(q.key()),
		law: partitions.New(0),
	}
}

func (s *scaler) mode() (partitions.Settings, adaptive.Mode) {
	settings := s.m.scalerSettings(workflowNamespace, workerTaskQueue, s.q.kind)
	return settings, adaptive.HostMode(settings.Mode, settings.RolloutPercent, s.key)
}

// wantsStats asks for the partitions' stats while the controller runs, unless no tasks
// arrived in the last StatsIdle and the controller doesn't size the partitions.
func (s *scaler) wantsStats() bool {
	settings, mode := s.mode()
	switch {
	case mode == adaptive.ModeOff:
		return false
	case mode == adaptive.ModeOn && s.managed:
		return true
	default:
		return !s.lastTasks.IsZero() && s.m.l.now.Sub(s.lastTasks) < settings.StatsIdle
	}
}

func (s *scaler) onTasks(in scalerInput) scalerDecision {
	if in.numTasks > 0 {
		s.lastTasks = s.m.l.now
	}
	var static scalerDecision // the simple scaler, disabled
	settings, mode := s.mode()
	s.managed = mode == adaptive.ModeOn && in.currentTarget > 0
	if mode == adaptive.ModeOff {
		s.lastStats = time.Time{}
		return s.drainFirst(in, static)
	}
	if mode == adaptive.ModeOn && !s.loaded {
		if in.private {
			s.law = partitions.New(in.privateCapacity)
			s.loadedFrom, s.reported = in.privateCapacity, false
		}
		s.loaded = true
	}
	if in.stats == nil || !in.stats.time.After(s.lastStats) {
		if mode == adaptive.ModeOn {
			return scalerDecision{noChange: true}
		}
		return s.drainFirst(in, static)
	}
	s.lastStats = in.stats.time
	target, held := s.step(settings, mode, in)
	if mode != adaptive.ModeOn {
		return s.drainFirst(in, static)
	}
	if held {
		return scalerDecision{noChange: true}
	}
	return scalerDecision{newTarget: target, capacity: s.law.Learned(), private: true}
}

// drainFirst keeps managed scaling on at dynamic config's partitions while partitions the
// controller added still drain.
func (s *scaler) drainFirst(in scalerInput, d scalerDecision) scalerDecision {
	if d.noChange || d.newTarget != 0 || in.currentTarget == 0 || !in.private {
		return d
	}
	static := s.q.staticWritePartitions()
	if in.readPartitions <= static {
		return d
	}
	return scalerDecision{newTarget: static, capacity: in.privateCapacity, private: true}
}

// step runs the law on the partitions' stats and reports its decision, as matching's
// learned scaler does.
func (s *scaler) step(settings partitions.Settings, mode adaptive.Mode, in scalerInput) (int, bool) {
	stats := in.stats
	snap := s.m.plane.signals.Latest()
	params := settings.Params(mode == adaptive.ModeShadow)
	count := stats.writePartitions
	if in.currentTarget > 0 {
		count = in.currentTarget
	}
	sig := partitions.Signals{
		Now:          stats.time,
		Partitions:   count,
		Described:    stats.described,
		AddRate:      float64(stats.addRate),
		DispatchRate: float64(stats.dispatchRate),
		RateLimited:  stats.rateLimited,
		BackingOff:   snap != nil && snap.PersistenceBackingOff(settings.Backoff()),
	}
	var target int
	var reason adaptive.Reason
	held := false
	switch {
	case s.m.frozen():
		target, reason, held = count, adaptive.ReasonFrozen, true
	case snap == nil || stats.described == 0:
		target, reason, held = count, adaptive.Reason(partitions.ReasonNoData), true
	default:
		var r partitions.Reason
		target, r = s.law.Step(params, sig)
		reason = adaptive.Reason(r)
	}
	static := s.q.staticWritePartitions()
	applied := static
	if mode == adaptive.ModeOn {
		applied = target
	}
	adaptive.Report(metrics.NoopMetricsHandler, s.m.plane.signals, adaptive.Decision{
		Controller: partitions.Name,
		Time:       stats.time,
		Mode:       mode,
		Reason:     reason,
		Static:     float64(static),
		Proposed:   float64(target),
		Applied:    float64(applied),
		Detail: partitions.Detail{
			Namespace:     workflowNamespace,
			TaskQueue:     workerTaskQueue,
			TaskQueueType: s.q.kind.String(),
			Params:        params,
			Signals:       sig,
			Capacity:      s.law.Capacity(params),
			Started:       !s.reported,
			Loaded:        s.loadedFrom,
			Held:          held,
			BacklogCount:  stats.backlogCount,
			BacklogAge:    stats.backlogAge,
		},
	})
	s.loadedFrom, s.reported = 0, true
	return target, held
}

// emptySource is a host's signals where the model has none: the snapshot carries only the
// decisions its plane records.
type emptySource struct{}

func (emptySource) Read(time.Time, int) signals.Snapshot { return signals.Snapshot{} }

func (emptySource) Idle() {}
