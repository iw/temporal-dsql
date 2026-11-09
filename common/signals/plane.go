package signals

import (
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/stats"
)

type (
	// PersistenceHealth is what the plane reads of persistence health.
	PersistenceHealth interface {
		RecentSnapshot(windows int) persistence.HealthSnapshot
	}

	// RPCHealth is what the plane reads of RPC handler health, with latencies in
	// milliseconds. A reading's bool is false when there's none.
	RPCHealth interface {
		AverageLatency() float64
		LatencyQuantile(quantile float64) (float64, bool)
		ErrorRatio() (float64, bool)
	}

	// Source is what a plane reads its host's signals from.
	Source interface {
		// Read returns the host's signals at now, with persistence health and the queues'
		// delays over the newest windows. The plane sets the snapshot's time and decisions.
		Read(now time.Time, windows int) Snapshot
		// Idle is called on each tick that takes no snapshot, so that a signal covering the
		// time since the previous read starts again at the next.
		Idle()
	}

	// processSource reads the process a service runs in: its persistence and RPC health,
	// its queues' delays, the Go runtime and the process's cgroup.
	processSource struct {
		persistence PersistenceHealth
		rpc         RPCHealth // nil on hosts that don't record RPC health
		queues      *queueDelays
		shards      *shardWaits
		runtime     *runtimeSource
		memory      *cgroupMemory
	}

	// Plane publishes a Snapshot of the host every Settings.Interval while it's enabled.
	Plane struct {
		settings   func() Settings
		source     Source
		sink       adaptive.DecisionSink // nil unless NewPlane was given one
		queues     queueDelays
		shards     *shardWaits
		timeSource clock.TimeSource

		latest  atomic.Pointer[Snapshot]
		running atomic.Bool // whether the last tick took a snapshot

		mu          sync.Mutex // guards the fields below
		decisions   map[string]adaptive.Decision
		subscribers []subscriber
		nextID      int
		trace       *traceWriter

		started  atomic.Bool
		stopOnce sync.Once
		stopCh   chan struct{}
		doneCh   chan struct{}
	}

	subscriber struct {
		id     int
		fn     func(*Snapshot)
		active func() bool
	}
)

var _ adaptive.DecisionSink = (*Plane)(nil)

// newPlane returns a plane that reads the process it runs in.
func newPlane(
	settings func() Settings,
	persistenceHealth PersistenceHealth,
	rpcHealth RPCHealth,
	memory *cgroupMemory,
	timeSource clock.TimeSource,
	traceHeader Header,
	logger log.Logger,
) *Plane {
	p := planeWith(settings, timeSource, traceHeader, logger)
	p.source = &processSource{
		persistence: persistenceHealth,
		rpc:         rpcHealth,
		queues:      &p.queues,
		shards:      p.shards,
		runtime:     newRuntimeSource(),
		memory:      memory,
	}
	return p
}

// NewPlane returns a plane for a host that's simulated. It reads the host's signals from
// source, the queues' delays among them, so its QueueRecorders go unread, and it passes
// each decision it records on to sink, unless sink is nil. Each service's own plane comes
// from PlaneProvider and reads the process it runs in. tools/adaptivesim ticks a simulated
// host's plane with Tick, on the simulation's clock.
func NewPlane(
	settings func() Settings,
	source Source,
	sink adaptive.DecisionSink,
	timeSource clock.TimeSource,
	traceHeader Header,
	logger log.Logger,
) *Plane {
	p := planeWith(settings, timeSource, traceHeader, logger)
	p.source, p.sink = source, sink
	p.openTrace()
	return p
}

func planeWith(settings func() Settings, timeSource clock.TimeSource, traceHeader Header, logger log.Logger) *Plane {
	return &Plane{
		settings:   settings,
		queues:     queueDelays{byQueue: make(map[string]stats.TimeWindowedStats)},
		shards:     newShardWaits(),
		timeSource: timeSource,
		decisions:  make(map[string]adaptive.Decision),
		trace:      newTraceWriter(traceHeader, logger),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

// Latest returns the newest snapshot, or nil while the plane isn't running.
func (p *Plane) Latest() *Snapshot {
	return p.latest.Load()
}

// Subscribe calls fn with each new snapshot, on the plane's goroutine and in the order
// subscribers subscribed, until the returned function cancels it. While active, if it
// isn't nil, returns true, the plane runs even if Settings.Enabled is off.
func (p *Plane) Subscribe(fn func(*Snapshot), active func() bool) (cancel func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := p.nextID
	p.nextID++
	// Copied on write, so a tick can call the subscribers without holding the lock.
	p.subscribers = append(p.subscribers[:len(p.subscribers):len(p.subscribers)], subscriber{id: id, fn: fn, active: active})
	return func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.subscribers = deleteSubscriber(p.subscribers, id)
	}
}

func deleteSubscriber(subscribers []subscriber, id int) []subscriber {
	kept := make([]subscriber, 0, len(subscribers))
	for _, s := range subscribers {
		if s.id != id {
			kept = append(kept, s)
		}
	}
	return kept
}

// RecordDecision keeps a controller's decision for the next snapshot, writes it to the
// trace, and passes it on to the plane's sink, if it has one.
func (p *Plane) RecordDecision(d adaptive.Decision) {
	p.mu.Lock()
	p.decisions[d.Controller] = d
	p.trace.write(Record{Decision: &d})
	p.mu.Unlock()
	if p.sink != nil {
		p.sink.RecordDecision(d)
	}
}

// Tick takes and publishes a snapshot at now, if the plane is enabled or a subscriber
// is active. Only one goroutine may call it.
func (p *Plane) Tick(now time.Time) {
	s := p.settings()
	if !s.Enabled && !p.anyActive() {
		p.running.Store(false)
		p.latest.Store(nil)
		p.source.Idle()
		p.mu.Lock()
		p.trace.close()
		p.mu.Unlock()
		return
	}
	p.running.Store(true)

	read := p.source.Read(now, max(1, s.Windows))
	snap := &read
	snap.Time = now

	p.mu.Lock()
	snap.Decisions = maps.Clone(p.decisions)
	subscribers := p.subscribers
	p.trace.configure(s.TraceDirectory, s.TraceMaxBytes, now)
	p.trace.write(Record{Snapshot: snap})
	p.trace.flush()
	p.mu.Unlock()

	p.latest.Store(snap)
	for _, sub := range subscribers {
		sub.fn(snap)
	}
}

func (p *Plane) anyActive() bool {
	p.mu.Lock()
	subscribers := p.subscribers
	p.mu.Unlock()
	for _, sub := range subscribers {
		if sub.active != nil && sub.active() {
			return true
		}
	}
	return false
}

func (s *processSource) Read(now time.Time, windows int) Snapshot {
	snap := Snapshot{
		Persistence: s.persistence.RecentSnapshot(windows),
		Queues:      s.queues.snapshot(windows, now),
		ShardWrites: s.shards.snapshot(windows, now),
	}
	if s.rpc != nil {
		if errorRatio, ok := s.rpc.ErrorRatio(); ok {
			snap.RPC = RPC{Reported: true, Mean: millis(s.rpc.AverageLatency()), ErrorRatio: errorRatio}
			if p99, ok := s.rpc.LatencyQuantile(0.99); ok {
				snap.RPC.P99 = millis(p99)
			}
		}
	}
	snap.Runtime, snap.Memory.GoLimit = s.runtime.read()
	snap.Memory.Limit, snap.Memory.Usage = s.memory.read()
	return snap
}

func (s *processSource) Idle() {
	s.runtime.reset()
}

func millis(ms float64) time.Duration {
	return time.Duration(ms * float64(time.Millisecond))
}

// openTrace opens the trace before the first tick while the plane is enabled, so the
// trace has the decisions controllers make as the host starts.
func (p *Plane) openTrace() {
	s := p.settings()
	if !s.Enabled {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.trace.configure(s.TraceDirectory, s.TraceMaxBytes, p.timeSource.Now())
}

// Start ticks the plane on its own goroutine until Stop.
func (p *Plane) Start() {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	go p.run()
}

func (p *Plane) run() {
	defer close(p.doneCh)
	for {
		interval := p.settings().Interval
		if interval <= 0 {
			interval = DefaultSettings.Interval
		}
		fired, timer := p.timeSource.NewTimer(interval)
		select {
		case <-p.stopCh:
			timer.Stop()
			return
		case <-fired:
			p.Tick(p.timeSource.Now())
		}
	}
}

// Stop ends the plane's ticks and closes its trace file.
func (p *Plane) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		if p.started.Load() {
			<-p.doneCh
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		p.trace.close()
	})
}
