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

	// Plane publishes a Snapshot of the host every Settings.Interval while it's enabled.
	Plane struct {
		settings    func() Settings
		persistence PersistenceHealth
		rpc         RPCHealth // nil on hosts that don't record RPC health
		runtime     *runtimeSource
		memory      *cgroupMemory
		timeSource  clock.TimeSource

		latest atomic.Pointer[Snapshot]

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
		id int
		fn func(*Snapshot)
	}
)

var _ adaptive.DecisionSink = (*Plane)(nil)

func newPlane(
	settings func() Settings,
	persistenceHealth PersistenceHealth,
	rpcHealth RPCHealth,
	memory *cgroupMemory,
	timeSource clock.TimeSource,
	traceHeader Header,
	logger log.Logger,
) *Plane {
	return &Plane{
		settings:    settings,
		persistence: persistenceHealth,
		rpc:         rpcHealth,
		runtime:     newRuntimeSource(),
		memory:      memory,
		timeSource:  timeSource,
		decisions:   make(map[string]adaptive.Decision),
		trace:       newTraceWriter(traceHeader, logger),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
}

// Latest returns the newest snapshot, or nil while the plane is disabled.
func (p *Plane) Latest() *Snapshot {
	return p.latest.Load()
}

// Subscribe calls fn with each new snapshot, on the plane's goroutine and in the order
// subscribers subscribed, until the returned function cancels it.
func (p *Plane) Subscribe(fn func(*Snapshot)) (cancel func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := p.nextID
	p.nextID++
	// Copied on write, so a tick can call the subscribers without holding the lock.
	p.subscribers = append(p.subscribers[:len(p.subscribers):len(p.subscribers)], subscriber{id: id, fn: fn})
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

// RecordDecision keeps a controller's decision for the next snapshot, and writes it to
// the trace.
func (p *Plane) RecordDecision(d adaptive.Decision) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.decisions[d.Controller] = d
	p.trace.write(Record{Decision: &d})
}

// Tick takes and publishes a snapshot at now, if the plane is enabled. Only one
// goroutine may call it.
func (p *Plane) Tick(now time.Time) {
	s := p.settings()
	if !s.Enabled {
		p.latest.Store(nil)
		p.runtime.reset()
		p.mu.Lock()
		p.trace.close()
		p.mu.Unlock()
		return
	}

	snap := &Snapshot{Time: now}
	snap.Persistence = p.persistence.RecentSnapshot(max(1, s.Windows))
	if p.rpc != nil {
		if errorRatio, ok := p.rpc.ErrorRatio(); ok {
			snap.RPC = RPC{Reported: true, Mean: millis(p.rpc.AverageLatency()), ErrorRatio: errorRatio}
			if p99, ok := p.rpc.LatencyQuantile(0.99); ok {
				snap.RPC.P99 = millis(p99)
			}
		}
	}
	snap.Runtime, snap.Memory.GoLimit = p.runtime.read()
	snap.Memory.Limit, snap.Memory.Usage = p.memory.read()

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

func millis(ms float64) time.Duration {
	return time.Duration(ms * float64(time.Millisecond))
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
