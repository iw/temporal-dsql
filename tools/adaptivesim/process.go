package adaptivesim

import (
	"fmt"
	"math/rand/v2"
	"time"

	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/gate"
)

type (
	// process is one history host: its plugin's pacing, pool and store, its persistence
	// client and capacity limiter, its health signals and signal plane, history's tasks and
	// the workers that poll them.
	process struct {
		w       *world
		name    string
		seed    uint64
		started time.Time
		trace   *traceFile

		pacer    *pacer
		pool     *pool
		store    *store
		health   *health
		limiter  *capacityLimiter
		client   *client
		history  *history
		workers  *workers
		plane    *plane
		poolLoad poolLoad
		nextID   int64
		rng      *rand.Rand
	}

	// poolLoad is a load of bare statements on the store.
	poolLoad struct {
		inUse int
		waits []time.Duration
		stats PoolLoadStats
	}

	// PoolLoadStats counts the bare statements a host ran, and how long they waited for a
	// connection.
	PoolLoadStats struct {
		Requests, Completed, Failed int
		WaitP50, WaitP99, WaitMax   time.Duration
	}
)

// start starts the host as a process starts: its pools open, its plane ticks, and its
// load begins.
func (p *process) start() {
	w := p.w
	sc := w.sc
	h := sc.Host
	l := w.l
	p.started = l.now
	p.rng = randoms(p.seed, p.name+"/load")
	p.health = newHealth(sc.Signals)
	// The plugin declares that it reports facts when the store is set up.
	p.health.factsDeclared = true
	p.pacer = newPacer(l, w.dsql, h.Pacing, w.shared, p.seed, p.name)
	p.pool = newPool(l, h.Pool, sc.Network, p.pacer, w.dsql, p.seed, p.name)
	p.store = newStore(l, h, sc.Network, p.pool, w.dsql, p.health, w.clock, p.seed, p.name)
	p.plane = newPlane(l, sc.Signals, p.health, p.trace)
	p.limiter = newCapacityLimiter(h.Capacity, []byte(p.name), p.health, p.plane, l.now)
	p.client = &client{l: l, limiter: p.limiter, health: p.health, store: p.store, retryDelay: h.History.RetryDelay}
	p.history = newHistory(l, h.History, p.client, p.health)
	p.history.started, p.history.reloadTotal = l.now, sc.Load.Reload.Tasks
	p.workers = newWorkers(l, h.Workers, p.history)

	p.pool.start()
	p.store.reportFacts(acquireCounts{})
	p.plane.start()
	p.startupPools()
	p.startLoad()
	l.after(sc.Duration, func() {
		p.history.stats.Backlog = p.history.stats.Arrived - p.history.stats.Completed
	})
}

// startupPools opens the short-lived pools a process opens as it starts. Each takes its
// connections at once, holds each for a round trip, and closes after Duration.
func (p *process) startupPools() {
	cfg := p.w.sc.Host.StartupPools
	for i := range cfg.Pools {
		poolCfg := Pool{
			MaxConns:       cfg.Connections,
			Lifetime:       p.w.sc.Host.Pool.Lifetime,
			LifetimeJitter: p.w.sc.Host.Pool.LifetimeJitter,
			HealthCheck:    p.w.sc.Host.Pool.HealthCheck,
		}
		name := fmt.Sprintf("%s/startup-%d", p.name, i)
		sp := newPool(p.w.l, poolCfg, p.w.sc.Network, p.pacer, p.w.dsql, p.seed, name)
		sp.start()
		for range cfg.Connections {
			sp.acquire(time.Time{}, func(c *conn, result acquireResult) {
				if result != acquired {
					return
				}
				p.w.l.after(p.w.sc.Network.RoundTrip, func() { sp.release(c) })
			})
		}
		p.w.l.after(cfg.Duration, sp.close)
	}
}

// startLoad schedules the host's load, from its start.
func (p *process) startLoad() {
	load := p.w.sc.Load
	if r := load.Reload; r.Tasks > 0 {
		for i := range r.Tasks {
			at := r.At
			if r.Tasks > 1 {
				at += time.Duration(float64(r.Over) * float64(i) / float64(r.Tasks-1))
			}
			p.w.l.after(at, func() { p.newTask(true) })
		}
	}
	for _, phase := range load.Tasks {
		p.arrivals(phase, func() { p.newTask(false) })
	}
	for _, phase := range load.Workflows {
		p.arrivals(phase, p.newWorkflow)
	}
	if load.Pool.Every > 0 {
		p.w.l.soon(p.poolBurst)
	}
}

// arrivals calls arrive at the phase's rate, evenly spaced, from At for Duration.
func (p *process) arrivals(phase Phase, arrive func()) {
	if phase.Rate <= 0 {
		return
	}
	n := int(phase.Rate * phase.Duration.Seconds())
	interval := time.Duration(float64(time.Second) / phase.Rate)
	var next func(i int)
	next = func(i int) {
		if i >= n {
			return
		}
		arrive()
		p.w.l.after(interval, func() { next(i + 1) })
	}
	p.w.l.after(phase.At, func() { next(0) })
}

func (p *process) id() (int64, int) {
	p.nextID++
	return p.nextID, int(p.nextID % int64(p.w.sc.Host.History.Shards))
}

// newTask adds a task. A reloaded task's workflow isn't cached; a warm task's is.
func (p *process) newTask(reload bool) {
	wf, shard := p.id()
	if !reload {
		p.history.cached[wf] = true
	}
	p.history.arrive(&task{wf: wf, shard: shard, reload: reload})
}

func (p *process) newWorkflow() {
	wf, shard := p.id()
	p.workers.start(&workflow{id: wf, shard: shard})
}

// poolBurst starts up to Burst bare statements, keeping at most MaxInUse at once, every
// Every until the load ends.
func (p *process) poolBurst() {
	cfg := p.w.sc.Load.Pool
	if p.w.l.now.Sub(p.started) >= p.w.sc.Duration {
		return
	}
	for range min(cfg.Burst, cfg.MaxInUse-p.poolLoad.inUse) {
		p.poolStatement()
	}
	p.w.l.after(cfg.Every, p.poolBurst)
}

func (p *process) poolStatement() {
	cfg := p.w.sc.Load.Pool
	pl := &p.poolLoad
	pl.inUse++
	pl.stats.Requests++
	start := p.w.l.now
	p.store.connect(gate.Read, start.Add(cfg.MaxWait), func(ls *lease, result connectResult) {
		if result != connected {
			pl.inUse--
			pl.stats.Failed++
			return
		}
		pl.waits = append(pl.waits, p.w.l.now.Sub(start))
		p.w.l.after(cfg.Hold+between(p.rng, 0, cfg.HoldJitter), func() {
			p.store.release(ls)
			pl.inUse--
			pl.stats.Completed++
		})
	})
}

func (p *process) result() HostResult {
	pl := p.poolLoad.stats
	pl.WaitP50, pl.WaitP99 = percentile(p.poolLoad.waits, 0.5), percentile(p.poolLoad.waits, 0.99)
	for _, wait := range p.poolLoad.waits {
		pl.WaitMax = max(pl.WaitMax, wait)
	}
	return HostResult{
		Name:      p.name,
		Started:   p.started.Sub(p.w.start),
		Tasks:     p.history.result(),
		Workflows: p.workers.result(),
		Reads:     p.client.reads,
		Writes:    p.client.writes,
		Store:     p.store.stats,
		Pool: PoolStats{
			Opened:           p.pool.opened,
			Acquires:         p.pool.acquires,
			EmptyAcquires:    p.pool.emptyAcquires,
			CanceledAcquires: p.pool.canceledAcquires,
			LifetimeDestroys: p.pool.lifetimeDestroys,
			GateWaited:       p.store.gateWaited,
		},
		PoolLoad:  pl,
		Decisions: p.plane.decisions,
	}
}
