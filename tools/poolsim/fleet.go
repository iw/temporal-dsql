package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"
	"time"
)

type comparisonRun struct {
	cfg         *Config
	name        string
	sim         *Sim
	start       time.Time
	cluster     *dsqlCluster
	limiter     *clientLimiter
	instances   []*instance
	pools       []*modelPool
	stats       *comparisonStats
	legacy      *Metrics
	convergence map[string]*convergenceState
}

type convergenceState struct {
	started, reached time.Time
	target           int
	stable           bool
	failure          string
}

func seeded(seed int64, key string) *rand.Rand {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return rand.New(rand.NewSource(seed ^ int64(h.Sum64())))
}

func newComparison(original *Config, variant string) (*comparisonRun, error) {
	c := *original
	c.Services = append([]ServiceConfig(nil), original.Services...)
	applyDefaults(&c)
	if variant != "" {
		c.Model = variant
		for j := range c.Services {
			c.Services[j].Model = variant
		}
	}
	comparisonDefaults(&c)
	if err := validate(&c); err != nil {
		return nil, err
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r := &comparisonRun{cfg: &c, name: variant, sim: NewSim(start, c.Duration), start: start, stats: newComparisonStats(), legacy: NewMetrics(), convergence: make(map[string]*convergenceState)}
	r.sim.quiet = true
	for _, s := range c.Services {
		for _, phase := range s.Workload.Phases {
			if r.stats.phases[phase.Name] == nil {
				r.stats.phaseNames = append(r.stats.phaseNames, phase.Name)
				r.stats.phases[phase.Name] = newPoolStats()
			}
		}
	}
	if r.name == "" {
		r.name = c.Model
	}
	r.cluster = &dsqlCluster{run: r, cfg: c.Cluster, bucket: newTokenBucket(start, 100, 1000), rng: seeded(c.Seed, "cluster"), conns: make(map[int64]*physicalConn)}
	r.limiter = &clientLimiter{run: r, mode: c.Limiter, bucket: newTokenBucket(start, 100, 1000), perSecond: NewPerSecondLimiter(c.LimiterPerSec), rng: seeded(c.Seed, "limiter")}
	startRNG := seeded(c.Seed, "fleet-start")
	for _, s := range c.Services {
		for replica := 0; replica < s.Replicas; replica++ {
			name := fmt.Sprintf("%s/%02d", s.Name, replica+1)
			i := r.newInstance(name, s, 0)
			r.instances = append(r.instances, i)
			at := start.Add(s.StartDelay)
			if c.Scenario.Kind == "fleet_start" && c.Scenario.StartWindow > 0 {
				at = at.Add(time.Duration(startRNG.Int63n(int64(c.Scenario.StartWindow) + 1)))
			}
			r.sim.Schedule(at, "instance-start", i.start)
		}
	}
	r.scenario()
	r.sim.After(c.SampleEvery, "comparison-sample", r.sample)
	return r, nil
}

func (r *comparisonRun) newInstance(name string, s ServiceConfig, generation int) *instance {
	return &instance{run: r, name: name, cfg: s, generation: generation, adaptive: newTokenBucket(r.sim.Now(), r.cfg.Adaptive.InitialRate, r.cfg.Adaptive.Capacity)}
}

func (i *instance) newPool(suffix string, ephemeral bool) *modelPool {
	cfg := i.cfg
	model := cfg.Model
	if model == "" {
		model = i.run.cfg.Model
	}
	switch model {
	case "as-deployed":
		model = "reservoir-sql"
		cfg.EnforceLifetimeInUse = false
		cfg.ConcurrentRefill = false
		cfg.LeakOnClose = nil
		cfg.ReconnectOnConnError = nil
	case "option-a":
		model = "reservoir-sql"
		cfg.EnforceLifetimeInUse = true
		cfg.ConcurrentRefill = true
		no := false
		cfg.LeakOnClose = &no
		cfg.ReconnectOnConnError = &no
	default:
	}
	if ephemeral {
		cfg.InflightLimit = min(2, cfg.InflightLimit)
		cfg.TargetReady = 5
		cfg.MaxOpen = cfg.Ephemeral.Connections
		cfg.PGX.MinConns = 0
		cfg.PGX.MaxConns = cfg.Ephemeral.Connections
	}
	name := fmt.Sprintf("%s/g%d/%s", i.name, i.generation, suffix)
	p := &modelPool{instance: i, name: name, model: model, cfg: cfg, rng: seeded(i.run.cfg.Seed, name), stats: newPoolStats(), ephemeral: ephemeral, owned: make(map[int64]*physicalConn)}
	i.run.pools = append(i.run.pools, p)
	return p
}

func (i *instance) start() {
	if i.closed {
		return
	}
	for j := 0; j < i.cfg.Ephemeral.Pools; j++ {
		p := i.newPool(fmt.Sprintf("ephemeral-%d", j+1), true)
		i.ephemeral = append(i.ephemeral, p)
		p.start()
		// Reservoir ephemeral settings: ready=5, low watermark=5, initial-fill
		// timeout=10s, at most 2 opens locally (sequential refiller uses just 1).
		w := WorkloadConfig{Enabled: true, MaxInUse: i.cfg.Ephemeral.Connections, ArrivalBurst: i.cfg.Ephemeral.Connections, SpawnEvery: 250 * time.Millisecond, HoldTime: 10 * time.Millisecond, MaxWait: 10 * time.Second}
		i.workload(p, w, i.cfg.Ephemeral.Duration)
	}
	i.main = i.newPool("main", false)
	i.run.convergence[i.main.name] = &convergenceState{started: i.run.sim.Now(), target: i.main.target(), stable: true}
	i.main.start()
	// Workload arrival and hold RNGs do not depend on connection/limiter RNGs.
	if i.cfg.Workload.Enabled {
		i.workload(i.main, i.cfg.Workload, 0)
	}

}

func (i *instance) workload(p *modelPool, w WorkloadConfig, duration time.Duration) {
	rng := seeded(i.run.cfg.Seed, p.name+"/workload")
	active := 0
	var until time.Time
	var tick func()
	tick = func() {
		if i.closed || p.closed {
			return
		}
		if !until.IsZero() && !i.run.sim.Now().Before(until) {
			p.close()
			return
		}
		elapsed := i.run.sim.Now().Sub(i.run.start)
		level, phase, boundary := w.at(elapsed)
		for n := min(level.ArrivalBurst, level.MaxInUse-active); n > 0; n-- {
			hold := level.HoldTime
			if level.HoldJitter > 0 {
				hold += time.Duration(rng.Int63n(int64(level.HoldJitter) + 1))
			}
			active++
			p.requestInPhase(hold, level.MaxWait, phase, func() { active-- })
		}
		delay := level.SpawnEvery
		if boundary > elapsed {
			delay = min(delay, boundary-elapsed)
		}
		i.run.sim.After(delay, "workload", tick)
	}
	begin := func() {
		if duration > 0 {
			p.onFirstAcquire = func() {
				until = i.run.sim.Now().Add(duration)
				i.run.sim.Schedule(until, "ephemeral-close", p.close)
			}
			// A failed startup cannot leave a short-lived pool around indefinitely.
			i.run.sim.After(10*time.Second, "ephemeral-start-timeout", func() {
				if until.IsZero() {
					p.close()
				}
			})
		}
		tick()
	}
	p.afterInitialFill(begin)
}

func (p *modelPool) afterInitialFill(begin func()) {
	if p.isPGX() {
		begin()
		return
	}
	// Match the reservoir low-watermark startup barrier, bounded by its fill
	// timeout. Once admitted, driver.Open is always a nonblocking checkout.
	timeout := 30 * time.Second
	if p.ephemeral {
		timeout = 10 * time.Second
	}
	deadline := p.run().sim.Now().Add(timeout)
	var ready func()
	ready = func() {
		if p.closed || p.instance.closed {
			return
		}
		if len(p.ready) >= p.cfg.TargetReady || !p.run().sim.Now().Before(deadline) {
			begin()
			return
		}
		p.run().sim.After(100*time.Millisecond, "initial-fill", ready)
	}
	ready()
}

func (i *instance) close() {
	if i.closed {
		return
	}
	i.closed = true
	if i.main != nil {
		i.main.close()
	}
	for _, p := range i.ephemeral {
		p.close()
	}
	pending := i.queue
	i.queue = nil
	for _, job := range pending {
		job.done(nil, "closed")
	}
}

// Each dead request queues a rebuild, including errors from the old pool while
// it drains. A single worker serializes the rebuilds; none overlap or coalesce.
func (i *instance) rebuild() {
	if i.closed {
		return
	}
	i.rebuildPending++
	if i.rebuilding {
		return
	}
	i.rebuilding = true
	i.nextRebuild()
}

func (i *instance) nextRebuild() {
	if i.closed {
		return
	}
	if i.rebuildPending == 0 {
		i.rebuilding = false
		return
	}
	i.rebuildPending--
	i.run.stats.rebuilds++
	i.main.close()
	i.generation++
	i.main = i.newPool("main", false)
	i.main.start()
	if i.cfg.Workload.Enabled {
		i.workload(i.main, i.cfg.Workload, 0)
	}
	// Serialized rebuild bookkeeping takes 1 simulated second. Initial fill and
	// subsequent requests retain their normal limiter/handshake delays.
	i.run.sim.After(time.Second, "rebuild-next", i.nextRebuild)
}

func (r *comparisonRun) scenario() {
	sc := r.cfg.Scenario
	switch sc.Kind {
	case "mass_drop":
		r.sim.Schedule(r.start.Add(sc.DropAt), "mass-drop", func() {
			ids := make([]int64, 0, len(r.cluster.conns))
			for id := range r.cluster.conns {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			rng := seeded(r.cfg.Seed, "mass-drop")
			rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
			count := int(float64(len(ids)) * sc.DropFraction)
			for _, id := range ids[:count] {
				if c := r.cluster.conns[id]; c != nil {
					r.cluster.drop(c)
				}
			}
		})
	case "rolling_restart", "rolling_deploy":
		interval := sc.RollEvery
		if interval == 0 {
			interval = time.Minute
		}
		count := len(r.instances)
		if sc.Kind == "rolling_restart" {
			count = int((r.cfg.Duration-sc.EventAt)/interval) + 1
		}
		for n := 0; n < count; n++ {
			slot := n % len(r.instances)
			r.sim.Schedule(r.start.Add(sc.EventAt+time.Duration(n)*interval), "replace-instance", func() {
				old := r.instances[slot]
				old.close()
				replacement := r.newInstance(old.name, old.cfg, old.generation+1)
				r.instances[slot] = replacement
				replacement.start()
			})
		}
	case "ddb_outage":
		r.sim.Schedule(r.start.Add(sc.EventAt), "ddb-outage", r.limiter.outage)
	default:
	}
}
