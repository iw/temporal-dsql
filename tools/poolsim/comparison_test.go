package main

import (
	"container/heap"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testConfig(model string) *Config {
	return &Config{
		Seed: 42, Duration: 3 * time.Hour, SampleEvery: time.Second, LimiterPerSec: 100,
		AssertMaxConnectsPerSec: -1, AssertConvergeWithin: -1, AssertMaxClosurePerSec: -1,
		ComparisonConfig: ComparisonConfig{Model: model, Limiter: "perSecond", Cluster: ClusterConfig{MaxConnections: 10000, Handshake: LatencyRange{time.Millisecond, time.Millisecond}}},
		Scenario:         ScenarioConfig{Kind: "cold_start"},
		Services:         []ServiceConfig{{Name: "test", TargetReady: 2, BaseLifetime: 10 * time.Second, Jitter: time.Nanosecond, GuardWindow: time.Second, PoolConfig: PoolConfig{MaxOpen: 2, PGX: PGXConfig{MaxConns: 2, MaxConnLifetime: 10 * time.Second, HealthCheckPeriod: time.Second}}}},
	}
}

func testPool(t *testing.T, model string) (*comparisonRun, *modelPool) {
	t.Helper()
	r, err := newComparison(testConfig(model), "")
	require.NoError(t, err)
	r.sim.q = nil // Tests schedule just the events relevant to each lifecycle boundary.
	i := r.instances[0]
	i.main = i.newPool("main", false)
	return r, i.main
}

func advance(r *comparisonRun, d time.Duration) {
	until := r.sim.Now().Add(d)
	for len(r.sim.q) > 0 && !r.sim.q[0].At.After(until) {
		e := heap.Pop(&r.sim.q).(*Event)
		r.sim.now = e.At
		e.Run()
	}
	r.sim.now = until
}

func TestDSQLTokenBucket(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	callbacks := 0
	for range 1001 {
		r.cluster.dial(p, func(c *physicalConn, code string) {
			callbacks++
			if code == "" {
				require.NotNil(t, c)
			} else {
				require.Equal(t, "53400", code)
			}
		})
	}
	require.Zero(t, callbacks, "rejected handshakes must also take time")
	advance(r, time.Millisecond)
	require.Equal(t, 1001, callbacks)
	require.EqualValues(t, 1, r.stats.rejections["53400"])
	require.Equal(t, 1000, r.cluster.open)
	advance(r, 9*time.Millisecond)
	r.cluster.dial(p, func(c *physicalConn, code string) { require.Empty(t, code); require.NotNil(t, c) })
	advance(r, time.Millisecond)
	require.Equal(t, 1001, r.cluster.open)
	// A long idle interval refills only up to the burst capacity.
	b := newTokenBucket(r.sim.Now(), 100, 1000)
	b.tokens = 0
	b.refill(r.sim.Now().Add(time.Hour))
	require.InDelta(t, 1000, b.tokens, 1e-9)
}

func TestDSQLCapAndAgeCut(t *testing.T) {
	r, p := testPool(t, "reservoir-sql")
	p.cfg.BaseLifetime = 2 * time.Hour
	r.cluster.cfg.MaxConnections = 2
	p.start()
	advance(r, 3*time.Millisecond)
	r.cluster.dial(p, func(c *physicalConn, code string) { require.Nil(t, c); require.Equal(t, "53300", code) })
	advance(r, time.Millisecond)
	require.EqualValues(t, 1, r.stats.rejections["53300"])
	idle := p.ready[0]
	advance(r, time.Hour-2*time.Second)
	req := p.request(10*time.Second, time.Second, nil)
	require.Same(t, idle, req.conn)
	advance(r, 3*time.Second)
	require.True(t, req.done)
	require.EqualValues(t, 1, r.stats.cutInUse)
	require.EqualValues(t, 1, r.stats.cutIdle)
	require.EqualValues(t, 1, r.stats.failures["dead"])
}

func TestSQLLifetimeAndDoubling(t *testing.T) {
	for _, variant := range []string{"as-deployed", "option-a"} {
		t.Run(variant, func(t *testing.T) {
			r, p := testPool(t, variant)
			p.start()
			advance(r, 3*time.Millisecond)
			a := p.request(20*time.Second, time.Second, nil)
			b := p.request(20*time.Second, time.Second, nil)
			advance(r, 3*time.Millisecond)
			require.Equal(t, 4, r.cluster.open, "SQL owns two and reservoir refills two alongside them")
			require.Equal(t, 2, p.borrowed)
			advance(r, 21*time.Second)
			require.True(t, a.done)
			require.True(t, b.done)
			if variant == "as-deployed" {
				require.Len(t, p.idle, 2)
				require.True(t, a.conn.alive, "SQL does not enforce reservoir lifetime")
				require.Equal(t, 4, r.cluster.open)
			} else {
				require.Empty(t, p.idle)
				require.False(t, a.conn.alive)
				require.Equal(t, 2, r.cluster.open)
			}
		})
	}
}

func TestPGXExpiredRelease(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	req := p.request(11*time.Second, time.Second, nil)
	advance(r, 2*time.Millisecond)
	require.NotNil(t, req.conn)
	require.True(t, req.conn.alive)
	advance(r, 10*time.Second)
	require.True(t, req.conn.alive, "client lifetime does not interrupt a query")
	advance(r, time.Second)
	require.True(t, req.done)
	require.False(t, req.conn.alive)
	require.Empty(t, p.owned)
	require.Zero(t, r.stats.failures["dead"])
}

func TestPGXHealthRefillsMinConns(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	p.cfg.PGX.MinConns = 2
	p.start()
	advance(r, 2*time.Millisecond)
	require.Len(t, p.idle, 2)
	c := p.idle[0]
	p.idle = p.idle[1:]
	p.destroy(c, false)
	advance(r, time.Second)
	require.Len(t, p.idle, 2)
	require.EqualValues(t, 3, p.stats.opens)
	// Idle expiry may not shrink below MinConns; lifetime expiry may go one below.
	p.cfg.PGX.MaxConnIdleTime = time.Millisecond
	advance(r, 2*time.Second)
	require.Len(t, p.idle, 2)
	advance(r, 10*time.Second)
	require.Len(t, p.idle, 2)
	require.Greater(t, p.stats.opens, int64(3))
}

func TestPGXOnDemandLimiterAndCancellation(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	r.limiter.mode = "tokenBucket"
	r.limiter.bucket.tokens = 0
	r.cfg.LimiterLatency = LatencyRange{5 * time.Millisecond, 5 * time.Millisecond}
	req := p.request(time.Millisecond, 2*time.Millisecond, nil)
	require.Equal(t, 1, p.constructing)
	require.Zero(t, r.cluster.open)
	advance(r, 3*time.Millisecond)
	require.True(t, req.done)
	require.EqualValues(t, 1, r.stats.failures["timeout"])
	advance(r, 20*time.Millisecond)
	require.Len(t, p.idle, 1, "puddle continues the constructor after acquire cancellation")
	require.EqualValues(t, 1, r.stats.waitedNew)
	require.Equal(t, 0, p.instance.active)
}

func TestPGXGuardAndIdleOrder(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	p.cfg.PGX.MinConns = 2
	p.start()
	advance(r, 2*time.Millisecond)
	last := p.idle[len(p.idle)-1]
	req := p.request(time.Millisecond, time.Second, nil)
	require.Same(t, last, req.conn, "puddle idle order is LIFO")
	advance(r, 2*time.Millisecond)
	last.expires = r.sim.Now().Add(500 * time.Millisecond)
	next := p.request(time.Millisecond, time.Second, nil)
	require.NotSame(t, last, next.conn)
	require.False(t, last.alive)
	require.Zero(t, r.stats.failures["dead"])
}

func TestPGXAcquireGuardOff(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	off := false
	p.cfg.PGX.AcquireGuard = &off
	p.cfg.PGX.MinConns = 2
	p.start()
	advance(r, 2*time.Millisecond)
	last := p.idle[len(p.idle)-1]
	last.expires = r.sim.Now().Add(500 * time.Millisecond)
	req := p.request(600*time.Millisecond, time.Second, nil)
	require.Same(t, last, req.conn, "stock Acquire hands out a connection inside the guard window")
	require.False(t, req.waitedNew)
	advance(r, 601*time.Millisecond)
	require.True(t, req.done)
	require.False(t, last.alive, "Release retires it once past its lifetime")
	require.Empty(t, r.stats.failures)
}

func TestAdaptiveAIMDAndOutage(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	b := p.instance.adaptive
	b.feedback(r.sim.Now(), "", .5)
	require.InDelta(t, 10.5, b.rate, 1e-9)
	b.feedback(r.sim.Now(), "53400", .5)
	require.InDelta(t, 5.25, b.rate, 1e-9)
	b.feedback(r.sim.Now(), "53300", .5)
	require.InDelta(t, 5.25, b.rate, 1e-9)
	r.limiter.mode = "tokenBucket"
	r.limiter.bucket.tokens = 0
	calls := 0
	r.limiter.acquire(p.instance, func() { calls++ })
	r.limiter.outage()
	advance(r, time.Second)
	require.Equal(t, 1, calls, "outage must not run an outstanding acquisition twice")
	require.Equal(t, "adaptive", r.limiter.mode)
}

func TestPoolCloseLeakAndRelease(t *testing.T) {
	for _, variant := range []string{"as-deployed", "option-a", "pgxpool"} {
		t.Run(variant, func(t *testing.T) {
			r, p := testPool(t, variant)
			if p.isPGX() {
				p.cfg.PGX.MinConns = 2
			}
			p.start()
			advance(r, 3*time.Millisecond)
			req := p.request(time.Second, time.Second, nil)
			advance(r, 3*time.Millisecond)
			p.close()
			advance(r, 2*time.Second)
			require.True(t, req.done)
			if variant == "as-deployed" {
				require.Positive(t, r.cluster.open)
			} else {
				require.Zero(t, r.cluster.open)
			}
			advance(r, time.Hour)
			require.Zero(t, r.cluster.open)
		})
	}
}

func TestSQLWaitTimeoutAndDeadRebuild(t *testing.T) {
	r, p := testPool(t, "as-deployed")
	p.start()
	advance(r, 3*time.Millisecond)
	a := p.request(time.Second, time.Second, nil)
	b := p.request(time.Second, time.Second, nil)
	timeout := p.request(time.Second, time.Millisecond, nil)
	advance(r, 2*time.Millisecond)
	require.True(t, timeout.done)
	require.EqualValues(t, 1, r.stats.failures["timeout"])
	r.cluster.drop(a.conn)
	r.cluster.drop(b.conn)
	require.EqualValues(t, 2, r.stats.failures["dead"])
	advance(r, 2*time.Second)
	require.EqualValues(t, 2, r.stats.rebuilds, "dead errors must queue serialized rebuilds")
}

func TestSharedInstanceCapAndEphemeralClose(t *testing.T) {
	cfg := testConfig("pgxpool")
	cfg.Services[0].InflightLimit = 2
	cfg.Services[0].Replicas = 2
	cfg.Services[0].PGX.MinConns = 2
	cfg.Services[0].Ephemeral = EphemeralConfig{Pools: 3, Connections: 3, Duration: time.Second}
	r, err := newComparison(cfg, "")
	require.NoError(t, err)
	advance(r, 3*time.Second)
	for _, i := range r.instances {
		require.Equal(t, 2, i.peakInflight)
		for _, p := range i.ephemeral {
			require.True(t, p.closed)
			require.Zero(t, p.physical)
		}
	}
	require.Equal(t, 4, r.cluster.open)
}

func TestComparisonDeterminismAndScenarios(t *testing.T) {
	for _, kind := range []string{"cold_start", "rolling_restart", "rolling_deploy", "fleet_start", "mass_drop", "ddb_outage"} {
		for _, variant := range []string{"reservoir", "as-deployed", "option-a", "pgxpool"} {
			t.Run(kind+"/"+variant, func(t *testing.T) {
				cfg := testConfig(variant)
				cfg.Duration = 5 * time.Second
				cfg.Scenario = ScenarioConfig{Kind: kind, EventAt: 2 * time.Second, DropAt: 2 * time.Second, DropFraction: .5, RollEvery: time.Second, StartWindow: time.Second}
				cfg.Limiter = "tokenBucket"
				cfg.Services[0].Replicas = 2
				cfg.Services[0].Workload = WorkloadConfig{Enabled: true, MaxInUse: 2, ArrivalBurst: 2, SpawnEvery: 100 * time.Millisecond, HoldTime: 10 * time.Millisecond, MaxWait: time.Second}
				a, err := newComparison(cfg, "")
				require.NoError(t, err)
				a.sim.Run()
				b, err := newComparison(cfg, "")
				require.NoError(t, err)
				b.sim.Run()
				require.Equal(t, a.summary(), b.summary())
				require.Equal(t, a.stats.samples, b.stats.samples)
				require.NoError(t, a.assertions())
			})
		}
	}
}

func TestComparisonAssertionsAndCSV(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	r.cfg.AssertZeroDSQLRejections = true
	r.stats.rejections["53400"] = 1
	require.ErrorContains(t, r.assertions(), "DSQL rejections")
	r.cfg.AssertZeroDSQLRejections = false
	r.cfg.AssertZeroDeadRequests = true
	r.stats.failures["dead"] = 1
	require.ErrorContains(t, r.assertions(), "dead connection")
	p.start()
	advance(r, time.Second)
	r.sample()
	path := filepath.Join(t.TempDir(), "model.csv")
	require.NoError(t, r.writeCSV(path))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "rejected_53400")
	var h waitHistogram
	for range 999 {
		h.add(0)
	}
	h.add(time.Second)
	require.Zero(t, h.percentile(.999))
	require.Equal(t, time.Second, h.percentile(1))
}

func TestHealthFailureSkipsEvictionAndCancelsBatch(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	p.cfg.PGX.MinConns = 0
	req := p.request(time.Millisecond, time.Second, nil)
	advance(r, 3*time.Millisecond)
	req.conn.expires = r.sim.Now().Add(-time.Second)
	p.cfg.PGX.MinConns = 100
	p.cfg.PGX.MaxConns = 100
	r.cluster.cfg.MaxConnections = 1
	p.health()
	advance(r, time.Second)
	require.True(t, req.conn.alive, "checkHealth skips eviction when checkMinConns fails")
	require.LessOrEqual(t, r.stats.rejections["53300"], int64(p.cfg.InflightLimit), "createIdleResources cancels queued constructors on first error")
	require.Zero(t, p.constructing)
}

func TestPGXIdleTimeoutAndHealthStack(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	a := p.request(time.Millisecond, time.Second, nil)
	b := p.request(time.Millisecond, time.Second, nil)
	advance(r, 3*time.Millisecond)
	require.Same(t, b.conn, p.idle[1])
	before := a.conn.lastUsed
	p.health()
	require.Same(t, a.conn, p.idle[1], "health sweep pops and releases resources in reverse stack order")
	require.Equal(t, before, a.conn.lastUsed, "ReleaseUnused does not reset idle time")
	p.cfg.PGX.MaxConnIdleTime = 0
	advance(r, 2*time.Second)
	require.Len(t, p.idle, 2, "zero explicitly disables idle timeout in this proposal")
	p.cfg.PGX.MaxConnIdleTime = time.Second
	p.health()
	require.Empty(t, p.idle)
	require.Zero(t, r.cluster.open)
}

func TestOnDemandSuccessfulWaitAndSharedGlobalBucket(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	r.limiter.mode = "tokenBucket"
	r.limiter.bucket.tokens = 0
	r.cfg.LimiterLatency = LatencyRange{5 * time.Millisecond, 5 * time.Millisecond}
	second := r.newInstance("second", p.cfg, 0)
	second.main = second.newPool("main", false)
	a := p.request(time.Second, time.Second, nil)
	b := second.main.request(time.Second, time.Second, nil)
	advance(r, 17*time.Millisecond)
	require.True(t, a.acquired)
	require.False(t, b.acquired, "all instances share the same token bucket")
	advance(r, 10*time.Millisecond)
	require.True(t, b.acquired)
	require.Equal(t, 16*time.Millisecond, p.stats.wait.maximum)
	require.Equal(t, 26*time.Millisecond, second.main.stats.wait.maximum)
}

func TestSQLGuardOnReuseAndWaitingRelease(t *testing.T) {
	r, p := testPool(t, "option-a")
	p.start()
	advance(r, 3*time.Millisecond)
	a := p.request(time.Millisecond, time.Second, nil)
	advance(r, 2*time.Millisecond)
	a.conn.expires = r.sim.Now().Add(time.Millisecond)
	b := p.request(100*time.Millisecond, time.Second, nil)
	require.False(t, a.conn.alive)
	require.NotSame(t, a.conn, b.conn)
	c := p.request(100*time.Millisecond, time.Second, nil)
	waiting := p.request(time.Millisecond, time.Second, nil)
	require.False(t, waiting.acquired)
	advance(r, 101*time.Millisecond)
	require.True(t, waiting.acquired)
	require.True(t, c.done)
	require.Zero(t, r.stats.failures["timeout"])
}

func TestComparisonConfigAndServiceModels(t *testing.T) {
	cfg := testConfig("pgxpool")
	cfg.Services[0].Model = "option-a"
	cfg.Services = append(cfg.Services, ServiceConfig{Name: "second", TargetReady: 1})
	r, err := newComparison(cfg, "")
	require.NoError(t, err)
	advance(r, time.Millisecond)
	require.Equal(t, "reservoir-sql", r.instances[0].main.model)
	require.True(t, r.instances[0].main.cfg.EnforceLifetimeInUse)
	require.Equal(t, "pgxpool", r.instances[1].main.model)
	for _, test := range []struct {
		name   string
		change func(*Config)
	}{
		{"model", func(c *Config) { c.Model = "typo" }},
		{"limiter", func(c *Config) { c.Limiter = "typo" }},
		{"negative sample", func(c *Config) { c.SampleEvery = -time.Second }},
		{"replicas", func(c *Config) { c.Services[0].Replicas = -1 }},
		{"bounds", func(c *Config) { c.Services[0].PGX.MinConns = 100 }},
		{"latency", func(c *Config) { c.Cluster.Handshake.Min = time.Second }},
		{"workload", func(c *Config) { c.Services[0].Workload.Enabled = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := testConfig("pgxpool")
			test.change(c)
			_, err := newComparison(c, "")
			require.Error(t, err)
		})
	}
}

func TestNamedScenarioConfigs(t *testing.T) {
	files, err := filepath.Glob("scenarios/*.yaml")
	require.NoError(t, err)
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			cfg, err := LoadConfig(file)
			require.NoError(t, err)
			if usesComparison(cfg) {
				require.GreaterOrEqual(t, cfg.Duration, 3*time.Hour)
				for _, s := range cfg.Services {
					require.Less(t, s.PGX.MaxConnLifetime+s.PGX.MaxConnLifetimeJitter+s.GuardWindow+5*time.Minute, dsqlMaxAge)
				}
			}
		})
	}
}

func TestCloseCancelsWaitingConstructor(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	r.limiter.mode = "tokenBucket"
	r.limiter.bucket.tokens = 0
	req := p.request(time.Second, time.Second, nil)
	require.False(t, req.done)
	p.close()
	require.True(t, req.done)
	require.EqualValues(t, 1, r.stats.failures["closed"])
	advance(r, time.Second)
	require.Zero(t, r.cluster.open)
	require.Zero(t, p.constructing)
}

func TestLegacyScenarioAssertions(t *testing.T) {
	for _, name := range []string{"local", "ecs-150wps", "ecs-400wps", "mass-drop", "workload-test"} {
		t.Run(name, func(t *testing.T) {
			cfg, err := LoadConfig("scenarios/" + name + ".yaml")
			require.NoError(t, err)
			require.False(t, usesComparison(cfg))
			sim := NewSim(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), cfg.Duration)
			sim.quiet = true
			limiter := NewPerSecondLimiter(cfg.LimiterPerSec)
			metrics := NewMetrics()
			services := make(map[string]*Service)
			for _, scfg := range cfg.Services {
				svc := NewService(scfg, cfg.Endpoint, limiter, metrics, sim)
				svc.Refiller.rng = seeded(cfg.Seed, scfg.Name)
				services[scfg.Name] = svc
				svc.Start(sim.Now().Add(scfg.StartDelay))
			}
			require.NoError(t, ApplyScenario(cfg, sim, services))
			scheduleSampling(sim, cfg, services, metrics)
			sim.Run()
			require.NoError(t, checkAssertions(cfg, metrics))
		})
	}
}

func TestPGXHealthServesWaitersBeforeBatchCompletes(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	p.cfg.PGX.MinConns = 2
	p.instance.cfg.InflightLimit = 1
	p.start()
	req := p.request(time.Second, time.Second, nil)
	require.False(t, req.acquired)
	advance(r, time.Millisecond)
	require.True(t, req.acquired, "CreateResource releases its semaphore slot individually")
	require.Equal(t, 1, p.constructing)
	require.Equal(t, time.Millisecond, p.stats.wait.maximum)
}
