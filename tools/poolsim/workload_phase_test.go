package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func phaseWorkload() WorkloadConfig {
	return WorkloadConfig{
		Enabled: true, MaxInUse: 2, ArrivalBurst: 2, SpawnEvery: time.Second,
		HoldTime: 250 * time.Millisecond, HoldJitter: time.Millisecond, MaxWait: time.Second,
		Phases: []WorkloadPhase{{Name: "peak", At: 1050 * time.Millisecond, Ramp: time.Second, Duration: 1800 * time.Millisecond,
			MaxInUse: 20, ArrivalBurst: 20, SpawnEvery: 100 * time.Millisecond, HoldTime: 200 * time.Millisecond, MaxWait: 2 * time.Second}},
	}
}

func TestWorkloadPhaseLevels(t *testing.T) {
	w := phaseWorkload()
	for _, tc := range []struct {
		name     string
		at       time.Duration
		level    int
		tag      string
		boundary time.Duration
	}{
		{"before", time.Second, 2, "", 1050 * time.Millisecond},
		{"ramp start", 1050 * time.Millisecond, 2, "peak", 2050 * time.Millisecond},
		{"mid ramp", 1550 * time.Millisecond, 11, "peak", 2050 * time.Millisecond},
		{"plateau", 2050 * time.Millisecond, 20, "peak", 3850 * time.Millisecond},
		{"end", 3850 * time.Millisecond, 2, "", 0},
		{"after", 5 * time.Second, 2, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			level, tag, boundary := w.at(tc.at)
			require.Equal(t, tc.level, level.MaxInUse)
			require.Equal(t, tc.tag, tag)
			require.Equal(t, tc.boundary, boundary)
			if tag != "" {
				require.Equal(t, 100*time.Millisecond, level.SpawnEvery)
				require.Equal(t, 200*time.Millisecond, level.HoldTime)
				require.Equal(t, w.HoldJitter, level.HoldJitter, "zero inherits base")
				require.Equal(t, 2*time.Second, level.MaxWait)
			} else {
				require.Equal(t, w, level)
			}
		})
	}
	w.Phases[0].Ramp = 0
	level, tag, boundary := w.at(w.Phases[0].At)
	require.Equal(t, 20, level.MaxInUse)
	require.Equal(t, "peak", tag)
	require.Equal(t, 2850*time.Millisecond, boundary)
	w.Phases[0] = WorkloadPhase{Name: "inherit", At: time.Second, Duration: time.Second}
	level, _, _ = w.at(time.Second)
	require.Equal(t, w, level, "all omitted overrides inherit the base")
	require.NoError(t, w.validatePhases())
}

func TestWorkloadPhaseValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*WorkloadConfig)
	}{
		{"overlap", func(w *WorkloadConfig) { w.Phases = append(w.Phases, w.Phases[0]) }},
		{"out of order", func(w *WorkloadConfig) {
			w.Phases = append(w.Phases, WorkloadPhase{Name: "early", Duration: time.Second})
		}},
		{"no name", func(w *WorkloadConfig) { w.Phases[0].Name = "" }},
		{"zero duration", func(w *WorkloadConfig) { w.Phases[0].Duration = 0 }},
		{"negative ramp", func(w *WorkloadConfig) { w.Phases[0].Ramp = -time.Second }},
		{"negative at", func(w *WorkloadConfig) { w.Phases[0].At = -time.Second }},
		{"negative demand", func(w *WorkloadConfig) { w.Phases[0].MaxInUse = -1 }},
		{"inherited invalid demand", func(w *WorkloadConfig) { w.MaxInUse = 0; w.Phases[0].MaxInUse = 0 }},
		{"negative wait", func(w *WorkloadConfig) { w.Phases[0].MaxWait = -time.Second }},
		{"negative tick", func(w *WorkloadConfig) { w.Phases[0].SpawnEvery = -time.Second }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig("pgxpool")
			c.Services[0].Workload = phaseWorkload()
			tc.change(&c.Services[0].Workload)
			_, err := newComparison(c, "")
			require.Error(t, err)
		})
	}
	w := phaseWorkload()
	w.Phases = append(w.Phases, WorkloadPhase{Name: "next", At: 3850 * time.Millisecond, Duration: time.Second})
	require.NoError(t, w.validatePhases(), "adjacent phases are allowed")
	path := filepath.Join(t.TempDir(), "legacy.yaml")
	require.NoError(t, os.WriteFile(path, []byte("services:\n  - name: history\n    workload:\n      maxInUse: 1\n      phases:\n        - {name: peak, at: 1s, duration: 1s, maxInUse: 2}\n"), 0600))
	_, err := LoadConfig(path)
	require.ErrorContains(t, err, "legacy ReservoirWorkload does not support phases")
}

func inUse(p *modelPool) int {
	n := 0
	for _, c := range p.owned {
		if c.request != nil {
			n++
		}
	}
	return n
}

func TestWorkloadPhaseGenerator(t *testing.T) {
	r, p := testPool(t, "pgxpool")
	p.cfg.PGX.MaxConns = 30
	p.cfg.PGX.MinConns = 30
	p.start()
	advance(r, 10*time.Millisecond)
	w := phaseWorkload()
	r.stats.phases["peak"] = newPoolStats()
	p.instance.workload(p, w, 0)
	advance(r, 1040*time.Millisecond) // Phase starts between base ticks.
	require.Equal(t, 2, inUse(p))
	for _, c := range p.owned {
		if c.request != nil {
			require.Empty(t, c.request.phase, "requests already in flight keep their base tag")
		}
	}
	advance(r, time.Second)
	require.Equal(t, 20, inUse(p), "reach plateau within one phase tick")
	advance(r, 1800*time.Millisecond)
	requestsAtEnd := p.stats.requests
	advance(r, 300*time.Millisecond)
	require.Equal(t, requestsAtEnd, p.stats.requests, "allow existing work to drain")
	require.Zero(t, inUse(p))
	advance(r, 700*time.Millisecond)
	require.Equal(t, 2, inUse(p), "next base tick uses base demand")
	require.Zero(t, p.stats.wait.maximum, "pool never queues")
	require.Empty(t, p.stats.failures)
	require.Positive(t, r.stats.phases["peak"].requests)
}

func TestWorkloadPhasesUseGlobalTimeAndExcludeEphemeral(t *testing.T) {
	cfg := testConfig("pgxpool")
	s := &cfg.Services[0]
	s.TargetReady = 30
	s.PGX.MaxConns = 30
	s.Workload = phaseWorkload()
	s.Workload.Phases[0].Ramp = 0
	s.StartDelay = 2 * time.Second // Start halfway through the phase.
	s.Ephemeral = EphemeralConfig{Pools: 1, Connections: 2, Duration: time.Second}
	r, err := newComparison(cfg, "")
	require.NoError(t, err)
	advance(r, 2010*time.Millisecond)
	p := r.instances[0].main
	require.EqualValues(t, 20, p.stats.requests)
	require.EqualValues(t, 20, r.stats.phases["peak"].requests)
	require.EqualValues(t, 2, r.instances[0].ephemeral[0].stats.requests)
	require.EqualValues(t, 22, r.stats.requests)
	require.EqualValues(t, 1, r.stats.startDelay.count)
	require.Zero(t, r.stats.startDelay.maximum)
	// A rebuilt main pool also uses the simulation clock.
	r.instances[0].rebuild()
	advance(r, 10*time.Millisecond)
	require.EqualValues(t, 20, r.instances[0].main.stats.requests)
	require.EqualValues(t, 40, r.stats.phases["peak"].requests)
}

func TestPhaseAttributionAndSummaryOrder(t *testing.T) {
	cfg := testConfig("pgxpool")
	cfg.Services[0].Workload = phaseWorkload()
	cfg.Services[0].Workload.Phases = append(cfg.Services[0].Workload.Phases, WorkloadPhase{Name: "spike", At: 5 * time.Second, Duration: time.Second})
	other := cfg.Services[0]
	other.Name = "other"
	cfg.Services = append(cfg.Services, other)
	r, err := newComparison(cfg, "")
	require.NoError(t, err)
	r.sim.q = nil
	i := r.instances[0]
	i.main = i.newPool("main", false)
	p := i.main
	p.start()
	advance(r, time.Second)
	otherInstance := r.instances[1]
	otherInstance.main = otherInstance.newPool("main", false)
	otherInstance.main.start()
	otherInstance.main.requestInPhase(time.Millisecond, time.Second, "peak", nil)
	phase := r.stats.phases["peak"]
	a := p.requestInPhase(4*time.Second, time.Second, "peak", nil)
	p.request(time.Second, time.Second, nil)
	p.requestInPhase(time.Second, time.Millisecond, "peak", nil)
	advance(r, 2*time.Millisecond)
	require.Equal(t, "peak", a.phase)
	require.EqualValues(t, 3, phase.requests)
	require.EqualValues(t, 3, phase.waitedNew)
	require.EqualValues(t, 1, phase.failures["timeout"])
	advance(r, 4*time.Second)
	require.True(t, a.done)
	require.EqualValues(t, 2, phase.completed, "completion after phase stays attributed to spawn phase")
	require.EqualValues(t, 3, phase.wait.count)
	require.Zero(t, r.stats.phases["spike"].requests)
	require.Equal(t, []string{"peak", "spike"}, r.stats.phaseNames)
	rows := r.summary()
	require.Equal(t, "peak: requests / failed", rows[17][0])
	require.Equal(t, "3 / 1", rows[17][1])
	require.Equal(t, "spike: requests / failed", rows[21][0])
}

func TestWorkloadStartDelay(t *testing.T) {
	for _, model := range []string{"as-deployed", "option-a", "pgxpool"} {
		t.Run(model, func(t *testing.T) {
			cfg := testConfig(model)
			cfg.Services[0].Workload = phaseWorkload()
			cfg.Cluster.Handshake = LatencyRange{time.Second, time.Second}
			r, err := newComparison(cfg, "")
			require.NoError(t, err)
			advance(r, 3*time.Second)
			require.EqualValues(t, 1, r.stats.startDelay.count)
			if model == "pgxpool" {
				require.Zero(t, r.stats.startDelay.maximum)
			} else {
				require.GreaterOrEqual(t, r.stats.startDelay.maximum, time.Second)
			}
		})
	}
}

func TestWorkloadStartDelayFillTimeoutAndUnstartedPools(t *testing.T) {
	cfg := testConfig("as-deployed")
	cfg.Cluster.MaxConnections = 1 // Target 2 can never fill.
	cfg.Services[0].Workload = phaseWorkload()
	r, err := newComparison(cfg, "")
	require.NoError(t, err)
	advance(r, 30*time.Second-time.Nanosecond)
	require.Zero(t, r.stats.startDelay.count)
	advance(r, time.Nanosecond)
	require.EqualValues(t, 1, r.stats.startDelay.count)
	require.Equal(t, 30*time.Second, r.stats.startDelay.maximum)
	p := r.instances[0].newPool("closed-before-work", false)
	p.start()
	p.instance.workload(p, cfg.Services[0].Workload, 0)
	p.close()
	advance(r, time.Second)
	require.EqualValues(t, 1, r.stats.startDelay.count, "pools closed before their first request have no observation")
}
