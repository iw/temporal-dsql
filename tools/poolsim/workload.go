package main

import (
	"math/rand"
	"time"
)

// WorkloadConfig configures simulated database workload.
type WorkloadConfig struct {
	Phases []WorkloadPhase `json:"phases" yaml:"phases"`

	// Enabled controls whether workload simulation runs.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// MaxInUse is the target number of concurrent connections in use.
	// Workload will try to maintain this many active "queries".
	MaxInUse int `json:"maxInUse" yaml:"maxInUse"`

	// HoldTime is how long each "query" holds a connection.
	HoldTime time.Duration `json:"holdTime" yaml:"holdTime"`

	// HoldJitter adds randomness to hold time (0 to HoldJitter added).
	HoldJitter time.Duration `json:"holdJitter" yaml:"holdJitter"`

	// SpawnEvery is how often the workload spawns new work.
	SpawnEvery time.Duration `json:"spawnEvery" yaml:"spawnEvery"`

	// ArrivalBurst is max work items to start per spawn tick.
	ArrivalBurst int `json:"arrivalBurst" yaml:"arrivalBurst"`

	// MaxWait is max time to wait for a connection before giving up.
	MaxWait time.Duration `json:"maxWait" yaml:"maxWait"`
}

// ReservoirWorkload simulates database queries consuming connections from the reservoir.
// This models Temporal's persistence layer making database calls.
type ReservoirWorkload struct {
	ServiceName string
	Endpoint    string

	Cfg WorkloadConfig

	Reservoir *Reservoir
	Metrics   *Metrics
	Sim       *Sim

	startedAt time.Time
}

// Start begins the workload simulation.
func (w *ReservoirWorkload) Start(at time.Time) {
	if !w.Cfg.Enabled {
		return
	}
	w.startedAt = at
	w.Sim.Schedule(at, w.ServiceName+":workload_start", func() {
		w.tick()
	})
}

func (w *ReservoirWorkload) tick() {
	now := w.Sim.Now()

	// How many connections are currently in use?
	stats := w.Reservoir.Stats()
	inUse := stats.InUse

	// How many more do we need to reach MaxInUse?
	need := w.Cfg.MaxInUse - inUse
	if need <= 0 {
		// Already at target, schedule next tick
		w.Sim.After(w.Cfg.SpawnEvery, w.ServiceName+":workload_tick", func() {
			w.tick()
		})
		return
	}

	// Start up to ArrivalBurst work items
	toStart := need
	if toStart > w.Cfg.ArrivalBurst {
		toStart = w.Cfg.ArrivalBurst
	}

	for i := 0; i < toStart; i++ {
		w.tryStartOne(now)
	}

	w.Sim.After(w.Cfg.SpawnEvery, w.ServiceName+":workload_tick", func() {
		w.tick()
	})
}

func (w *ReservoirWorkload) tryStartOne(now time.Time) {
	// Try to checkout from reservoir (non-blocking)
	conn, ok := w.Reservoir.TryCheckout(now)
	if !ok {
		// Reservoir empty - this is what we want to avoid!
		// In production, this would return ErrBadConn and trigger retry
		// For simulation, we just record the empty event (already done in TryCheckout)
		return
	}

	w.Metrics.RecordWorkStarted(now, w.ServiceName)

	// Calculate hold time with jitter
	hold := w.Cfg.HoldTime
	if w.Cfg.HoldJitter > 0 {
		hold += time.Duration(rand.Int63n(int64(w.Cfg.HoldJitter + 1)))
	}

	// Schedule release
	w.Sim.After(hold, w.ServiceName+":work_release", func() {
		releaseTime := w.Sim.Now()
		w.Reservoir.Return(conn, releaseTime)
		w.Metrics.RecordWorkCompleted(releaseTime, w.ServiceName)
	})
}
