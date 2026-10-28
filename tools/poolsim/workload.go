package main

import (
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
