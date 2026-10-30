// Package signals is the per-host signal plane that the adaptive controllers read. Each
// tick it publishes one immutable Snapshot of the host: persistence health and task
// queueing delay, RPC handler health, the Go scheduler and heap, and the memory limit.
// Controllers report their decisions to it, and it can write snapshots and decisions to
// a trace file for offline replay. See docs/dsql/adaptive-behaviour.md, Proposal 2.
//
// The plane stays host-local: it never shares signals with other hosts.
package signals

import (
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
)

type (
	// Snapshot is the plane's view of a host at one tick. It's never changed once
	// published, so readers mustn't change it either.
	Snapshot struct {
		Time time.Time
		// Persistence is the persistence health over the newest Settings.Windows latency
		// windows, including the p90 of task queueing delay on history hosts.
		Persistence persistence.HealthSnapshot
		RPC         RPC
		Runtime     Runtime
		Memory      Memory
		// Decisions holds each controller's latest decision, by controller. Traces record
		// decisions as their own lines instead.
		Decisions map[string]adaptive.Decision `json:"-"`
	}

	// RPC is the health of the host's RPC handlers, which only history records, over its
	// health check aggregator's windows.
	RPC struct {
		Reported   bool
		Mean       time.Duration
		ErrorRatio float64
		// P99 is 0 unless system.historyHealthSignalUsePercentiles is set, which also
		// makes history's deep health check use it in place of the mean.
		P99 time.Duration
	}

	// Runtime is the Go runtime's view of the process.
	Runtime struct {
		// SchedP99 is the 99th percentile of how long runnable goroutines waited to run
		// since the previous snapshot, and SchedSamples how many waits it covers.
		SchedP99     time.Duration
		SchedSamples uint64
		Goroutines   uint64
		HeapLive     uint64 // bytes of heap live after the last GC
		HeapGoal     uint64 // heap size in bytes at which the next GC starts
		Mapped       uint64 // bytes of memory the runtime has mapped
	}

	// Memory is the process's memory limit and use.
	Memory struct {
		// Limit and Usage come from the process's cgroup: memory.max and memory.current,
		// or memory.limit_in_bytes and memory.usage_in_bytes on cgroup v1. Limit is 0 when
		// the cgroup sets none, and both are 0 outside a cgroup.
		Limit, Usage uint64
		// GoLimit is GOMEMLIMIT, the runtime's soft limit, or 0 when it isn't set.
		GoLimit uint64
	}

	// Settings is the plane's dynamic config.
	Settings struct {
		// Enabled runs the plane. It's off by default.
		Enabled bool
		// Interval is the time between snapshots.
		Interval time.Duration
		// Windows is how many of the newest persistence latency windows a snapshot covers.
		Windows int
		// TraceDirectory, when set, is where each service writes its snapshots and its
		// controllers' decisions, as one JSON object per line.
		TraceDirectory string
		// TraceMaxBytes caps each trace file. Recording stops at the cap.
		TraceMaxBytes int64
	}
)

// DefaultSettings leaves the plane off.
var DefaultSettings = Settings{
	Interval:      time.Second,
	Windows:       3,
	TraceMaxBytes: 1 << 30,
}

// Config is the plane's dynamic config setting.
var Config = dynamicconfig.NewGlobalTypedSetting(
	"adaptive.signals",
	DefaultSettings,
	`adaptive.signals configures the per-host signal plane that the adaptive controllers read.
Enabled runs it, taking a snapshot every Interval over the newest Windows persistence latency
windows. When TraceDirectory is set, each service writes its snapshots and its controllers'
decisions there as JSON lines, up to TraceMaxBytes per file.`,
)
