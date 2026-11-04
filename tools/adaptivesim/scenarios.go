package adaptivesim

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/taskrate"
	"go.temporal.io/server/common/dynamicconfig"
)

type (
	// scenarioDef is a named scenario and its variants. The first variant is the default.
	scenarioDef struct {
		name, about string
		variants    []variant
		build       func(v variant) Scenario
	}

	variant struct {
		name, about string
		// For the short pool: how the store reads, and whether the gate admits its callers.
		read ReadPath
		gate bool
		// For start-up: the capacity controller's QueuedGrace.
		grace time.Duration
		// The capacity and task scheduler rate controllers' modes, where the scenario
		// varies them.
		capacity, taskRate string
	}
)

// pathVariants are the plugin's paths in the short pool comparison: today's common store
// path, the gate alone, the pipelined read alone, and both, which #108 built.
var pathVariants = []variant{
	{name: "pipeline-gate", about: "the gate and the pipelined read, as the plugin has them", read: PipelinedRead, gate: true},
	{name: "base", about: "the common store's nine-query read, with no gate", read: SequentialRead},
	{name: "gate", about: "the gate, with the nine-query read", read: SequentialRead, gate: true},
	{name: "pipeline", about: "the pipelined read, with no gate", read: PipelinedRead},
}

var scenarios = []scenarioDef{
	{
		name:     "short-pool-burst",
		about:    "4 connections and 22 ms to the store: 200 reloaded tasks at the start, then 9 tasks/s for 150 s",
		variants: pathVariants,
		build: func(v variant) Scenario {
			return shortPool("short-pool-burst", v, Load{
				Reload: Reload{Tasks: 200},
				Tasks:  []Phase{{Duration: 150 * time.Second, Rate: 9}},
			})
		},
	},
	{
		name:     "short-pool-overload",
		about:    "4 connections and 22 ms to the store: 24 tasks/s for 150 s, about 130% of what the pool serves",
		variants: pathVariants,
		build: func(v variant) Scenario {
			return shortPool("short-pool-overload", v, Load{
				Tasks: []Phase{{Duration: 150 * time.Second, Rate: 24}},
			})
		},
	},
	{
		name:  "startup",
		about: "4 hosts of 64 connections start within 10 s and reload 100 tasks each as they take their shards, with the capacity controller enabled",
		variants: []variant{
			{name: "grace", about: "QueuedGrace 30 s, as #107 set it", grace: 30 * time.Second},
			{name: "no-grace", about: "QueuedGrace 0, as before #107"},
		},
		build: startup,
	},
	{
		name: "slow-store",
		about: "one host of 48 connections and 128 shards, the store's replies 40 ms later from 60 s to 150 s: " +
			"80 tasks/s and 10 workflows/s for 240 s, with the scheduler's rate limiter on",
		variants: []variant{
			{name: "cap", about: "the capacity controller enabled, the task scheduler rate in shadow", capacity: adaptive.ModeNameEnabled, taskRate: adaptive.ModeNameShadow},
			{name: "cap-tsr", about: "both enabled", capacity: adaptive.ModeNameEnabled, taskRate: adaptive.ModeNameEnabled},
		},
		build: slowStore,
	},
	{
		name: "surge",
		about: "one host whose static persistence rate is 100/s: 50 tasks/s, 120/s from 60 s to 120 s, " +
			"and 5 workflows/s, for 240 s, with the scheduler's rate limiter on and the capacity controller in shadow",
		variants: []variant{
			{name: "tsr", about: "the task scheduler rate enabled", taskRate: adaptive.ModeNameEnabled},
			{name: "shadow", about: "the task scheduler rate in shadow", taskRate: adaptive.ModeNameShadow},
		},
		build: surge,
	},
	{
		name:     "fleet-start",
		about:    "4 hosts of 500 connections start within 2 s on shared pacing, for 3 h: poolsim's pgxpool-fleet-start",
		variants: []variant{{name: "default"}},
		build:    func(variant) Scenario { return fleetStart() },
	},
	{
		name:     "workflows",
		about:    "2 hosts of 64 connections run 10 workflows/s each for 120 s, through 32 pollers a host",
		variants: []variant{{name: "default"}},
		build:    func(variant) Scenario { return workflows() },
	},
}

// Lookup returns a named scenario's variant; an empty variant is the scenario's first.
func Lookup(name, variantName string) (Scenario, error) {
	for _, def := range scenarios {
		if def.name != name {
			continue
		}
		if variantName == "" {
			return def.build(def.variants[0]), nil
		}
		for _, v := range def.variants {
			if v.name == variantName {
				sc := def.build(v)
				sc.Name += "/" + v.name
				return sc, nil
			}
		}
		return Scenario{}, fmt.Errorf("scenario %s has no variant %q; it has %s", name, variantName, strings.Join(variantNames(def), ", "))
	}
	return Scenario{}, fmt.Errorf("no scenario %q; there are %s", name, strings.Join(Names(), ", "))
}

// Names are the scenarios' names.
func Names() []string {
	names := make([]string, len(scenarios))
	for i, def := range scenarios {
		names[i] = def.name
	}
	return names
}

func variantNames(def scenarioDef) []string {
	names := make([]string, len(def.variants))
	for i, v := range def.variants {
		names[i] = v.name
	}
	return names
}

// dsqlCluster is DSQL's connection admission and limits.
func dsqlCluster() Cluster {
	return Cluster{Rate: 100, Burst: 1000, MaxConnections: 10000, MaxAge: 60 * time.Minute}
}

// rigNetwork is the local rig of the short pool comparison: 22 ms to the database, 5 ms at
// the database for each statement, and 100 ms more for each new connection, as from the
// laptop to eu-west-1 in the appreciation.
func rigNetwork() Network {
	return Network{
		RoundTrip: 22 * time.Millisecond,
		Server:    5 * time.Millisecond,
		Pipelined: 5 * time.Millisecond,
		Jitter:    0.1,
		Connect:   Range{Min: 140 * time.Millisecond, Max: 170 * time.Millisecond},
		Cancel:    60 * time.Millisecond,
	}
}

// historyModel is history's task processing in the local rig: 512 task workers, a 3 s
// deadline for each attempt, 16 shards, and its retries.
func historyModel() History {
	return History{
		Shards:             16,
		Workers:            512,
		Deadline:           3 * time.Second,
		TxRoundTrips:       7,
		RetryDelay:         50 * time.Millisecond,
		ImmediateAttempts:  10,
		UnavailableBackoff: Backoff{Initial: time.Second, Coefficient: 1.1},
		ExhaustedBackoff:   Backoff{Initial: 3 * time.Second, Coefficient: 1.5},
	}
}

func servicePool(conns int) Pool {
	return Pool{MaxConns: conns, MinConns: conns, Lifetime: 50 * time.Minute, LifetimeJitter: 4 * time.Minute, HealthCheck: time.Minute}
}

// localPacing is the plugin's default pacing: 8 attempt slots a process and a limiter of
// 10 connections a second with a burst of 100.
func localPacing() Pacing {
	return Pacing{AttemptSlots: 8, Rate: 10, Burst: 100}
}

// capacityParams is history's persistence rate limiting with the capacity controller in
// the given mode.
func capacityParams(mode string) Capacity {
	params := dynamicconfig.DefaultDynamicRateLimitingParams
	params.Enabled = true
	params.Capacity.Mode = mode
	return Capacity{Params: params, Static: 2000, BurstRatio: 1}
}

func signalWindows() Signals {
	return Signals{Window: time.Second, Windows: 10, PlaneInterval: time.Second, PlaneWindows: 3}
}

// shortPool is finding 7 of the appreciation, as the local rig modeled it: one history
// host with 4 connections and 22 ms to the database, and the capacity controller in shadow.
func shortPool(name string, v variant, load Load) Scenario {
	return Scenario{
		Name:     name,
		Seed:     1,
		Duration: 150 * time.Second,
		Drain:    90 * time.Second,
		Cluster:  dsqlCluster(),
		Network:  rigNetwork(),
		Hosts:    1,
		Host: Host{
			Pool:     servicePool(4),
			Gate:     v.gate,
			Read:     v.read,
			Pacing:   localPacing(),
			History:  historyModel(),
			Workers:  Workers{Pollers: 32, Steps: 1},
			Capacity: capacityParams(adaptive.ModeNameShadow),
		},
		Signals: signalWindows(),
		Load:    load,
	}
}

// startup is finding 1 of the appreciation: as each host takes its shards, it reloads
// their tasks, which queue on its pool for a few seconds. The hosts run today's common
// store path, as the appreciation's did.
func startup(v variant) Scenario {
	capacity := capacityParams(adaptive.ModeNameEnabled)
	capacity.Params.Capacity.QueuedGrace = v.grace
	return Scenario{
		Name:        "startup",
		Seed:        1,
		Duration:    90 * time.Second,
		Drain:       10 * time.Second,
		Cluster:     dsqlCluster(),
		Network:     rigNetwork(),
		Hosts:       4,
		StartWindow: 10 * time.Second,
		Host: Host{
			Pool:     servicePool(64),
			Read:     SequentialRead,
			Pacing:   localPacing(),
			History:  historyModel(),
			Workers:  Workers{Pollers: 32, Steps: 1},
			Capacity: capacity,
		},
		Signals: signalWindows(),
		Load: Load{
			Reload: Reload{Tasks: 100, At: 2 * time.Second},
			Tasks:  []Phase{{Duration: 90 * time.Second, Rate: 30}},
		},
	}
}

// taskHost is a host for the task scheduler rate: the plugin's path, enough shards that
// the shards' writes don't bind first, and the scheduler's rate limiter on.
func taskHost(conns, shards int, static float64, v variant) Host {
	host := Host{
		Pool:     servicePool(conns),
		Gate:     true,
		Read:     PipelinedRead,
		Pacing:   localPacing(),
		History:  historyModel(),
		Workers:  Workers{Pollers: 32, Steps: 2, Process: 50 * time.Millisecond, RetryDelay: 10 * time.Second},
		Capacity: capacityParams(v.capacity),
	}
	if v.capacity == "" {
		host.Capacity = capacityParams(adaptive.ModeNameShadow)
	}
	host.Capacity.Static = static
	host.History.Shards = shards
	host.History.SchedulerRateLimiter = true
	host.Controllers.TaskRate = taskrate.DefaultSettings
	host.Controllers.TaskRate.Mode = v.taskRate
	return host
}

// slowStore is the appreciation's slow store, on a host whose pool binds while the store
// is slow and whose static persistence rate is close enough to the load for the capacity
// controller's cuts to bind: it cuts, and the task scheduler rate follows it.
func slowStore(v variant) Scenario {
	return Scenario{
		Name:     "slow-store",
		Seed:     1,
		Duration: 240 * time.Second,
		Drain:    60 * time.Second,
		Cluster:  dsqlCluster(),
		Network:  rigNetwork(),
		Hosts:    1,
		Host:     taskHost(48, 128, 300, v),
		Signals:  signalWindows(),
		Slow:     Slow{At: 60 * time.Second, Duration: 90 * time.Second, Delay: 40 * time.Millisecond},
		Load: Load{
			Tasks:     []Phase{{Duration: 240 * time.Second, Rate: 80}},
			Workflows: []Phase{{Duration: 240 * time.Second, Rate: 10}},
		},
	}
}

// surge is a surge of tasks past the host's static persistence rate. The scheduler holds
// the tasks over the task scheduler rate until the surge ends, where without it they fail
// on the persistence rate limit and back off.
func surge(v variant) Scenario {
	return Scenario{
		Name:     "surge",
		Seed:     1,
		Duration: 240 * time.Second,
		Drain:    120 * time.Second,
		Cluster:  dsqlCluster(),
		Network:  rigNetwork(),
		Hosts:    1,
		Host:     taskHost(64, 64, 100, v),
		Signals:  signalWindows(),
		Load: Load{
			Tasks: []Phase{
				{Duration: 60 * time.Second, Rate: 50},
				{At: 60 * time.Second, Duration: 60 * time.Second, Rate: 120},
				{At: 120 * time.Second, Duration: 120 * time.Second, Rate: 50},
			},
			Workflows: []Phase{{Duration: 240 * time.Second, Rate: 5}},
		},
	}
}

// fleetStart is poolsim's scenarios/pgxpool-fleet-start.yaml: four hosts of 500
// connections start within 2 s, on pacing shared through the DynamoDB token bucket, each
// with two short-lived startup pools and a light load of bare statements, for three hours.
func fleetStart() Scenario {
	return Scenario{
		Name:        "fleet-start",
		Seed:        42,
		Duration:    3 * time.Hour,
		Cluster:     dsqlCluster(),
		Network:     Network{RoundTrip: time.Millisecond, Connect: Range{Min: 50 * time.Millisecond, Max: 200 * time.Millisecond}},
		Hosts:       4,
		StartWindow: 2 * time.Second,
		Host: Host{
			Pool: servicePool(500),
			Pacing: Pacing{
				AttemptSlots: 8, Rate: 100, Burst: 1000, Shared: true,
				Latency: Range{Min: 5 * time.Millisecond, Max: 10 * time.Millisecond},
			},
			StartupPools: StartupPools{Pools: 2, Connections: 3, Duration: time.Second},
			History:      historyModel(),
			Capacity:     capacityParams(adaptive.ModeNameDisabled),
		},
		Signals: Signals{Window: time.Second, Windows: 10},
		Load: Load{Pool: PoolLoad{
			Every: time.Second, Burst: 10, MaxInUse: 30,
			Hold: 10 * time.Millisecond, HoldJitter: 10 * time.Millisecond, MaxWait: 10 * time.Second,
		}},
	}
}

// workflows runs the coarse workflow model on the plugin's current path, with the capacity
// controller in shadow.
func workflows() Scenario {
	return Scenario{
		Name:     "workflows",
		Seed:     1,
		Duration: 120 * time.Second,
		Drain:    30 * time.Second,
		Cluster:  dsqlCluster(),
		Network:  rigNetwork(),
		Hosts:    2,
		Host: Host{
			Pool:    servicePool(64),
			Gate:    true,
			Read:    PipelinedRead,
			Pacing:  localPacing(),
			History: historyModel(),
			Workers: Workers{
				Pollers: 32, Steps: 3,
				Process: 50 * time.Millisecond, RetryDelay: 10 * time.Second,
			},
			Capacity: capacityParams(adaptive.ModeNameShadow),
		},
		Signals: signalWindows(),
		Load: Load{
			Reload:    Reload{Tasks: 50, At: time.Second},
			Workflows: []Phase{{Duration: 120 * time.Second, Rate: 10}},
		},
	}
}

// describe lists the scenarios and their variants.
func describe() string {
	var b strings.Builder
	for _, def := range scenarios {
		fmt.Fprintf(&b, "%s: %s\n", def.name, def.about)
		for i, v := range def.variants {
			if v.about == "" {
				continue
			}
			marker := ""
			if i == 0 {
				marker = " (default)"
			}
			fmt.Fprintf(&b, "  %s%s: %s\n", v.name, marker, v.about)
		}
	}
	return b.String()
}
