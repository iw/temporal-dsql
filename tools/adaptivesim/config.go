package adaptivesim

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/server/common/adaptive/memory"
	"go.temporal.io/server/common/adaptive/taskrate"
	workercount "go.temporal.io/server/common/adaptive/workers"
	"go.temporal.io/server/common/dynamicconfig"
)

type (
	// Scenario is one simulated run.
	Scenario struct {
		Name string
		Seed uint64
		// The load runs for Duration, then the run goes on for Drain with no new load.
		Duration, Drain time.Duration
		Cluster         Cluster
		Network         Network
		// Hosts is how many history hosts run, each in its own process. Each starts at a
		// time drawn from [0, StartWindow].
		Hosts       int
		StartWindow time.Duration
		Host        Host
		Signals     Signals
		Load        Load
		// Slow holds back each of the store's replies.
		Slow Slow
	}

	// Slow holds back each of the store's replies by Delay, from At for Duration after the
	// run's start, as the appreciation's proxy did for the slow store.
	Slow struct {
		At, Duration, Delay time.Duration
	}

	// Cluster is DSQL's side of the connections, shared by every host.
	Cluster struct {
		// Rate and Burst are DSQL's connection admission: new connections a second, and
		// how many it admits at once.
		Rate, Burst float64
		// MaxConnections is the cluster's connection limit.
		MaxConnections int
		// MaxAge is when DSQL closes a connection.
		MaxAge time.Duration
	}

	// Network is what the store's statements and connections cost.
	Network struct {
		// RoundTrip is one round trip to the store, and Server the store's own time in each.
		RoundTrip, Server time.Duration
		// Pipelined is the store's time for the pipelined read's statements, sent in one
		// round trip.
		Pipelined time.Duration
		// Jitter spreads each statement's time by up to this share either way.
		Jitter float64
		// Connect is what opening a connection costs: TCP, TLS and IAM authentication. A
		// connection DSQL rejects costs the same.
		Connect Range
		// Cancel is how long a cancel request's connection stays open.
		Cancel time.Duration
	}

	// Range is a span that a duration is drawn from uniformly.
	Range struct {
		Min, Max time.Duration
	}

	// Host is one history host and its process.
	Host struct {
		Pool Pool
		// Gate admits the store's callers to the pool by the plugin's gate.Policy.
		Gate bool
		// Read is how the store reads a workflow's mutable state.
		Read         ReadPath
		Pacing       Pacing
		StartupPools StartupPools
		History      History
		Workers      Workers
		Capacity     Capacity
		Controllers  Controllers
		Memory       Memory
	}

	// Controllers are the dynamic config of the host's controllers that step on the
	// signal plane's snapshots. A zero setting is its controller's default, which leaves it
	// disabled.
	Controllers struct {
		TaskRate    taskrate.Settings
		WorkerCount workercount.Settings
		Memory      memory.Settings
	}

	// Pool is the plugin's pgxpool.
	Pool struct {
		MaxConns, MinConns int
		// A connection's lifetime is Lifetime plus whole seconds of jitter up to
		// LifetimeJitter.
		Lifetime, LifetimeJitter time.Duration
		HealthCheck              time.Duration
	}

	// Pacing is the plugin's BeforeConnect.
	Pacing struct {
		// AttemptSlots bounds the connects in a process that wait for the limiter at once.
		AttemptSlots int
		// Rate and Burst are the plugin's connection limiter. With Shared, every host takes
		// tokens from one bucket, as with the DynamoDB token bucket, each reservation taking
		// Latency; otherwise each host has a bucket of its own, as with local pacing.
		Rate, Burst float64
		Shared      bool
		Latency     Range
	}

	// StartupPools are the short-lived pools a process opens as it starts, each with a
	// light load, closed Duration after their first connection.
	StartupPools struct {
		Pools, Connections int
		Duration           time.Duration
	}

	// History is the history service's part.
	History struct {
		// Shards is how many shards the host owns. A shard's writes run one at a time.
		Shards int
		// Workers is how many task attempts each queue's scheduler runs at once: its worker
		// count setting, which the worker count controllers drive.
		Workers int
		// Deadline is each task attempt's and each API call's deadline.
		Deadline time.Duration
		// TxRoundTrips is a write transaction's round trips: BEGIN, its statements and
		// COMMIT.
		TxRoundTrips int
		// RetryDelay is when the persistence client retries a call that returned
		// Unavailable, once.
		RetryDelay time.Duration
		// A task attempt that fails with Unavailable is resubmitted at once until the
		// task's ImmediateAttempts attempt, then after UnavailableBackoff. A task is
		// resubmitted at once after its first ResourceExhausted, and after
		// ExhaustedBackoff after later ones.
		ImmediateAttempts  int
		UnavailableBackoff Backoff
		ExhaustedBackoff   Backoff
		// SchedulerRateLimiter turns history's task scheduler rate limiter on, with its
		// shadow mode off. Its own limit is the static persistence rate, and the task rate
		// controller holds tasks to its rate on top while it's enabled.
		SchedulerRateLimiter bool
		// CacheMaxBytes limits the workflow cache by size, as history.cacheSizeBasedLimit
		// with history.hostLevelCacheMaxSizeBytes does; 0 leaves it unlimited. Each
		// workflow's state takes CacheEntryBytes of it.
		CacheMaxBytes, CacheEntryBytes int
		// MaxPendingTasks and CriticalPendingTasks are each shard's queues' pending-task
		// limits, history.queuePendingTasksMaxCount and
		// history.queuePendingTaskCriticalCount. A shard's queue loads tasks only while it
		// holds fewer than MaxPendingTasks; 0 sets no limit. The critical count only bounds
		// the memory budget.
		MaxPendingTasks, CriticalPendingTasks int
	}

	// Memory is the history process's heap against GOMEMLIMIT.
	Memory struct {
		// Limit is GOMEMLIMIT, in bytes; 0 sets none.
		Limit int
		// Base is the heap besides the workflow cache and the tasks the queues hold, and
		// TaskBytes each task's.
		Base, TaskBytes int
		// Extra is more heap for a while, as another part of the process might take.
		Extra Extra
	}

	// Extra is Bytes more heap from At for Duration after the run's start.
	Extra struct {
		At, Duration time.Duration
		Bytes        int
	}

	// Backoff is an exponential backoff: Initial × Coefficient^n, at most Max.
	Backoff struct {
		Initial     time.Duration
		Coefficient float64
		Max         time.Duration
	}

	// Workers are the SDK workers that poll the host's workflow tasks.
	Workers struct {
		// Pollers is how many tasks the workers hold at once. Each poller holds its task
		// until history has written the worker's response.
		Pollers int
		// Steps is how many worker tasks each workflow has after its start.
		Steps int
		// Process is how long a worker takes over a task before it responds.
		Process time.Duration
		// RetryDelay is how long a task whose response failed waits before it's dispatched
		// again, standing in for the task's timeout.
		RetryDelay time.Duration
	}

	// Capacity is the persistence capacity controller's dynamic config on the host.
	Capacity struct {
		// Params is the persistence rate limiter's dynamic config, with the controller's
		// envelope in Params.Capacity.
		Params dynamicconfig.DynamicRateLimitingParams
		// Static is the host's static persistence rate, in calls a second.
		Static float64
		// BurstRatio is the limiter's burst as a share of its rate.
		BurstRatio float64
	}

	// Signals are the health signals' windows and the signal plane's tick.
	Signals struct {
		// Window is the persistence latency window, and Windows how many the host keeps.
		Window  time.Duration
		Windows int
		// PlaneInterval is the plane's tick, or 0 for none, and PlaneWindows how many of
		// the newest windows a snapshot covers.
		PlaneInterval time.Duration
		PlaneWindows  int
	}

	// Load is what each host is given to do.
	Load struct {
		// Reload is the cold tasks a host reloads as it takes its shards.
		Reload Reload
		// Tasks are warm tasks: writes for workflows the host has cached.
		Tasks []Phase
		// Workflows are workflow starts.
		Workflows []Phase
		// Pool is a load of bare statements on the store, without history.
		Pool PoolLoad
	}

	// Reload spreads Tasks cold tasks over [At, At+Over] after the host starts.
	Reload struct {
		Tasks    int
		At, Over time.Duration
	}

	// Phase is a rate, per host and per second, from At for Duration.
	Phase struct {
		At, Duration time.Duration
		Rate         float64
	}

	// PoolLoad starts up to Burst statements every Every, keeping at most MaxInUse at once.
	// Each holds its connection for Hold plus up to HoldJitter, and gives up waiting after
	// MaxWait.
	PoolLoad struct {
		Every            time.Duration
		Burst, MaxInUse  int
		Hold, HoldJitter time.Duration
		MaxWait          time.Duration
	}
)

// ReadPath is how the store reads a workflow's mutable state.
type ReadPath int

const (
	// SequentialRead is the common store's read: nine queries in turn, each taking a
	// connection of its own.
	SequentialRead ReadPath = iota
	// PipelinedRead is the plugin's read: the nine queries in one read-only transaction,
	// sent as one batch on one connection.
	PipelinedRead
)

func (r ReadPath) String() string {
	if r == PipelinedRead {
		return "pipelined"
	}
	return "sequential"
}

// readQueries is how many queries a workflow read makes.
const readQueries = 9

func (sc *Scenario) validate() error {
	var errs []error
	if sc.Duration <= 0 {
		errs = append(errs, errors.New("the scenario needs a positive Duration"))
	}
	if sc.Hosts < 1 {
		errs = append(errs, errors.New("the scenario needs at least one host"))
	}
	if sc.Cluster.Rate <= 0 || sc.Cluster.Burst < 1 || sc.Cluster.MaxConnections < 1 || sc.Cluster.MaxAge <= 0 {
		errs = append(errs, errors.New("the cluster needs a positive Rate, Burst, MaxConnections and MaxAge"))
	}
	if sc.Network.RoundTrip <= 0 || sc.Network.Connect.Max < sc.Network.Connect.Min {
		errs = append(errs, errors.New("the network needs a positive RoundTrip and a Connect range"))
	}
	if sc.Signals.Window < 100*time.Millisecond || sc.Signals.Windows < 1 || sc.Signals.PlaneInterval < 0 {
		errs = append(errs, errors.New("the signals need a Window of at least 100 ms, and Windows"))
	}
	errs = append(errs, sc.Host.validate(), sc.Load.validate(&sc.Host))
	return errors.Join(errs...)
}

func (h *Host) validate() error {
	var errs []error
	if h.Pool.MaxConns < 1 || h.Pool.MinConns > h.Pool.MaxConns || h.Pool.HealthCheck <= 0 {
		errs = append(errs, fmt.Errorf("the pool needs 0 <= MinConns <= MaxConns, MaxConns >= 1 and a HealthCheck: %+v", h.Pool))
	}
	if h.Pacing.AttemptSlots < 1 || h.Pacing.Rate <= 0 || h.Pacing.Burst < 1 {
		errs = append(errs, errors.New("the pacing needs AttemptSlots, a Rate and a Burst"))
	}
	if h.History.Shards < 1 || h.History.Workers < 1 || h.History.Deadline <= 0 || h.History.TxRoundTrips < 1 {
		errs = append(errs, errors.New("history needs Shards, Workers, a Deadline and TxRoundTrips"))
	}
	if h.Capacity.Static <= 0 {
		errs = append(errs, errors.New("the capacity limiter needs a positive Static rate"))
	}
	return errors.Join(errs...)
}

func (ld *Load) validate(h *Host) error {
	var errs []error
	if len(ld.Workflows) > 0 && (h.Workers.Pollers < 1 || h.Workers.Steps < 1) {
		errs = append(errs, errors.New("a workflow load needs Workers.Pollers and Workers.Steps"))
	}
	if ld.Pool.Every > 0 && (ld.Pool.Burst < 1 || ld.Pool.MaxInUse < 1) {
		errs = append(errs, errors.New("a pool load needs a Burst and MaxInUse"))
	}
	return errors.Join(errs...)
}

// holds reports whether the store's replies are held back at now.
func (s Slow) holds(now time.Time) bool {
	at := now.Sub(epoch)
	return s.Delay > 0 && at >= s.At && at < s.At+s.Duration
}

// delay is the backoff's nth delay, from 0.
func (b Backoff) delay(n int) time.Duration {
	d := float64(b.Initial)
	for range n {
		d *= b.Coefficient
		if b.Max > 0 && d >= float64(b.Max) {
			return b.Max
		}
	}
	return time.Duration(d)
}
