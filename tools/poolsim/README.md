# DSQL Pool Simulator

`poolsim` has two execution paths. Existing scenario files, with no new pool
settings, retain the original reservoir engine and CSV format. An explicit
`model`, a comparison flag, or any instance/pool settings enable the shared DSQL
cluster engine described below. Neither path connects to a database.

## Compare the deployed reservoir, option A and pgxpool

```bash
go run ./tools/poolsim -config tools/poolsim/scenarios/compare-400wps.yaml \
  -compare as-deployed,option-a,pgxpool -out /tmp/compare.csv
```

This prints a side-by-side Markdown table and writes
`/tmp/compare-as-deployed.csv`, `/tmp/compare-option-a.csv` and
`/tmp/compare-pgxpool.csv`. Without `-out`, comparison files are named
`poolsim-<variant>.csv` in the current directory. A single configured model also
writes a CSV, using `-out` literally when supplied. Comma-separated variants
must be unique. All variants use the same seed. Separate random streams govern
workload holds, pool lifetimes, cluster handshake latency, limiter RPC latency
and fleet start times. Event ties use insertion order; map iteration never
chooses which connection to acquire or drop.

| Variant / `model` | Behavior |
|---|---|
| `reservoir` | Direct FIFO checkout/return, continuous sequential refiller, guard checks and one-second scans. The legacy path remains the default; selecting it explicitly or with `-compare` adds the cluster/limiter/instance environment. |
| `reservoir-sql` | SQL idle connections are reused LIFO with session-reset pings and internal bad-connection retries; `driver.Open` takes from the reservoir immediately or fails empty. At `maxOpen`, callers queue until release or `workload.maxWait`. SQL keeps released connections indefinitely by default. |
| `as-deployed` | Named preset for `reservoir-sql`, forcing defect toggles to their deployed defaults, including the sequential refiller. |
| `option-a` | Named preset with lifetime enforcement and concurrent refill enabled, close leaks disabled and whole-pool reconnects disabled. |
| `pgxpool` | Demand creation, bounded total connections including constructors, idle LIFO, release-time lifetime enforcement, periodic health checks and the proposed limiter/guard hooks. |

`model` can be set at the top level or per service; a service value wins.
`-compare` overrides both so each run changes the entire fleet. The two named
reservoir presets override toggles; use `reservoir-sql` to vary them separately:

```yaml
model: reservoir-sql
services:
  - name: history
    replicas: 4
    targetReady: 500
    maxOpen: 500
    inflightLimit: 8
    enforceLifetimeInUse: false  # #29: true checks guard on SQL reuse/release
    concurrentRefill: false     # true opens up to inflightLimit per pool
    leakOnClose: true           # #30: closed pools retain physical connections
    reconnectOnConnError: true  # #31: each dead request queues a whole-pool rebuild
    ephemeral: {pools: 2, connections: 3, duration: 1s}
    pgxpool:
      maxConns: 500
      minConns: 500
      maxConnLifetime: 50m
      maxConnLifetimeJitter: 4m
      maxConnIdleTime: 0s
      healthCheckPeriod: 1m
      acquireGuard: true         # proposed BeforeAcquire guard; false is stock pgxpool
```

Lifetime enforcement runs at reuse/release, never in the middle of a query.
SQL-owned connections and the separate reservoir buffer both count against the
physical cap: at full SQL occupancy their total can reach `maxOpen +
targetReady`. Leaked connections remain charged to the old pool generation
until DSQL closes them, including connections opened just as a pool closes.
Dead requests enqueue serialized rebuilds, including late errors from a draining
old pool. Option A and pgxpool let acquired connections finish and close them
when returned after `Close`.

Every replica has an independent main pool, workload and in-flight semaphore.
Startup queues ephemeral work before main warmup so a large `MinConns` batch
cannot starve startup checks. Ephemeral pools run queries for `duration` after
the first successful acquire, then close; unsuccessful startup is bounded.
Reservoir ephemeral settings follow the design: ready/low watermark 5, initial
fill timeout 10s, local open limit 2 (the deployed sequential refiller only uses one).
Main reservoir workloads begin at the low watermark or after the 30s fill
window. PgX workloads begin immediately and wait in Acquire. PgX ephemeral
pools use `MinConns=0`, `MaxConns=ephemeral.connections`, and actually close.
All constructors share their instance's cap, default 8; no extra cap slots are
created for ephemeral pools. `ephemeral.pools` defaults to zero, `connections`
to 3 and `duration` to 1s. `replicas` defaults to 1.

### Reservoir fidelity corrections

The deployed `reservoirRefiller.loop` in
`common/persistence/sql/sqlplugin/dsql/driver/reservoir_refiller.go` calls
`openOne` synchronously. `as-deployed` preserves this sequential behavior.
Option A implements the concurrent refiller described in
`docs/dsql/reservoir-design.md`: up to `inflightLimit` constructors per main
pool, while `ready + constructing < targetReady`. Each failed open reserves
its refill slot for the 250ms backoff; other slots can continue. The shared
instance semaphore still bounds main and ephemeral handshakes together.
This changes warmup, replacement throughput and competition for cluster slots;
it does not reduce the independent ready buffer when SQL connections are busy.

Both SQL reservoir models now model `database/sql` session resets:
`reservoir_conn.go` forwards `ResetSession` to pgx v5.7.2's stdlib connection.
It pings after more than one second since the previous handout, **not** since
release. A stale idle connection detected by that ping is destroyed internally.
Go's `database/sql.DB.retry` allows two cached attempts, then forces a new
connection (here, a reservoir checkout) even if idle connections remain.
Option A's lifetime check (#29) returns the same `driver.ErrBadConn` from
`ResetSession`, so its expired-connection discards share those two attempts.
These discards do not trigger #31 rebuilds. A latent dead connection handed
out one second ago or less still fails the request as `dead`, triggering a
rebuild only in `as-deployed`. As with pgxpool, ping network latency is omitted.

### Workload phases

Comparison main pools accept optional workload overrides:

```yaml
workload:
  enabled: true
  maxInUse: 30
  holdTime: 10ms
  holdJitter: 10ms
  spawnEvery: 1s
  arrivalBurst: 10
  maxWait: 10s
  phases:
    - name: peak
      at: 40m
      ramp: 5m
      duration: 30m
      maxInUse: 450
      arrivalBurst: 450
      spawnEvery: 100ms
      holdTime: 500ms
      holdJitter: 1s
```

Times are measured from **simulation start**, so every service peaks together,
and replacements or rebuilds start at the current global phase level. During
`[at, at+ramp)`, only `maxInUse` interpolates linearly from base to phase level,
rounded toward the base integer; all other overrides apply immediately. The
plateau lasts `duration` after the ramp; at its end all base settings resume.
Zero ramp gives a step. Zero or omitted workload overrides inherit base values,
including `maxWait` and `holdJitter` (zero cannot disable base jitter).
Existing requests drain normally; no new requests spawn at or above the
current active-request limit, including requests waiting to acquire.

Each pool has one tick chain. Every tick selects the current settings and
schedules the earlier of `now + spawnEvery` and the next start, plateau or end
boundary. A phase therefore starts on time even between slow base ticks.
Ephemeral pools retain their fixed workload. Names must be nonblank, durations
positive, ramps and offsets nonnegative, and effective `maxInUse` at least one.
Within each service phases must be ordered and nonoverlapping; adjacent windows
are allowed. The legacy `ReservoirWorkload` configuration rejects phases.

## Cluster and client limiters

```yaml
limiter: tokenBucket                  # perSecond (default) | tokenBucket | adaptive
limiterPerSec: 100                    # only the legacy/perSecond client limiter
limiterLatency: {min: 5ms, max: 10ms} # successful logical tokenBucket acquire
cluster:
  maxConnections: 10000
  handshake: {min: 50ms, max: 200ms}
adaptive: {initialRate: 10, increase: 0.1, capacity: 10}
steadyAfter: 90m
```

The server always has one token bucket: 100 tokens/s, capacity 1,000, initially
full. Dial attempts without tokens get `53400`; attempts with a token but no
cap slot get `53300`. Both complete after handshake latency. Admission reserves
a physical slot during the handshake. A successful connection is cut exactly
60 minutes after dial admission; an active request fails immediately, and an
idle connection stays stale in its client pool until detected. Server cuts
count separately as idle or in use, including leaks.

The client `tokenBucket` is one global bucket shared by every pool and replica.
It uses a FIFO logical acquisition queue and an independent 5–10ms RPC delay
after token debit. `perSecond` uses the existing fixed-window limiter. The
adaptive fallback is a separate bucket per instance: successful handshakes add
`increase` to the rate, each `53400` halves it, with a 0.1/s floor; `53300` does
not change it. Bucket capacity stays fixed. This is a proposed fallback policy,
not a guarantee of fleet-wide rate safety. The instance semaphore is obtained
before the client token so permits cannot accumulate behind a saturated dial
queue. Variable token RPC latency can slightly bunch dials and cause a server
rejection even with identically sized client/server buckets.

## pgxpool fidelity

The implementation comments refer to the locally inspected pgx **v5.7.2** and
puddle **v2.2.2** sources. These are the modeled functions:

| Behavior | Source function(s) |
|---|---|
| Bound idle + acquired + constructing resources by MaxConns | puddle `Pool.acquire`, `createNewResource`, `CreateResource` |
| On-demand construction waits for limiter, instance slot and handshake | puddle `Pool.acquire`, `initResourceValue`; pgxpool `NewWithConfig` constructor / `BeforeConnect` |
| Caller timeout does not cancel resource construction | puddle `initResourceValue` |
| MinConns startup and health refill; cancel queued batch constructors on first failure | pgxpool `NewWithConfig`, `checkMinConns`, `createIdleResources` |
| Lifetime = configured lifetime plus nonnegative jitter, truncated to seconds, starting after connect | pgxpool `NewWithConfig` constructor |
| LIFO acquisition; health sweeps reverse returned idle order and preserve last-use time | puddle `tryAcquireIdleResource`, `AcquireAllIdle`, `Resource.ReleaseUnused`; `genstack.GenStack.Pop/Push/NextGen` |
| Ping after more than one second idle; destroy on failed ping and retry; proposed guard rejection also retries | pgxpool `Pool.Acquire`, `BeforeAcquire` |
| Destroy closed/expired resources on release; signal health after 500ms | pgxpool `Conn.Release`, `isExpired`, `triggerHealthCheck` |
| Refill before idle scan; skip scan on refill failure; lifetime can go one below MinConns; idle expiry stops at MinConns | pgxpool `checkHealth`, `checkConnsHealth` |
| Close idle immediately and acquired resources on their return | pgxpool `Pool.Close`; puddle `Pool.Close`, `releaseAcquiredResource` |

The proposed acquire hook checks lifetime minus the guard window; stock pgxpool
does not perform that lifetime check on Acquire. `pgxpool.acquireGuard: false`
turns the hook off, leaving Release and the health check to retire connections
past their lifetime. Under peak load the hook moves replacement into the
request path: a request that rejects a guarded idle connection creates the
replacement itself and waits for the limiter and handshake, where Release's
background refill would have let it take another idle connection
(`compare-400wps-peak-noguard.yaml`). With 50m lifetime, up to 4m
jitter, 45s guard and a 5m maximum transaction, even the conservative sum is
59m45s, below the server's 60m maximum. This protects active requests but does
**not** imply idle connections cannot be cut: if `MinConns` cannot be restored,
stock `checkHealth` skips its idle sweep. The oversubscribed comparison exposes
that behavior. The small scenarios use the same lifetime configuration.

Simplifications and deliberate differences:

- `maxConnIdleTime: 0` means **off**, as requested for the proposal. Stock
  v5.7.2's `checkConnsHealth` compares idle duration directly to zero.
- Health checks are atomic simulated callbacks, so there are no simultaneous
  old/new generations during `AcquireAllIdle`. Normal LIFO and the sweep's
  reversal are retained. Semaphore wakeups use deterministic FIFO ordering;
  real goroutine scheduling and database/sql waiter selection may differ.
- Ping has zero latency and detects latent dead connections only after >1s
  idle for pgxpool, or >1s since handout for SQL reservoirs. The simulation
  does not distinguish a latent disconnect from an already-known closed pgx
  connection, which real `ResetSession` rejects immediately regardless of age.
  It also starts the SQL reset clock on the first handout; real stdlib starts
  with a zero reset timestamp, so the first reuse always pings.
  Busy/transaction-status rejection,
  protocol framing, SQL errors other than connection loss, cancellation during
  a query, IAM refresh, distributed slot leases and visibility pools are not
  simulated. Successful queries return transaction-idle connections.
- DDB is a logical bucket with configurable RPC delay, not individual
  conditional-write retries, clock skew, partition failures or credential work.
  Refill-batch cancellation stops queued constructors; a handshake already
  admitted by DSQL completes before its result is handled. Close/destruction
  has zero network latency; the 500ms health recheck delay remains.
- Reconnects are serialized with one second of rebuild bookkeeping, plus the
  regular reservoir fill window. They model the amplification of #31, not exact
  Temporal retry/backoff scheduling. No retry of a failed workload query is
  hidden from the failure counters.
- The existing sequential refiller's scheduled failure retry can be overtaken
  by a checkout/scan-triggered refill. The real deployed loop sleeps through
  its backoff. This simplification is retained for `as-deployed`; Option A's
  failed refill slots enforce their individual backoffs.
- Reservoir lifetimes keep the existing simulator's **base + [0,jitter)**
  convention. The design document also describes symmetric jitter elsewhere;
  this simulator does not silently change its established convention.
- The workload is a capped burst-arrival generator, not a Temporal workflow
  execution model. Request latency covers acquisition only. Acquires still
  waiting at the run horizon are right-censored; active queries may finish
  beyond the horizon. Thus requests need not equal completed plus failed.

## Comparison scenarios and metrics

All the comparison scenarios run for three hours with ten-second CSV samples.
The original light-load scenarios take roughly 1–20 seconds for all three
variants on the provided machine. Peak workloads have substantially more events;
the 44-instance scenarios target at most about three minutes per comparison.

| File | Topology and event |
|---|---|
| `compare-400wps.yaml` | 16 history + 16 matching + 9 frontend + 3 worker instances; cold start. Their 22,000 connections exceed the 10,000 cap |
| `compare-400wps-cap25k.yaml` | The same, with the cap raised to 25,000 so every pool reaches its target |
| `compare-400wps-200.yaml` | The same, with 200 connections per pool so the fleet fits the default cap |
| `compare-fleet-start.yaml` | Four history instances start uniformly within 2s |
| `compare-rolling-deploy.yaml` | Replace four history instances once each, 30s apart, starting at 70m |
| `compare-mass-drop.yaml` | Drop a seeded random 50% of all physical connections at 70m, including active ones |
| `compare-ddb-outage.yaml` | Switch from global DDB bucket to local AIMD at 5s during startup |
| `compare-400wps-peak.yaml` | 25,000-cap topology with synchronized 40–45m ramp, 45–75m peak, and step spikes at 100–103m and 150–153m |
| `compare-400wps-peak-200.yaml` | 200-connection topology with every base and phase `maxInUse` scaled by 0.4 |
| `compare-400wps-peak-warm50.yaml` | Same 500-connection peak profile, but `targetReady=minConns=50`; `maxOpen=maxConns=500` |
| `compare-400wps-peak-noguard.yaml` | `compare-400wps-peak.yaml` with pgxpool's proposed acquire guard off; only the pgxpool column differs |
| `compare-fleet-start-peak.yaml` | Four history replicas start into demand 450 from simulation time 0 to 30m |
| `compare-rolling-deploy-peak.yaml` | History ramps from 60–65m, holds 450 until 95m; replacements at 70m occur inside the peak |
| `compare-mass-drop-peak.yaml` | Same 60–95m history phase, with the 70m mass drop during the plateau |
| `compare-ddb-outage-peak.yaml` | History ramps from 40–45m, holds 450 until 75m; the outage from 5s never ends |

The 500-connection fleet's history/matching/frontend peak levels are 450/300/100;
spikes are 600/450/150. Workers stay at base demand. Every phase ticks at 100ms
with `arrivalBurst=maxInUse`. The peak spans pgxpool's first lifetime expiry
wave (roughly 50–58m including warmup) and several reservoir lifetimes. Spikes
fall in later expiry waves and put history 20% over its pool. The small warm
buffer scenario uses the reservoir design's production recommendation of 50
ready connections for both models. The 25,000 cap fits the 22,000 warm target,
but reservoir buffers plus SQL-owned connections can exceed it under peak load.
The already over-cap `compare-400wps.yaml` has no peak derivative.

Phase holds are deliberately stretched from real **5–50ms** holds to
`500ms + [0,1s)` for history/matching and `250ms + [0,500ms)` for frontend.
This preserves the intended concurrency while reducing requests and simulation
events. Queue waits scale with hold time: **read phase wait percentiles as
relative comparisons between models, not production latencies**. The generator
retains its existing nanosecond-inclusive jitter endpoint. These scenarios
measure pool pressure, not actual WPS or database query throughput.

`cold_start`, `rolling_restart` and `mass_drop` work with every model.
`rolling_restart` repeats replacements until the horizon; `rolling_deploy`
replaces the fleet once. Each replacement closes old pools with the selected
leak policy and starts a new instance including ephemeral pools. `eventAt`
defaults to 5m and `rollEvery` to 1m. `fleet_start` uses `startWindow` (zero means
simultaneous). `mass_drop` uses the existing `dropAt`/`dropFraction` settings.
`ddb_outage` switches queued and subsequent acquisitions to adaptive without
duplicating them; its default initial limiter is `tokenBucket`.

The large file uses the **topology** of `ecs-400wps.yaml`; that original file
has no enabled workload. Per-service bursts/holds come from
`workload-test.yaml`. Its 100ms arrival ticks are coarsened to 1s, leaving hold
times and burst concurrency unchanged: 425 sampled queries/s across the fleet,
not 400 literal workflow executions/s. This reduces workload events tenfold.
There is no extrapolation of counts or percentiles. Connection lifetimes,
handshakes, cuts, limiter timing and query completions retain nanosecond event
times. All variants use equal warm targets (`MinConns=targetReady=500`), so the
44-instance target of 22,000 deliberately exceeds the default cap of 10,000.
No model can meet every warm target; the experiment compares failure and
recovery behavior under that contention, not capacity sizing advice.

The table reports all workload requests, including startup checks: p50, p99,
p99.9 and maximum acquire wait; requests waiting on a constructor (including background warmup);
failures split into empty reservoir, timeout, dead connection, server connect
error and pool closed; successful opens/s and dial attempt peaks; both DSQL
rejections; main/ephemeral physical peaks; fleet targets; cluster peak/cap;
idle/in-use age cuts; lifetime churn and rebuilds. Zero wait is exact; positive
percentiles round up to microseconds. The maximum retains full precision
internally. The new-connection counter counts each request once if it invokes a constructor
or queues while constructors occupy pool slots, including requests that time
out before construction completes.

`Workload start delay p50 / max` records pool start to first spawned request for
each main pool generation that issues work, including rebuilds and replacements.
Ephemeral pools and main pools closed before their first request are excluded.
Reservoir initial fill can defer workload up to 30s; pgxpool starts immediately.
This exposes the startup barrier behind otherwise lower reservoir request counts.

The generator is closed-loop: a request that fails at once frees its slot, and
the next tick (every 100ms in a phase) issues a new request in its place. A
model that rejects fast, such as a reservoir that's empty, records up to ten
failures a second for each slot, while one that queues records at most one per
`maxWait`. So compare completed requests (requests minus failed) between
models, not failure counts.

Each phase name then gets four rows, aggregated across services in configuration
order of first appearance: `<phase>: requests / failed`,
`<phase>: failed empty / timeout / dead`, `<phase>: acquire p99 / p99.9 / max`,
and `<phase>: waited on new connection`. Requests keep the tag from spawn time,
including during a ramp and when acquisition/completion crosses the phase end.
Base and ephemeral requests have no tag. Phase failed totals include every
failure reason, including connect errors and closed pools. Phases with no
requests still appear with zeros. The CSV columns are unchanged; these new
breakdowns appear only in the summary.

Rates use fixed simulated one-second bins. Steady averages include every
second after `steadyAfter` (default half the run), including zero-open seconds.
Lifetime churn counts successful opens that replace prior lifetime/guard
retirements in that pool after `steadyAfter`, divided by steady seconds and the
100/s budget. Cap failures, initial fill, mass-drop replacements and rebuilds
are not counted as lifetime churn. A replacement pays an outstanding lifetime
retirement first when several reasons for demand coexist.

Each CSV has one row per pool generation per sample: ready/idle/in-use,
constructing/physical/peak, target/limit, shared cluster physical/peak/cap,
requests/completions/waited-new, failure classes, opens/rejections/cuts/churn,
maximum acquire wait, instance peak in-flight and local adaptive rate.
Closed pools retain rows while leaking, then receive a final row; short-lived
pools therefore still expose their peaks even between samples. Cluster gauges
repeat across rows and **must not be summed**. Table pool peaks are maxima over
individual pool generations; each pool's exact peak is in its CSV.

The existing rate/convergence/stability/empty assertions remain available.
The comparison engine also enforces `assertMaxClosurePerSec` and supports
`assertZeroDSQLRejections` and `assertZeroDeadRequests`. Set a numeric rate
assertion to `-1` or convergence to `-1s` to disable it. Comparison files disable
legacy fixed-rate/convergence checks explicitly because token buckets allow
bursts and the large fleet cannot reach every target. Failure counters are
still reported; assertions are opt-in, never inferred from a model name.

```bash
for name in 400wps fleet-start rolling-deploy mass-drop ddb-outage; do
  go run ./tools/poolsim -config tools/poolsim/scenarios/compare-$name.yaml \
    -compare as-deployed,option-a,pgxpool -out /tmp/$name.csv
done
go test -race ./tools/poolsim/...
go vet ./tools/poolsim/...
```

The legacy engine below is intentionally unchanged. It historically seeds its
refiller from wall-clock time and query jitter from the global RNG rather than
`seed`; its jitter-sensitive CSV counters are therefore not reproducible even
between two runs of the old binary. The comparison engine does not inherit that
limitation. Existing CSV fields and reservoir/refiller/workload behavior remain
available for prior consumers.

## Legacy reservoir engine

A discrete event simulator for validating DSQL connection reservoir behavior under various conditions. This simulator models the actual reservoir implementation in `temporal-dsql`:

- **Reservoir**: Channel-based connection buffer with fast checkout
- **Continuous Refiller**: Back-to-back connection creation with rate limiter as ONLY throttle
- **Proactive Expiry Scanner**: Background goroutine evicting connections before they expire
- **Guard Window**: Connections discarded if remaining lifetime < guard window
- **Rate Limiting**: DSQL's cluster-wide 100 connections/second limit
- **Workload Simulation**: Models database queries consuming connections

## Core Design Principles

| Principle | Description |
|-----------|-------------|
| **Fast Checkout** | Sub-millisecond checkout from channel buffer (hot path) |
| **Continuous Refill** | Rate limiter is the ONLY throttle - no artificial delays |
| **Proactive Expiry** | Background scanner evicts connections before they expire |
| **Eviction Callback** | Connections release lease on discard (not modeled in sim) |

## Quick Start

```bash
# Run with default config
go run ./tools/poolsim

# Run with custom config
go run ./tools/poolsim -config tools/poolsim/scenarios/ecs-400wps.yaml

# Output to CSV for analysis
go run ./tools/poolsim -config tools/poolsim/scenarios/local.yaml -out results.csv
```

## How It Works

The simulator uses discrete event simulation - simulated time advances instantly between events, so a 20-minute simulation completes in seconds.

### Components Modeled

1. **Reservoir** (`reservoir.go` behavior)
   - Channel-based buffer holding ready connections
   - FIFO checkout with guard window validation
   - Connections discarded if expired or within guard window
   - Tracks checkout latency, empty events, discard reasons

2. **Continuous Refiller** (`reservoir_refiller.go` behavior)
   - Runs back-to-back `openOne()` calls
   - Rate limiter is the ONLY throttle - no artificial delays
   - Brief idle check (100ms) when at target capacity
   - Backoff (250ms) only on connection creation failure

3. **Expiry Scanner** (second background goroutine)
   - Scans reservoir every 1 second
   - Evicts connections within guard window
   - Eager eviction for clustered expiry times
   - Creates refill demand for continuous refiller

4. **Connection Lifecycle**
   - `BaseLifetime = 11 minutes`: Base connection lifetime
   - `Jitter = 2 minutes`: Random jitter added to each connection
   - `GuardWindow = 45 seconds`: Discard if remaining < this
   - Connections expire individually based on creation time + jitter

5. **Rate Limiter**
   - Models DSQL's 100 connections/second cluster-wide limit
   - All services share the same rate limit budget

6. **Workload Simulation**
   - Models database queries consuming connections
   - Configurable concurrency target (`maxInUse`)
   - Connection hold time with jitter
   - Tracks empty events when reservoir has no connections

## Configuration

```yaml
seed: 42                          # Random seed for reproducibility
endpoint: cluster.dsql.on.aws     # DSQL endpoint (for rate limiter grouping)

duration: 20m                     # Simulation duration
sampleEvery: 1s                   # Metrics sampling interval

limiterPerSec: 100                # DSQL cluster rate limit

# Assertions (checked at end)
assertMaxConnectsPerSec: 100      # Must not exceed rate limit
assertConvergeWithin: 60s         # All reservoirs reach target within this
assertStableFor: 5s               # Reservoirs must stay at target for this duration
assertZeroEmptyEvents: true       # No checkout attempts should find empty reservoir

scenario:
  kind: cold_start                # cold_start | rolling_restart | mass_drop

services:
  - name: history
    targetReady: 50               # Target reservoir size
    guardWindow: 45s              # Discard if remaining lifetime < this
    baseLifetime: 11m             # Base connection lifetime
    jitter: 2m                    # Lifetime jitter (0 to jitter added)
    
    startDelay: 0s                # Delay before service starts
    
    # Workload simulation (optional)
    workload:
      enabled: true               # Enable workload simulation
      maxInUse: 20                # Target concurrent connections in use
      holdTime: 10ms              # How long each query holds a connection
      holdJitter: 5ms             # Random jitter added to hold time
      spawnEvery: 200ms           # How often to spawn new work
      arrivalBurst: 5             # Max work items per spawn tick
      maxWait: 10s                # Max wait for connection before giving up
```

## Scenarios

### cold_start
All services start from zero connections. Tests:
- Reservoir fill time
- Rate limit compliance during startup
- Connection age distribution with jitter

### rolling_restart
Services restart one-by-one. Tests:
- Recovery time per service
- Rate limit contention during recovery
- Impact on other services

### mass_drop
Sudden connection loss (network event). Tests:
- Recovery from partial reservoir loss
- Rate limit contention during mass recovery

## Output

CSV output includes per-second samples:

| Column | Description |
|--------|-------------|
| `t_rfc3339` | Timestamp |
| `service` | Service name |
| `size` | Current connections in reservoir |
| `target` | Target reservoir size |
| `in_use` | Connections currently checked out |
| `checkouts` | Total successful checkouts |
| `empty_events` | Checkout attempts when reservoir empty |
| `discards` | Total connections discarded |
| `discard_expired` | Discards due to expiry |
| `discard_guard` | Discards due to guard window |
| `discard_full` | Discards due to reservoir full |
| `refills` | Connections created by refiller |
| `refill_failures` | Failed refill attempts |
| `scan_evictions` | Connections evicted by expiry scanner |
| `limiter_denies` | Rate limit denials |
| `work_started` | Work items started |
| `work_completed` | Work items completed |

## Assertions

The simulator validates behavior with configurable assertions:

| Assertion | Description |
|-----------|-------------|
| `assertMaxConnectsPerSec` | Global connection rate must not exceed this (default: 100) |
| `assertConvergeWithin` | All reservoirs must reach target within this duration |
| `assertStableFor` | After reaching target, reservoirs must stay stable for this duration |
| `assertZeroEmptyEvents` | No checkout attempts should find an empty reservoir |

The `assertZeroEmptyEvents` assertion is critical - if the reservoir ever runs empty during checkout, it means the refiller isn't keeping up with demand.

## Comparison with Production

| Aspect | Simulator | Production |
|--------|-----------|------------|
| Reservoir | Slice-based buffer | Channel-based buffer |
| Refiller | Simulated continuous loop | Goroutine with rate limiter |
| Expiry Scanner | Simulated periodic scan | Goroutine every 1 second |
| Rate limiting | Simulated per-second | Driver-level blocking |
| Time | Discrete events | Real time |

The simulator validates the algorithm, not the exact implementation. Use it to:
- Validate configuration before deployment
- Test edge cases (mass drops, rolling restarts)
- Prove convergence within time bounds
- Verify zero empty events under workload
- Analyze discard patterns (expired vs guard vs full)

## Example Output

```
Running reservoir simulation for 20m0s...

=== Reservoir Simulation Summary ===
Duration: 20m0s
Services: 4
Rate limit: 100/sec

Service: history
  Final size: 50 / 50
  Checkouts: 0
  Empty events: 0
  Refills: 100
  Discards: 50 (expired=0, guard=50, full=0)

Max connects/sec: 100 (limit: 100)
Total empty events: 0
OK
```

The output shows:
- All services reach their target of 50 connections
- Zero empty events (reservoir always has connections available)
- ~100 refills per service over 20 minutes (initial 50 + ~50 replacements)
- 50 discards per service due to guard window eviction (proactive expiry working)
