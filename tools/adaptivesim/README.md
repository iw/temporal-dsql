# adaptivesim

`adaptivesim` is a discrete-event simulator of the Temporal service's adaptive plane. It
runs the signal plane, the adaptive controllers and their actuators closed-loop on
simulated time, over a coarse model of history, its workers, the DSQL plugin's pool and
DSQL itself. It's written in Go so that it runs the real code where that matters: the
signal plane (`common/signals`), the adaptive controllers (`common/adaptive/...`) and the
pool gate's admission policy (`common/persistence/sql/sqlplugin/dsql/gate`). The service,
its connections and its store are models, kept coarse, since the simulator's job is the
whole loop.

[Offline replay](../../docs/dsql/adaptive-behaviour.md#offline-replay) runs the laws
open-loop on recorded signals, so a proposal doesn't change the signals that follow.
`adaptivesim` closes that loop: the controller's decisions change the simulated limits, and
the simulated service answers. The [controllers](#controllers) say which are wired.

## Run

From the repository root:

```bash
go run ./cmd/tools/adaptivesim -list
go run ./cmd/tools/adaptivesim -scenario short-pool-overload -variant base
go run ./cmd/tools/adaptivesim -scenario startup -variant no-grace -trace /tmp/adaptivesim
go run ./cmd/tools/adaptivereplay /tmp/adaptivesim/*.jsonl
```

- `-scenario name` runs a scenario, and `-variant name` one of its variants, its first by
  default. `-list` lists them.
- `-seed n` runs with another random seed. A run with the same scenario and seed gives
  the same result.
- `-capacity mode` runs the capacity controller `disabled`, in `shadow` or `enabled`, and
  `-queued-grace duration` sets its `QueuedGrace`. `-task-rate mode`, `-workers mode` and
  `-memory mode` do the same for the task scheduler rate, the worker counts and the
  memory budget.
- `-trace dir` writes each host's signal plane trace to the directory, in the signal
  plane's format and with its file names, such as `history-history-1-1.jsonl`, for
  `tools/adaptivereplay`. Write traces outside the repository.

The report gives, for each host, its tasks and how many completed, its persistence calls,
the gate's refusals and the queries cut off at their deadlines, and its pool's connections
and acquires. For a host whose heap the model counts, it gives the heap's peak, how long the
heap pressed on its limit and passed it, the tasks the queues held and left in the store,
and the workflow cache's hits and size. Then come the cluster's connection attempts: the pools' dials and pgx's
cancel requests, the connections opened, and DSQL's rejections. Last come the controllers'
decisions, a row for each controller on each host: its mode, how many decisions it made,
its first change, the range of its proposals, and its reasons.

`go test ./tools/adaptivesim` runs the scenarios below as seeded tests, in a couple of
seconds, with no AWS.

## Scenarios

| Scenario | Variants | What it runs |
|---|---|---|
| `short-pool-burst` | `pipeline-gate`, `base`, `gate`, `pipeline` | Finding 7 of the [appreciation](../../docs/dsql/adaptive-appreciation.md#short-pool), as the local rig modeled it: one history host with 4 connections and 22 ms to the database, 200 reloaded tasks at the start, then 9 tasks/s for 150 s and 90 s to drain |
| `short-pool-overload` | as above | The same host at 24 tasks/s, about 130% of what 4 connections serve |
| `startup` | `grace`, `no-grace` | Finding 1: four hosts of 64 connections start within 10 s, and each reloads 100 tasks as it takes its shards, with the capacity controller enabled. `no-grace` sets `QueuedGrace` to 0, as the appreciation ran |
| `fleet-start` | | `tools/poolsim`'s `pgxpool-fleet-start.yaml`, for the [cross-check](#cross-check-with-poolsim) |
| `workflows` | | Two hosts run 10 workflows/s each through 32 pollers a host, on the plugin's current path |
| `slow-store` | `cap`, `cap-tsr`, `cap-wrk` | The appreciation's slow store: one host of 48 connections, whose static persistence rate is 300/s, runs 80 tasks/s and 10 workflows/s for 240 s, and the store's replies come 40 ms later from 60 s to 150 s. `cap` enables the capacity controller with the task scheduler rate in shadow; `cap-tsr` enables both; `cap-wrk` enables the capacity controller and the worker counts |
| `surge` | `tsr`, `shadow` | One host whose static persistence rate is 100/s runs 50 tasks/s, 120/s from 60 s to 120 s, and 5 workflows/s, with the capacity controller in shadow and the task scheduler rate enabled or in shadow |
| `busy-queue` | `wrk`, `shadow` | One host whose queues have 8 workers each, so that the workers bind first, runs 60 tasks/s for 90 s, then 5 tasks/s until 540 s, with the worker counts enabled or in shadow |
| `memory-pressure` | `mem`, `shadow` | The appreciation's 512 MiB `GOMEMLIMIT`, with history's cache at its 1000 MiB default and each shard's queues limited to 10,000 pending tasks, on one host in the cluster's region: 5 workflows/s and 20 tasks/s for 600 s, and from 120 s, 224 MiB more heap for 120 s while 60,000 tasks reload, with the memory budget enabled or in shadow |

The short pool's variants are the rig's: `base` is the plugin's path before the gate, with
the common store reading a workflow in nine queries; `gate` adds the gate; `pipeline` reads
a workflow in one pipelined batch; `pipeline-gate` is both, the plugin's current path.

## What it models

- **The loop.** Events run in time order, and in the order they were scheduled at the same
  time. Each part of the model draws from its own random stream, from the seed and the
  part's name, so a change to one part doesn't move another's draws.
- **DSQL.** Admission of 100 new connections a second, with a burst of 1,000; the
  cluster's connection limit, 10,000; rejections for the rate (53400) and the limit (53300),
  which cost a connect as an admitted connection does; and the 60-minute cutoff. A connect
  costs a time drawn from a range.
- **The plugin's pacing.** Each process's connects take one of its 8 attempt slots while
  they wait for the connection limiter, and give it back before they dial. The limiter is
  the process's own, 10 a second with a burst of 100, or the DynamoDB token bucket that
  every process shares, 100 a second with a burst of 1,000, each reservation taking 5–10 ms.
- **pgxpool**, as puddle runs it: a semaphore of `MaxConns` units, which acquired
  connections and constructions hold, waited for first in, first out; idle connections
  reused last in, first out, with a ping after a second idle; construction on demand,
  which goes on after its caller gives up; the minimum fill in the background; lifetimes
  with whole seconds of jitter, retired on release and by the health checks, which run
  every minute and 500 ms after a destroyed connection. Its acquire, empty acquire and
  canceled acquire counts are pgxpool's.
- **The gate.** The plugin's own `gate.Policy`, on simulated time. A caller waits for its
  grant or for its give-up time.
- **Statements.** Each round trip takes the round trip plus the store's time, spread by
  10% either way. A slow store holds back each reply by a further delay, as the
  appreciation's proxy did. A statement still running at its deadline is cut off: pgx closes the
  connection and dials a cancel request on a connection of its own (`pgconn.asyncClose`),
  which the plugin's pacing doesn't see. The cancel request and the pool's replacement
  connection both count against DSQL's admission.
- **The store.** database/sql keeps no idle connections, so each statement outside a
  transaction, and each transaction, takes a connection from the connector. A workflow
  read is the common store's nine queries, each on a connection of its own, or the
  plugin's pipelined read on one. A write is the history append, then a transaction of 7
  round trips; both are writes at the gate.
- **Errors**, as the code maps them. A gate refusal, or a connection DSQL rejected, reports
  a throttled fact. The transaction and the pipelined read return it as
  `ResourceExhausted`; the common store's read and the history append flatten it to
  `Unavailable`. A query cut off, or an acquire that runs out of time, is `Unavailable`.
- **Store facts.** Every second, the plugin reports the pool's acquires as acquired facts,
  and its empty acquires and the gate's waits as queued facts.
- **The persistence client.** Each call passes the persistence rate limiter, and the
  health signal aggregator records each attempt. A call that returns `Unavailable` is
  retried once, 50 ms later.
- **Health signals**, as `healthSignalAggregatorImpl.RecentSnapshot` reads them: calls,
  unhealthy errors, rejections, facts and task attempts counted in fixed windows, and
  latencies in windows that start at their first sample. The real aggregator reads the
  wall clock, so the simulator keeps its own on simulated time, with exact quantiles where
  the real one uses t-digests.
- **The capacity controller.** The real controller, stepped from the calls every
  `StepInterval` as `capacityAwareRateLimiter` steps it, with the envelope that dynamic
  config gives it. Enabled, its rate sets the limiter's rate and burst. Its decisions carry
  the detail the real limiter records.
- **History.** Two task queues, transfer and timer, each with a scheduler of 512 workers,
  which take its tasks first in, first out; the model's tasks are transfer tasks. 16
  shards, each writing one task at a time (`history.shardIOConcurrency` = 1); a 3 s
  deadline for each attempt, covering its read and its write; a cache of the workflows'
  state, which a failed attempt clears. A task that fails with `Unavailable` is
  resubmitted at once until its tenth attempt, then after 1 s × 1.1ⁿ; one that fails with
  `ResourceExhausted` is resubmitted at once the first time, then after 3 s × 1.5ⁿ.
- **The workflow cache.** Least recently used out. Where a scenario limits it by size, as
  `history.cacheSizeBasedLimit` does, each workflow's state takes the same size, and the
  cache evicts past its limit; otherwise it keeps every workflow.
- **The queues' readers.** Where a scenario sets a pending-task limit, each shard's queue
  loads the tasks the shard writes while it holds fewer than the limit, and leaves the rest
  in the store, first in, first out, until its tasks complete. A task stays loaded from
  its load to its completion, through its retries.
- **The heap**, as the runtime reports it against `GOMEMLIMIT`: the rest of the process, the
  workflow cache and the loaded tasks, each a fixed size, and a scenario's extra heap for a
  while. The heap goal is twice the live heap, at the default `GOGC`, held under the
  limit, so the heap's pressure on the limit is the live heap's share of the goal.
- **The scheduler's rate limiter**, where a scenario turns it on, as
  `history.taskSchedulerEnableRateLimiter` does: its own limit is the static persistence
  rate, with a burst of twice that, and the task scheduler rate holds tasks to its own
  rate on top while it's enabled. A task the limiter refuses waits in its queue's
  rescheduler for 1 s × 1.1ⁿ, then goes back to the scheduler in time order; when the
  scheduler refuses it again, the rescheduler tries again 1–3 s later.
- **Workers.** A fixed number of pollers, 32 a host. A workflow is a start, then a few
  tasks, each held by a poller until history has written the worker's response.
- **The signal plane.** The real `signals.Plane`, ticked every second on simulated time.
  It reads the host's signals through a `signals.Source`, takes a snapshot over the newest
  3 windows, steps the controllers that `signals.Drive` subscribes to it, and writes its
  trace with the service's trace writer. Every controller's decisions go through it.
- **The task scheduler rate.** The real controller, driven by `taskrate.DriveRate` as the
  service drives it, steps on each snapshot: the capacity controller's rate while that's
  enabled and recent, otherwise the static rate, times attempts per call.
- **The worker counts.** The real controllers, one a queue, from
  `workers.NewHistoryQueues` as history wires them. Each sizes its queue's pool from the
  queue's p90 delay, which the plane reads over 1 s windows as its queue recorders keep
  it, with exact quantiles. A pool that shrinks stops each worker over the new count once
  its task ends.
- **The memory budget.** The real controllers, as history wires them: `memory.DriveCache`
  resizes the workflow cache, and `workers.HistoryQueues.PendingTaskLimits` scales each
  queue's pending-task limit by one multiplier, from the heap's signals in each snapshot.

## What it doesn't model

- **The store's capacity.** A statement's time at the store is fixed, whatever the load:
  no queueing at DSQL, no transaction conflicts (40001), no transaction size limits.
- **Priorities.** The persistence rate limiter is one bucket for every priority. While the
  controller is off or in shadow, it's the static rate; the existing per-priority health
  limiters aren't modeled. Freezing isn't simulated.
- **The rest of the service.** Only history's execution store is modeled: no frontend,
  matching, visibility or history events reads, and a workflow is a coarse chain of tasks.
- **Connections in detail.** No TLS or IAM, and no ping failures. A connection DSQL cuts
  off while idle leaves the pool at once, rather than at its next ping. poolsim models more
  of pgxpool and of deployments.
- **The host.** Snapshots carry no RPC signals, and their runtime and memory signals are
  the heap model's: the heap goal moves at once with the live heap, collections cost
  nothing, and a heap over its limit slows nothing. There's no cgroup limit.
- **The rest of history's memory.** No events cache, no cache TTL, and no mitigator that
  unloads tasks past a queue's critical count: a smaller limit stops the reader loading,
  and the tasks already loaded stay.
- **The other controllers.** See [Controllers](#controllers).

## Calibration against the local rig

The short pool scenarios model the local rig of finding 7: the plugin's pool against
PostgreSQL behind a proxy that added 22 ms to each round trip and 100 ms to each new
connection. The rig's uncontended reads took 240 ms with nine queries and 27 ms pipelined,
and a write 219 ms; the model's statements take 22 ms plus 5 ms at the store. With seed 1:

| Burst | Rig: completed of 1,549 | Rig: burst cleared | adaptivesim: completed of 1,550 | adaptivesim: burst cleared |
|---|---|---|---|---|
| `base` | 2 | never | 11 | never |
| `pipeline` | 19 | never | 141 | never |
| `gate` | 1,549 | 226 s | 1,550 | 185 s |
| `pipeline-gate` | 1,549 | 30 s | 1,550 | 27 s |

| Overload | Rig: completed of 3,599 | adaptivesim: completed of 3,600 | Rig: connections opened | adaptivesim: connections opened, after the first 4 |
|---|---|---|---|---|
| `base` | 244 | 219 | 928 | 764 |
| `pipeline` | 277 | 219 | 1,854 | 844 |
| `gate` | 2,764 | 2,718 | 11 | 0 |
| `pipeline-gate` | 3,547 and 3,504 | 3,574 | 50 | 0 |

In the burst, `base` made 11.7 connection attempts a second, half of them cancel requests.
Run against DSQL, the rig's `base` made 12–13 a second, also half of them cancel requests.
The gate's variants made fewer than 0.1 a second.

The rig's model returned a refusal as `ResourceExhausted`. The plugin's common store
returns a refused read, and a refused history append, as `Unavailable`, which history tries
again at once, and the simulator follows the code. That's why the simulator's `gate` tries
many more reads than the rig's did. The pipelined read keeps `ResourceExhausted`, and there
the two agree.

The tests assert orderings and rough ratios, not these counts: `base` completes fewer than
5% of the burst and 10% of the overload; the gate with the pipelined read completes all of
the burst, clearing it within a minute, and more than 95% of the overload; the gate alone
falls between. In the burst, `base` makes more than 5 connection attempts a second, about
half of them cancel requests, and the gate's variants fewer than one; over the overload and
its drain, `base` makes more than 50 times as many as the gate with the pipelined read.

## Cross-check with poolsim

`tools/poolsim` and `adaptivesim` model connections separately; `adaptivesim` doesn't
import poolsim. The `fleet-start` scenario is poolsim's `pgxpool-fleet-start.yaml`: four
hosts of 500 connections start within 2 s on the shared token bucket, each with two
startup pools of 3 connections for 1 s, and a light load of bare statements, for three
hours.

```bash
go run ./tools/poolsim -config tools/poolsim/scenarios/pgxpool-fleet-start.yaml -out /tmp/fleet-start.csv
go run ./cmd/tools/adaptivesim -scenario fleet-start
```

On 4 November 2026:

| | poolsim | adaptivesim |
|---|---|---|
| Connections the service pools opened | 8,000, 2,000 a pool (CSV `opens`) | 8,000, 2,000 a host |
| Connections the startup pools opened | 24 | 24 |
| Opens a second, steady | 0.74, after 90 minutes | 0.74, over the load's second half |
| Rejections, 53400 / 53300 | 1 / 0 | 1 / 0 |
| Connections cut at 60 minutes | 0 | 0 |
| Most connections open | 2,000 | 2,005, with the startup pools |
| Service pool acquires that waited for a connection | 40, 10 a pool (CSV `waited_new`) | 40, 10 a host |
| Acquire wait p99 / max | 0 / 206 ms | 0 / 211 ms |
| Opens in the busiest second | 256 | 1,059 |
| Dial attempts in the busiest second | 253 | 952 |

The tolerance: connections opened, rejections, cuts and waits within 1% or ±2, and steady
rates within 10%. Here they're equal. The busiest second differs by design: poolsim holds a
process's attempt slot through the handshake, so 8 slots on each of 4 hosts, at about
125 ms a handshake, bound it near 256 a second. The plugin frees its slot before the dial,
and so does `adaptivesim`, so the shared bucket's burst of 1,000 bounds it instead. DSQL's
own burst of 1,000 admits both.

## Relation to the other simulators

- **`tools/adaptivereplay`** replays recorded traces through the laws, open-loop, for what
  other parameters would have proposed on the same signals. `adaptivesim` writes the same
  trace format, so its runs replay too; its tests check that every controller's steps
  replay to the proposals and reasons they recorded.
- **`tools/poolsim`** stays dedicated to connection management: pgxpool, DSQL's admission
  and pacing strategies at fleet scale, deployments and outages. `adaptivesim` keeps a
  smaller connection model of its own, checked against poolsim's above.
- **iw/tempdes** simulates upstream Temporal 1.31.0, in Rust, and stays free of DSQL and of
  the controllers.

## Controllers

Each controller is an actuator on the simulated service: it reads the plane's snapshots, or
for capacity the calls, and sets one of the model's knobs.

| Controller | Package | What it drives in the model | Status |
|---|---|---|---|
| Persistence capacity | `common/adaptive/capacity` | The persistence rate limiter's rate | Wired |
| Task scheduler rate | `common/adaptive/taskrate` | The scheduler rate limiter's rate on history's task attempts | Wired |
| Worker counts | `common/adaptive/workers` | Each queue's scheduler workers, from `History.Workers` | Wired |
| Memory budget | `common/adaptive/memory` | The workflow cache's size limit, from `History.CacheMaxBytes`, and each shard's queues' pending-task limits, from `History.MaxPendingTasks` | Wired |
| Partition scaler | `common/adaptive/partitions` | Matching's partitions; needs a model of matching | Not yet |
| Fair share | `common/adaptive/fairshare` | Frontends' namespace shares; needs frontends | Not yet |

The capacity controller steps from the calls, as its limiter does. The others are driven by
`signals.Drive` or `signals.DriveFunc`, which step a `signals.Controller` on each of the
plane's snapshots, as they do in the service, and report each decision to the plane, which
writes it to the trace. A host's dynamic config is a static collection of the scenario's
controller settings.

## What the runs show

- **Through a surge, the task scheduler rate keeps the tasks in the scheduler.** In
  `surge`, with the rate enabled, the persistence rate limiter refused 20 calls, the
  workflows' p99 was 982 ms, and the tasks made 16,216 attempts. In shadow, it refused
  7,234, the workflows' p99 was 1m42s, and the tasks made 22,094 attempts. The backlog
  cleared in both.
- **The task scheduler rate follows each capacity cut, and with both enabled the cut
  stays.** In `slow-store`, the capacity controller cuts from 300/s to about 80/s while the
  store is slow, and the task scheduler rate follows within a few seconds. With the task rate in
  shadow (`cap`), tasks fail on the persistence limit until the store recovers, the limit
  binds, and capacity probes back up: every task completes. With both enabled
  (`cap-tsr`), the task scheduler rate keeps persistence's calls at its `Headroom`, 90% of
  capacity's rate, so the persistence limit doesn't bind once the store recovers. Capacity
  holds its cut as `idle` for the rest of the load, the task rate stays at about 42/s
  against 80 tasks arriving a second, and 6,648 tasks are still waiting when the load
  ends. A rate that isn't binding says nothing about the store, which is why `idle` holds;
  here the task rate is what keeps it from binding. In the appreciation, the task rate
  stayed far above the tasks' rate except in the short pool, so this didn't show there.
- **A busy queue's worker count grows, then shrinks.** In `busy-queue`, the transfer
  queue's count grows from 8 to 16 by 11 s, which serves the 60 tasks a second: the tasks'
  p99 was 1.8 s, against 55 s with the count in shadow. At 16 workers its tasks wait
  under a quarter of the target, which counts as idle, so it shrinks a worker every 30 s
  from 48 s on, while still busy, and reaches 4 at 6m18s. The idle timer queue reaches 4
  sooner.
- **The memory budget holds the heap under its limit, and gives way to the rest of the
  process.** In `memory-pressure`, the budget takes the cache from 1000 MiB to its share of
  the limit, 153.6 MiB or 0.15 of its setting as in the appreciation, and the pending tasks
  to 0.164 of their limits, a quarter at a time in the first 7 s. At 2:00 the extra heap
  and the reload take the live heap to 0.93 of its goal, so both shrink: the cache to
  115.2 MiB, and the pending tasks to their floor of 0.139, where each queue's critical
  count is 1,250. At 3:00 the cache shrinks again, to 86.4 MiB. Once the extra heap goes at
  4:00, both grow back 10% a minute, the pending tasks to their budget by 5:00 and the
  cache by 10:00. The heap peaked at 479 MiB, and the queues held at most 22,224 tasks. In
  shadow, the cache grows to its 1000 MiB setting: the heap peaked at 1,414 MiB and passed
  the limit for 8m30s, and the queues held 53,633 tasks. Both cleared the reload at 3m32s
  and completed every task, and the smaller cache made 0.8% more reads.
- **The worker counts back off while persistence does.** In `slow-store`'s `cap-wrk`, the
  counts shrink from 512 every 30 s until the store slows at 60 s, then hold as `backoff`
  after each capacity cut and while calls are throttled or rejected, until 3m39s. They
  shrink again from 4m9s. At 512 workers the pools never bind, as in the appreciation.
