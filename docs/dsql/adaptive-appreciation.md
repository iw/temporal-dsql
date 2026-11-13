# Adaptive behaviour: the system appreciation

*Run on 3 November 2026, once every phase was built and off by default. It tests the signals and actuators together, and takes the place of the roadmap's gate after each phase in [Phasing and roadmap](adaptive-behaviour.md#phasing-and-roadmap).*

## Summary

- 40 runs in seven scenarios on a throwaway DSQL cluster, with every controller disabled, in shadow, enabled one at a time, and all enabled together.
- Outside the short pool, enabling the controllers, alone or together, changed no scenario's goodput, latency or errors beyond the runs' noise, with one exception. Fair share lifted the admitted requests of a namespace whose load reached one frontend more than the other, from 48 to 63–64 a second against its limit of 60.
- The other controllers mostly held still, and rightly: the limits they own didn't bind. What bound goodput in the ramp and the slow store was history's writes to each shard, one at a time, and the worker's pollers waiting on them. No controller reads those.
- With 4 connections a host, history fell behind as it started and never caught up, in every config. The capacity controller cut to its floor, which was still above what the pool could serve, so when enabled its limit refused calls without restoring progress: about 54,000 in a run, or 2,700 with the task scheduler's rate holding tasks back.
- No two controllers chased each other, and their timescales stayed apart, from the task scheduler's rate every second to the memory budget once.
- Replaying the traces reproduced every one of the controllers' 233,980 decisions.
- It found seven things to follow up. A pool still opening its connections reads as a queue, so the capacity controller cut at most host starts. The latency law, in shadow, would have cut in every scenario but the short pool, though the store was never short of capacity. And a rate limit can't help a pool too short for its load.
- [Follow-up runs on 9 November](#follow-up-runs-9-november) confirmed on DSQL the fixes for the start-up cuts and fair share's overshoot, #107 and #106. In a slow store that makes capacity's cuts bind, `CoupledBindingShare` and `CoupledRetry` let capacity probe back once the store recovered, but at low rates the task rate's `no-data` hold can pause the probes, or stop them.
- A [rerun on 11 November](#rerun-of-the-coupled-slow-store-11-november), with the task rate moving its mix on steps with few calls (#119), let the probes through: with every controller enabled, capacity probed back and every workflow completed. Without the coupled settings, capacity still held its cut.

## How it ran

**Topology.** Four processes on one laptop, against a throwaway DSQL cluster in eu-west-1, about 22 ms away:

- two history hosts, which split the 32 shards between them, 11 and 21;
- one process with frontend, matching and worker;
- a second frontend.

Each scenario had its own persistence schema, except the short pool, which reused the pool pressure's, and all of them shared one visibility schema. Each run had its own namespace and started all four processes afresh. The cluster was deleted after the last run.

**Load.** A load generator started workflows open-loop on a schedule of rates. Each workflow ran one activity, then slept on a 1 s timer, so it made transfer, timer and visibility tasks. 80% of the starts went through the first frontend and 20% through the second. The worker polled through the first, with 32 workflow and 32 activity pollers.

**Settings in every run.** These let each controller act, and stay the same whatever the controllers' modes:

- history's static persistence rate is 2,000 calls a second per host, so the capacity controller has room above the load;
- history's task scheduler rate limiter is on, with its shadow mode off;
- the history cache is limited by size and the host-level events cache is on;
- each history host has a 512 MiB `GOMEMLIMIT`;
- the legacy health limiter is on with its multiplier held at 1, so with the capacity controller in shadow the rate stays at the static rate;
- the signal plane ticks every second and writes traces, except in the `off` config.

**Scenarios.** Each ran for 300 s of load, then drained.

| Scenario | Load | Conditions |
|---|---|---|
| Baseline | 5 workflows/s | None |
| Ramp | 5, 10, 20 and 30/s, 75 s each | None |
| Pool pressure | 5/s | Each history host's pool has 10 connections rather than 64 |
| Short pool | 5/s | 4 connections, added when 10 turned out not to bind |
| Slow store | 5/s | From 90 s to 240 s, every reply from the store comes 40 ms later |
| Connection storm | 5/s | Connections live 1 minute; from 120 s, a separate client opens 300 connections a second for 60 s |
| Uneven frontends | 5/s, plus 90 requests/s in a second namespace from 30 s to 270 s, 72 through one frontend and 18 through the other | That namespace's global rate limit is 60/s |

The slow store's delay came from a proxy between the processes and the cluster, which held back each reply by the delay set at the time. The storm's client spent the cluster's connection rate, about 100 a second, which history's pools need to replace their connections.

**Configs.**

| Config | Controllers |
|---|---|
| `off` | Every controller disabled and the signal plane off: today's defaults |
| `shadow` | Every controller in shadow. Run first and last in each scenario, as `shadow` and `shadow2` |
| `cap`, `wrk`, `tsr`, `mem`, `par`, `fair` | One enabled, the others in shadow: persistence capacity, worker counts, task scheduler rate, memory budget, partition scaler, fair share |
| `all` | Every controller enabled |

A controller in shadow applies the static value, so outside the baseline `shadow` stands in for disabled. Each scenario ran the alone configs whose controller its load exercises: the ramp all of them but fair share; the pools, the slow store and the storm capacity, worker counts and task rate; uneven frontends fair share. That made 40 runs.

**Measures.**

- From the load generator: starts attempted and accepted, and each start's latency.
- From visibility: each workflow's status, and its end-to-end time, from start to close.
- From each process's metrics every 5 s: persistence calls, errors and `ResourceExhausted` rejections, task errors, the task scheduler's throttling, the heap.
- From each process's metrics at the end of the run: history's calls' latencies, and how long writes waited for their shard.
- From the traces: the store facts, and every controller decision, replayed with `tools/adaptivereplay`. A change of a tenth of the static value or more counts as a swing; smaller ones as jitter.

The plugin's pool gauges carry no store label, so a history host's persistence and visibility pools report into the same series. The pool's queued share comes from the store facts in the traces instead, which are the persistence pool's alone.

## Results

### Start-up, in every scenario

A history host's pool opens its connections as the host starts, a few at a time. Meanwhile the host takes its shards and reloads its queues' tasks from their last saved positions, as it would after any restart. Here those were the previous run's tasks, since a scenario's runs shared its schema: 94–501 calls per host for the previous run's namespace, besides 200–480 for the system namespace. So most of a host's first acquires waited:

- In 51 of the 66 host starts with 10 connections or more, the queued share was 0.28–0.96 in the host's first seconds, over the 0.25 that `MaxQueuedRatio` allows. The capacity controller cut to half its static rate for `queued`, 22–73 s before the load began.
- In one, with 10 connections, 191 acquires gave up waiting and the reloaded tasks' reads failed as unavailable. The error ratio reached 0.32, and the controller cut again, to a quarter, for `errors`.
- The first run of each scenario had no earlier tasks to reload. Leaving out the baseline's, which had no traces, 8 of their 10 host starts made no cut, and the other two had 10 connections.
- The limit didn't bind afterwards, so `idle` held the cut for the rest of the run. Holding is by design: a rate that isn't binding says nothing about the store.
- The worker controllers backed off for a few steps after each cut, as they're meant to when persistence backs off.

These cuts cost nothing here, because the hosts made 75–250 calls a second against limits of 1,000. With a static rate closer to the load, the limit would bind once the load arrived, and the controller would probe back up while it refused calls.

With 4 connections, history never got past start-up: that's the [short pool](#short-pool).

### Baseline

| Config | Completed | Start mean | End-to-end p50 / p99 | Heap max |
|---|---|---|---|---|
| `off` | 1,495 | 214 ms | 2.47 / 4.40 s | 99 MiB |
| `shadow` | 1,495 | 211 ms | 2.46 / 3.97 s | 104 MiB |
| `all` | 1,497 | 202 ms | 2.40 / 4.11 s | 106 MiB |
| `shadow2` | 1,497 | 203 ms | 2.42 / 3.99 s | 103 MiB |

- No start timed out, no workflow took over 7 s, and nothing returned `ResourceExhausted`.
- Shadow costs nothing visible: `off` and `shadow` are within the runs' noise, and so is `all`.

### Pool pressure

With 10 connections a host, the pools didn't bind. Every config matched the baseline: 1,495–1,498 workflows completed, end-to-end p99 was 3.9–4.5 s, and nothing returned `ResourceExhausted`. The queued share averaged 0.02–0.08 over each run, and passed 0.25 only now and then, on the host with 21 shards. So the pools were cut again, to 4 connections.

### Short pool

With 4 connections a host, history fell behind as it started and never caught up, in every config:

| Config | Starts that timed out | Completed | Workflow reads that failed or were refused | Persistence `ResourceExhausted` | Connections opened per host, 11 / 21 shards |
|---|---|---|---|---|---|
| `shadow` | 155 | 5 | 99–100% | 0 | 1,072 / 1,799 |
| `cap` | 127 | 0 | 100% | 54,362 | 1,066 / 1,046 |
| `wrk` | 921 | 0 | 100% | 0 | 1,891 / 3,039 |
| `tsr` | 1,047 | 0 | 100% | 0 | 2,318 / 3,153 |
| `all` | 83 | 0 | 100% | 2,702 | 1,357 / 1,243 |
| `shadow2` | 1,436 | 0 | 100% | 0 | 3,280 / 3,709 |

- Each host began by reloading the previous run's tasks, as in every scenario. With 4 connections, those alone were more than its pool could serve, and almost no read of a workflow's state succeeded before the load began.
- A read of a workflow's state is up to 9 queries, one after another, and each waits its own turn for a connection. With the pool's queue long, reads ran out of time part-way, after using connections for their first queries, and their tasks tried them again, so the queue never drained.
- A call that ran out of time during its query lost its connection too, so the pools kept opening new ones: 1,000–3,700 a host in each run, against 20–30 with 10 connections.
- The store's p99, which counts the wait for a connection, sat at about 3 s, the tasks' deadline.
- Only once did a host catch up: in the first run, the host with 11 shards served its reads for about 30 s soon after the load began, then fell behind again.
- The runs shared one schema, so each inherited the unfinished workflows of the runs before it and started further behind. In `shadow` and `shadow2`, the same config first and last, 155 and 1,436 starts timed out. So the configs compare in kind, not in degree.

The controllers:

- **Capacity**, in shadow, proposed a cut to half for `queued` 25–27 s before the load, then a cut for `errors` at each cooldown, down to its floor of 0.05 of the static rate, 100 calls a second, before the load began. Enabled, it made the same cuts. The floor was still above what the pool could serve, so calls kept failing, and on the host with 21 shards the limit also refused about 50,000 calls with `ResourceExhausted`. Without the limit, reads failed after waiting instead, and no workflow completed either way.
- **Worker counts** held at their static values all run, as `backoff`, enabled or in shadow, though tasks waited up to 17 s in a scheduler. They back off while persistence does, and more workers would only have queued more calls at the pool.
- **The task scheduler's rate**, enabled alone, followed a capacity rate held at its static value, so it changed nothing. With every controller enabled, it followed the capacity rate down, to 60–95 task attempts a second per host against 2,000, and the schedulers held back about 94,000 attempts. So the hosts made fewer reads than with capacity alone, though they started further behind. The limit refused about 2,700 calls rather than 54,000, and the fewest starts of any run timed out, 83. Still no read succeeded, and no workflow completed.

What overloaded the pool was reads that each waited for a connection up to 9 times and ran out of time. A rate limit bounds how many calls start each second, not how many wait at once, and its floor kept it above what the pool served. A limit on the calls in flight near the pool's size, failing a call at once when it can't have a connection in time, or a read taking one connection for all its queries, would each act on the queue itself.

### Slow store

![slow store · 6 runs](adaptive-behaviour/appreciation-slow-store.svg)

| Config | Completed | End-to-end p50 / p99 / max | Over 10 s | History `ResourceExhausted` | Task errors |
|---|---|---|---|---|---|
| `shadow` | 1,495 | 11.5 / 119 / 145 s | 762 | 52 | 766 |
| `cap` | 1,495 | 14.3 / 123 / 152 s | 775 | 31 | 798 |
| `wrk` | 1,498 | 12.4 / 120 / 155 s | 765 | 43 | 860 |
| `tsr` | 1,495 | 16.7 / 131 / 148 s | 800 | 46 | 966 |
| `all` | 1,498 | 15.1 / 127 / 157 s | 791 | 34 | 844 |
| `shadow2` | 1,496 | 10.7 / 126 / 159 s | 756 | 31 | 772 |

- While each reply came 40 ms later, workflows closed at about 2 a second against 5 started. The store's p99 rose from about 190 ms to 810–930 ms, peaking at 1.2–1.3 s. The backlog cleared within 60 s of the delay ending, at up to 19 a second.
- Persistence calls fell by only 10–20%, to about 37 and 67 a second on the two hosts, so each closed workflow took about twice the calls. Slower writes held workflows' locks longer, and 700–1,560 task attempts per host found their workflow busy and were retried, against about 10 in the baseline.
- Persistence and the frontends returned no `ResourceExhausted`. History's were all `BusyWorkflow`, on matching's calls to record a workflow task as started: the call couldn't take the workflow's lock in time while a slower write held it.
- Every config gave the same goodput and latency, within the runs' noise. No controller acted on the slowdown:
  - **Capacity** held its rate, as `idle`: the hosts' 37 and 67 calls a second were far below their limits of 1,000 or 2,000, so the limit wasn't binding and a cut would change nothing.
  - **Worker counts** shrank, as they did in the baseline. Tasks waited about 0.1 ms in their schedulers, so the workers weren't what held them up.
  - **The task scheduler's rate** followed the capacity rate, far above the tasks' rate.
- What slowed workflows is history's writes, as in the October [shadow runs](adaptive-store-facts.md#shadow-runs)' ramp. A write waits on several of the store's replies in turn, so it took about 600 ms rather than 155 ms, and writes to each shard go one at a time (`history.shardIOConcurrency` is 1). The shards spent 31–84% of their time writing, and a workflow's 9 or so writes, mostly one after another, each waited for its shard's turn. No controller reads the shards' writes.
- **The latency law, in shadow.** DSQL reports store facts, so the capacity controller's latency law runs in shadow. Its condition held on 138–160 steps per host while the delay lasted. Had it acted, each cut would have set the rate to 90% of the host's throughput, down to the 100 calls a second floor, so the limit would have bound and refused calls the store was serving. The latency came from the network, not from load, and a rise in latency without a queue doesn't say the store is overloaded. That supports keeping the latency law in shadow for stores that report facts.

### Connection storm

| Config | Completed | End-to-end p50 / p99 | Queued share | Store p99, peak |
|---|---|---|---|---|
| `shadow` | 1,496 | 2.48 / 4.41 s | 0.00 | 470 ms |
| `cap` | 1,498 | 2.52 / 4.43 s | 0.02 | 591 ms |
| `wrk` | 1,495 | 2.49 / 4.46 s | 0.02 | 522 ms |
| `tsr` | 1,497 | 2.48 / 4.15 s | 0.04 | 408 ms |
| `all` | 1,497 | 2.47 / 4.20 s | 0.02 | 465 ms |
| `shadow2` | 1,496 | 2.51 / 4.42 s | 0.03 | 425 ms |

- DSQL refused 61–62% of each storm's 18,000 connection attempts, with 53400.
- History's calls didn't notice. None failed or was throttled, nothing returned `ResourceExhausted`, and every config matched the baseline.
- The plugin keeps each pool at its full size, so the pools had idle connections to spare while they replaced the ones whose minute was up.
- The store's p99 peaked at 400–590 ms, against about 370 ms in the baseline, but not during the storm's minute.
- No controller acted, and none needed to.

### Ramp

![ramp · 8 runs](adaptive-behaviour/appreciation-ramp.svg)

| Config | Completed | End-to-end p50 / p99 / max | Over 10 s | Drain | History `ResourceExhausted` | Task errors | Heap max |
|---|---|---|---|---|---|---|---|
| `shadow` | 4,863 | 16.9 / 119 / 156 s | 3,037 | 103 s | 2 | 660 | 222 MiB |
| `cap` | 4,863 | 19.6 / 112 / 149 s | 3,149 | 91 s | 0 | 547 | 221 MiB |
| `wrk` | 4,862 | 20.5 / 120 / 135 s | 3,066 | 88 s | 1 | 604 | 204 MiB |
| `tsr` | 4,864 | 20.7 / 118 / 172 s | 3,172 | 95 s | 5 | 615 | 224 MiB |
| `mem` | 4,866 | 17.8 / 118 / 168 s | 2,981 | 93 s | 0 | 643 | 210 MiB |
| `par` | 4,866 | 20.8 / 121 / 156 s | 3,255 | 103 s | 2 | 852 | 203 MiB |
| `all` | 4,863 | 21.4 / 115 / 163 s | 3,168 | 93 s | 2 | 628 | 224 MiB |
| `shadow2` | 4,863 | 17.0 / 106 / 146 s | 2,982 | 88 s | 0 | 578 | 215 MiB |

- Every workflow that started completed, in every config. At 5 and 10 a second, workflows closed as fast as they started. At 20 and 30 a second they closed at about 15 a second, and the backlog closed at 17–20 a second once the starts stopped, within 88–103 s. So about 3,000 of each run's 4,865 workflows took over 10 s.
- Every config gave the same goodput and latency, within the runs' noise.
- Persistence and the frontends returned no `ResourceExhausted`. History's 0–5 were `BusyWorkflow`, as in the slow store.
- The store kept up. Its p99 held at 180–188 ms, peaking at 326–422 ms, the pools' queued share was 0.00–0.04, and history's calls peaked at about 140 and 260 a second on the two hosts. A write took about 155 ms in the store at every rate.
- What limited goodput is history's writes to each shard, which go one at a time, and the worker's 32 workflow task pollers:
  - Each workflow made about 9 writes. At 20 and 30 a second, the shards spent 60–86% of their time writing, and over each run a write waited 205–296 ms on average for its shard's turn, against 30–44 ms in the baseline. At 155 ms a write, the 32 shards could carry at most about 22 workflows a second.
  - A poll returns once history has recorded the workflow task as started, which is one of those writes, so each poll took longer: recording a task as started took 354–444 ms on average over each run, against 179–198 ms in the baseline. The SDK's pollers favour workflows already running, so new workflows' first tasks waited in matching. Its backlog reached about 1,400 tasks, 85 s old, while tasks were added at 30 a second and taken at 12–13.
- The controllers did what the slow store showed, at every step of the load:
  - **Capacity** held its start-up cut, as `idle`, since history's calls came nowhere near the limit.
  - **Worker counts** shrank every 30 s, to their floor of half the static count, since tasks waited about 0.02 ms in the schedulers. Goodput didn't change: `wrk` and `all` match `shadow`.
  - **The task scheduler's rate** followed the capacity rate each second, and never throttled a task.
  - **The memory budget** brought each cache to its share of the 512 MiB limit in its first seconds: the history cache to 0.15 of its size setting, the events cache to 0.20 and the queues' pending tasks to 0.14. With it enabled, history read workflows' state and history from the store as often as in the shadow runs, and its heap peaked at 210 MiB, so the smaller caches still held what the load used.
  - **The partition scaler** kept the workflow task queue's 4 partitions through the load, though its backlog grew. Tasks were added at no more than 32 a second, below the 50 a second under which partitions don't hold a queue back, so it ran no trials, rightly: the pollers, not the partitions, held the queue back. It proposed 3 partitions in the drain, once its 300 s hold had passed.

### Uneven frontends

![uneven frontends · 4 runs](adaptive-behaviour/appreciation-uneven.svg)

| Config | Admitted a second over the 240 s: first frontend / second / together | Throttled a second: first / second | Shares applied | Workflows' end-to-end p99 |
|---|---|---|---|---|
| `shadow` | 30.2 / 18.0 / 48.2 | 41.8 / 0.0 | 30 / 30 | 4.49 s |
| `fair` | 44.4 / 17.6 / 62.0 | 27.6 / 0.3 | 45.4 / 17.5 | 4.52 s |
| `all` | 45.6 / 16.0 / 61.6 | 26.4 / 2.0 | 46.7 / 16.9 | 3.77 s |
| `shadow2` | 30.2 / 18.0 / 48.2 | 41.8 / 0.0 | 30 / 30 | 4.32 s |

- Without fair share, each frontend took half the namespace's limit of 60 a second. The first, sent 72 a second, admitted 30 and throttled 42. The second, sent 18, admitted them all, and left 12 a second of its half unused. Together they admitted 48 a second, 80% of the limit.
- With fair share enabled, the frontends exchanged their demand every 5 s. Within 25 s of the requests starting, the first frontend's share rose to 45–47 a second and the second's fell to about 17, and from then on together they admitted 63–64 a second.
- That's 105–106% of the limit. Each frontend measures demand over the last 30 s, so the first shares came from windows still filling, a round apart: the first frontend's own demand read 38–59 a second rather than 72. Once the windows had filled, neither share's proposal moved by the deadband, a tenth of an equal share or 3 a second, so both shares held for the rest of the run.
- In `all`, while the windows filled, the second frontend's share dipped to 12.7 a second, below the 18 it was sent, and for half a minute it throttled up to 5 a second.
- The workflows' load, through the same frontends, was unaffected in every config.

## Loops and timescales

![timescales · ramp, all enabled](adaptive-behaviour/appreciation-timescales.svg)

The controllers moved on separate timescales, and none moved more with the others enabled than alone. Changes per run count every series a controller drives, on both history hosts, or on both frontends for fair share. The short pool's runs are left out, since its hosts made almost no progress; its section covers what the controllers did there.

| Controller | Changes per run | When it moved | With the others enabled |
|---|---|---|---|
| Persistence capacity | 0–4 | Once at most host starts, for `queued` | The same |
| Task scheduler rate | 414–761 | Every second | Lower once capacity cut, as it follows the capacity rate |
| Worker counts | 60–84 | Every 30 s, by 5%, down to their floor | The same |
| Memory budget | 34–36 | In its first seconds, down to its budget, then not again | The same |
| Partition scaler | 0–1 | Once its 300 s hold had passed, in the ramp | The same |
| Fair share | 16–30 | At each 5 s round while the demand moved, then held | The same |

- The couplings built into the laws showed as built: the task rate followed the capacity rate each second, and the worker controllers backed off for 3–6 steps after each capacity cut.
- No two controllers chased each other. With every controller enabled, each made about the changes it made alone or in shadow.
- The task scheduler's rate is the one law that didn't settle. It steps with each second's ratio of task attempts to persistence calls, which is noisy at these rates, and passes it on unsmoothed: 6–37 swings per run, 0–19 of them reversed. Outside the short pool, its rate stayed far above the tasks' rate, so the scheduler never throttled a task and the jitter cost nothing. Replaying with a smaller `MaxStepFrac` only made the steps smaller.

## Shadow proposals

What each controller proposed while it was in shadow, against what the load needed:

- **Persistence capacity.** Besides the start-up cuts, it proposed two cuts during the load, both with 10 connections on the host with 21 shards: to 0.25 of its static rate at 5 s and to 0.125 at 98 s, at queued shares of 0.26 and 0.30. Those queues were real, but neither limit would have bound: the host made 82 and 111 calls a second, against 500 and 250. With 4 connections, it proposed cutting to its floor before the load began, as it cut when enabled.
- **The latency law.** DSQL reports store facts, so the latency law stays in shadow. Its condition held in every scenario but the short pool, in most runs on at least one host:

  | Scenario | Steps per host | What raised the store's p99 |
  |---|---|---|
  | Baseline | 0–3 | Nothing in particular |
  | Pool pressure | 0–12 | Start-up, while the pools opened |
  | Connection storm | 12–67 | Connections replaced every minute |
  | Ramp | 0–227 | The load, on the two hosts whose baselines were lowest, 93 and 100 ms |
  | Slow store | 138–160 | The delay |
  | Short pool | 0 | Counted only when the facts don't call for a cut, and in the short pool they did throughout |

  Had it acted, each cut would have set the rate to 90% of the host's throughput, and refused calls the store was serving. In none of these was the store short of capacity. That supports keeping the latency law in shadow for stores that report facts, as the October [shadow runs](adaptive-store-facts.md#shadow-runs) suggested. Its baseline also varied: learned from each host's first calls, it ranged from 93 to 176 ms across the host starts, so the same store passed the target on some hosts and not on others.
- **Worker counts.** Shrink every 30 s, down to the floor of half the static count, in every scenario but the short pool. There they held the static count, as `backoff`, while persistence backed off. Elsewhere tasks waited 0.02–0.1 ms in the schedulers against a 100 ms target, so the controllers never proposed growth.
- **Task scheduler rate.** The capacity rate scaled by attempts per call: 0.3–1.0 of the static rate, far above the tasks' rate, except in the short pool, where it followed the capacity cuts down to about 0.05.
- **Memory budget.** The budget at once, then steady: the history cache at 0.15 of its setting, the events cache at 0.20, the pending tasks at 0.14. The heap peaked at 86–224 MiB, well under the 512 MiB limit, so memory never pressed.
- **Partition scaler.** 4 partitions through every load, and 3 once its hold had passed in the ramp's drain. No queue's add rate reached the 50 a second its trials need.
- **Fair share.** Raise the busier frontend's share from 30 to 43–51 a second, and lower the other's to 13–17. Enabled, those shares lifted the namespace's admitted requests from 48 to 63 a second.

## Gate criteria

The roadmap put a gate after each phase. These are the three this appreciation can speak to:

| Gate | Phase | What the appreciation shows |
|---|---|---|
| Control laws stable on recorded traces | 0 | Replaying the 234 traces reproduced all 233,980 decisions, each with the same proposal and reason. No law oscillated, though the task scheduler's rate jittered. The traces are this rig's, not production's. |
| Worker controller safe to enable | 2 | Enabled alone or with the others, it only shrank workers, at no cost to goodput, latency or errors. In the short pool it held them while persistence backed off, though tasks waited up to 5.5 s in a scheduler. It never grew them, so growth rests on the controller's tests. |
| No `ResourceExhausted` churn when the database degrades | 3 | With the store 40 ms slower, persistence and the frontends returned none in any config. History's 31–52 were `BusyWorkflow`, and came with every controller in shadow too. The storm brought none. With 4 connections, though, the capacity controller's limit refused about 54,000 calls in a run when enabled alone, and 2,700 with every controller enabled, while history made no progress with the limit or without it. So this gate isn't met for a pool too short for its load. |

Phase 1's gate asks for shadow proposals reviewed, which the section above does, and for disabled mode unchanged, which was checked against main's build before the appreciation.

## What it found

Outside the short pool, enabling the controllers, alone or together, changed no scenario's goodput, latency or errors beyond the runs' noise, except fair share's, which lifted the uneven namespace's admitted requests. Along the way it found these, each a candidate for a follow-up:

1. **Pool warm-up reads as pool pressure.** In 51 of 66 host starts, the capacity controller cut to half for `queued` while the pool opened its connections, and `idle` then held the cut. Counting queued facts only once the pool is full, or a grace period after start, would keep the cut for real queues.
2. **The latency law would cut where the store isn't short.** Kept in shadow for stores that report facts, its condition held in every scenario but the short pool, where the facts already called for cuts. Its baseline, learned from a host's first calls, ranged from 93 to 176 ms across host starts.
3. **The task scheduler's rate moves every second.** It passes each second's attempts-per-call ratio on unsmoothed. Smoothing the ratio, or a deadband, would settle it; a smaller `MaxStepFrac` only makes the steps smaller.
4. **Fair share's deadband can let shares pass the limit.** Each frontend set its share from demand a round apart from the other's, and the deadband then held both, so they added up to 105% of the namespace's limit for the rest of the run. Applying any step down at once, or holding only while the shares fit the limit, would keep them within it.
5. **What bound goodput isn't a signal any controller reads.** The ramp and the slow store were bound by history's writes to each shard, one at a time, and by the worker's pollers waiting on them. Whether `history.shardIOConcurrency` can rise above 1 on DSQL is an open question.
6. **The plugin's pool gauges carry no store label.** A history host's persistence and visibility pools report into one series.
7. **A pool too short for its load falls behind for good, and a rate limit can't help it.** With 4 connections, reads of a workflow's state, up to 9 queries each waiting its own turn for a connection, ran out of time part-way and were tried again, and each host opened 1,000–3,700 connections a run. The capacity controller cut to its floor, still above what the pool served. A limit on the calls in flight near the pool's size, failing a call at once when it can't have a connection in time, or one connection for all of a read's queries, would act on the queue itself.

## What it doesn't cover

- **The store's own limits.** From one laptop, history's writes to each shard reach their limit long before DSQL does. No call of history's was throttled or saw a refused connection, so the cuts for throttled facts still have no live evidence.
- **Load a laptop can't reach.** That's the last part of the appreciation, in a discrete-event simulator. [`tools/adaptivesim`](adaptive-behaviour.md#closed-loop-simulation) runs the controllers closed-loop on a coarse model of history and its store. It runs all six: persistence capacity, the task scheduler rate, the worker counts, the memory budget, the partition scaler, which it takes past the 50 tasks a second its trials need, and fair share, between frontends whose exchanges serve each other over gRPC in memory. Its `ramp` runs the ramp's configs on a model of this rig, with one that disables every controller and turns the signal plane off, and every config gives the same goodput, bound by history's writes to each shard and the pollers, as in finding 5.
- **One run per config.** Each config ran once, so a difference smaller than the one between a scenario's `shadow` and `shadow2` runs is noise.
- **Disabled.** Outside the baseline, `shadow` stood in for disabled.
- **One machine.** The four processes shared one laptop's CPU and memory, and one network path to the cluster.
- **Five minutes of load.** The memory budget and the partition scaler move over minutes, so the runs show their first steps more than where they settle.
- **Growth.** Only the short pool's schedulers queued, and there the worker controllers held while persistence backed off. No task queue's add rate reached 50 a second. So the worker controllers never grew workers, and the partition scaler ran no trials.

## Follow-up runs, 9 November

*Run on 9 November 2026 on the branch as of #117. They confirm #106 and #107 live, and try `CoupledBindingShare` with `CoupledRetry` on DSQL before either is enabled by default.*

**How they ran.** The rig, settings and measures were those above, on a new throwaway cluster, deleted after the last run. There were three scenarios:

| Scenario | Load and conditions | Configs | Schema |
|---|---|---|---|
| Uneven frontends | As above | `shadow`, `fair`, `all`, and `fair2`, which repeats `fair` | One for all four runs, so each run after the first reloads the one before's tasks as it starts |
| Pool pressure | As above, with 10 connections a history host | `cap`, `all`, and `cap2`, which repeats `cap` | One for all three runs |
| Coupled slow store | 5 workflows a second for 390 s. Each history host has 10 connections, and history's static persistence rate is 300 calls a second, so capacity's cuts bind. From 90 s to 240 s, every reply from the store comes 40 ms later | `shadow`; `cap`, capacity enabled alone; `ct`, capacity and the task rate enabled; `ctc`, as `ct` with `CoupledBindingShare` at 0.9 and `CoupledRetry` at 200 ms; `allc`, every controller enabled, with both settings | A fresh one for each run |

Every run completed all its workflows but three of the coupled slow store's, below. Replaying the 72 traces reproduced all 83,744 of the controllers' decisions.

### Start-up, with the grace

| Connections | Reloads the run before's tasks | Host starts | Queued share over 0.25 in the first 30 s | Cuts made in the first 30 s | Cuts in a replay without the grace |
|---|---|---|---|---|---|
| 64 | Yes | 6 | 3 | 0 | 3 |
| 64 | No | 2 | 0 | 0 | 0 |
| 10 | Yes | 4 | 4 | 0 | 4 |
| 10 | No | 12 | 12 | 0 | 12 |

- In 19 of the 24 host starts, the queued share reached 0.57–0.98 in the host's first seconds, as on 3 November. Each time, it passed 0.25 within 6 s of the capacity controller's first step, well inside the 30 s `QueuedGrace`, and the controller made no cut.
- Replayed with `QueuedGrace` at 0, as before #107, the controller cuts for `queued` at each of those 19 starts, 2–5 s after its first step.
- After the grace, no host's queued share passed 0.25 before its load began. No start-up saw a throttled call or an error, so #108's pool gate refused nothing as the hosts started.
- During the load, before any slowdown, the controller cut three times for a queue, each time on the host with 21 shards and 10 connections, by half, at a queued share of 0.28–0.38. None of the cuts bound: the host made about 76 calls a second.

### Uneven frontends, with #106 and #115

| Config | Admitted a second over the 240 s: first frontend / second / together | Throttled a second: first / second | Shares the limiters took at 20 s, then at 80 s: first / second | Admitted together a second: 20–80 s / from 100 s | Most the shares added up to |
|---|---|---|---|---|---|
| `shadow` | 30.2 / 18.0 / 48.2 | 41.8 / 0.0 | 30 / 30, then 30 / 30 | 48.0 / 48.0 | 60 |
| `fair` | 42.6 / 16.3 / 59.0 | 29.4 / 1.7 | 43.4 / 17.3, then 43.4 / 15.6 | 61.3 / 59.0 | 68.6 |
| `all` | 39.8 / 16.5 / 56.3 | 32.2 / 1.5 | 30 / 30, then 44.3 / 15.6 | 48.0 / 59.9 | 60.0 |
| `fair2` | 39.8 / 16.5 / 56.3 | 32.2 / 1.5 | 30 / 30, then 44.3 / 15.6 | 48.0 / 59.9 | 60.0 |

- Once settled, the shares added up to 59.0 in `fair` and 59.9 in `all` and `fair2`, within the namespace's limit of 60, where on 3 November they held at 63–64 (#106). From 100 s on, each frontend admitted its share exactly.
- A frontend's limiter for a namespace takes its share once a minute, from the frontend's first request for the namespace, as the design expects. Here each frontend's limiter started as the namespace was registered, 40 s before the uneven requests, so the limiters took their shares 20 s and 80 s after the requests began. The first frontend's admitted rate stepped from 30 to 44 a second at one of those two moments in each run.
- In `all` and `fair2`, both shares stayed equal, with the reason `filling`, until the frontends' windows had filled, then moved to 44.3 and 15.6 within 2 s of each other, 29–31 s after the requests began. They never added up to more than 60 (#115). The limiters took them at 80 s, so after its first second's burst the first frontend admitted 30 a second until then, and over the 240 s the frontends together admitted 56.3 a second.
- In `fair`, the shares moved from the first round, 3–5 s after the requests began, on windows still filling, and for 25 s they added up to more than 60, at most 68.6. A few calls for the new namespace as it was registered had counted as its demand on both frontends, so neither window counted as filling when the uneven requests began. In `all` and `fair2`, those calls counted on the second frontend only.
- The overshoot in `fair` reached the limiters only as the shares they took at 20 s, which added up to 60.7. Between 20 s and 80 s, the frontends together admitted 61.3 a second, a little over the limit, while the second frontend's burst let through the 18 a second it was sent.

### Coupled slow store

![coupled slow store, host with 11 shards · 4 runs](adaptive-behaviour/appreciation-coupled-h1.svg)

![coupled slow store, host with 21 shards · 4 runs](adaptive-behaviour/appreciation-coupled-h2.svg)

![coupled slow store, workflows closed · 5 runs](adaptive-behaviour/appreciation-coupled-goodput.svg)

| Config | Completed | Running when the load ended | Running after the drain | Closed a second: while slow / from 240 s to 390 s | End-to-end p50 / p99 |
|---|---|---|---|---|---|
| `shadow` | 1,948 of 1,948 | 53 | None, 9 s in | 0.9 / 8.8 | 34 / 208 s |
| `cap` | 1,928 of 1,945 | 631 | 17, after 180 s | 0.4 / 5.4 | 35 / 456 s |
| `ct` | 826 of 1,947 | 1,157 | 991, after 180 s | 0.7 / 0.8 | – |
| `ctc` | 1,947 of 1,947 | 951 | None, 144 s in | 0.7 / 3.0 | 117 / 410 s |
| `allc` | 783 of 1,948 | 990 | 847, after 180 s | 0.7 / 0.7 | – |

The counts come from visibility. In `ct` and `allc`, visibility's own tasks waited in the same throttled scheduler, so it hadn't recorded 130 and 318 of the starts: more workflows were running than it shows, and their end-to-end times are left out.

Capacity's rate in calls a second, and the task rate's in tasks a second, on the host with 11 shards, then the one with 21:

| Config | Capacity while slow, least | At 240 s | Most from 240 s to 390 s | At 390 s | Task rate at 390 s | Queued cuts from 240 s to 390 s |
|---|---|---|---|---|---|---|
| `cap` | 24 / 25 | 27 / 30 | 125 at 274 s / 143 at 281 s | 103 / 98 | In shadow | 2 / 7 |
| `ct` | 25 / 30 | 31 / 36 | 38 at 245 s / 44 at 268 s | 38 / 44 | 7 / 6 | 0 / 0 |
| `ctc` | 27 / 26 | 37 / 36 | 124 at 367 s / 143 at 336 s | 107 / 109 | 53 / 31 | 1 / 3 |
| `allc` | 24 / 29 | 32 / 35 | 47 at 326 s / 56 at 351 s | 47 / 56 | 7 / 6 | 0 / 0 |

- While the store was slow, the hosts made about the same calls in every config, 30–38 a second, and closed 0.4–0.9 workflows a second. Enabled, capacity cut to 24–30 calls a second for the pool's queued and throttled facts, and the queue moved out of the pool: on the host with 21 shards, the queued share fell from 0.95 in `shadow` to 0.19–0.29, and the store's p99 from 3.3 s to 0.7 s.
- **Without the coupled settings** (`ct`), capacity held its cut after the store recovered, as the simulator showed. It probed to 38 and 44 calls a second within 30 s, then held as `idle` for the rest of the run, while the task rate held 6–8 tasks a second.
- **With both** (`ctc`), capacity probed back up, to 124 and 143 calls a second, 127 s and 96 s after the store recovered, and every workflow completed, 144 s into the drain. In the simulator, the share took capacity from about 100 a second to about 200 within 35 s. On the host with 21 shards, the queued share then cut the rate three times during the load and four times in the drain, each time to 94–109, as the waiting tasks were released.
- **The probes paused**, for 51 s on the host with 11 shards and 20 s on the other. At these rates, most of the task rate's steps saw fewer than its `MinSamples`, 100 calls, so it held its rate as `no-data` while capacity's rose. History's calls then stayed below `CoupledBindingShare` of what the scheduler let through, so capacity held as `idle` until a step saw 100 calls and the task rate moved.
- **With every controller enabled** (`allc`), the probes didn't resume. Both hosts' task rates held 6–7 tasks a second, at or near their floor of 6, as `no-data` on all but 8 of their 300 steps from 240 s to 390 s, and capacity rose no further than 47 and 56 calls a second. With one run per config, these runs can't say whether the other controllers made the difference; the hold is the same as in `ctc`'s pauses.
- **Capacity alone** (`cap`) probed back fastest, to 125 and 143 calls a second within 34 s and 41 s of the recovery. With the task rate in shadow, its limit bound on history's calls themselves: they saw 33,569 `ResourceExhausted`, against 605–1,197 with the task rate enabled.
- No enabled config cleared the backlog as quickly as `shadow`. After the recovery, `shadow`'s host with 21 shards made 121 calls a second with its pool's queued share at 1.0, and 8.8 workflows closed a second, where 5.4 did in `cap` and 3.0 in `ctc`. Capacity keeps the pool's queue under `MaxQueuedRatio`, 0.25: with 10 connections, its queued cuts held that host at 85–109 calls a second.
- With `CoupledRetry` at 200 ms, the task scheduler's throttled submissions rose to 172,592 in `ctc` and 234,097 in `allc`, from 34,791 in `ct`: about 320 and 410 a second across the two hosts, against 60.

### What they found

1. **The grace keeps the start-up cuts away.** In 19 of 24 host starts the pool's warm-up read as a queue, and a replay without #107's grace cuts at each of them. With it, the controller made none.
2. **Fair share's settled shares fit the limit.** They added up to 59.0–59.9 a second against the namespace's 60, where on 3 November they held at 63–64 (#106).
3. **Fair share's hold covers only a frontend's first requests for a namespace.** A namespace with earlier requests, as registration made here, moves on windows that are still filling. Starting the hold again after a window with no demand would cover a namespace that's been quiet. Letting a rising share wait a round for the falling ones, as it does after the hold, would cover a step in load.
4. **A share applies at the limiter's next refresh.** Each frontend's namespace limiter takes its share once a minute, as the design expects, so with the hold the uneven shares here applied 80 s after the requests began. Refreshing a namespace's limiters when the exchange changes its share would apply each share within a round.
5. **The coupled settings let capacity probe back on DSQL.** Without them, capacity and the task rate held the cut after the store recovered, and 991 workflows were still running after the drain. With them, every workflow completed.
6. **The task rate's `no-data` hold can stall the coupling at low rates.** Below `MinSamples` calls a step, the task rate keeps its rate rather than follow capacity's, so history's calls stay below `CoupledBindingShare` and capacity holds as `idle`. It paused the probes for 20–51 s in `ctc`, and held them for the rest of the run in `allc`. Following the persistence rate on those steps with the mix it last measured, as `held` steps do, isn't enough: replayed through these traces, it moved the task rate by at most 2.2 a second on the host with 11 shards, and not at all on the other, since only a step that measures the mix moves history's calls toward the share. The task rate now moves the mix on those steps by their calls' share of `MinSamples`, as [Proposal 3](adaptive-behaviour.md#proposal-3-one-persistence-capacity-controller) describes, and the [rerun on 11 November](#rerun-of-the-coupled-slow-store-11-november) probed back with it in every run with the coupled settings.
7. **With a short pool, capacity's queued cuts trade throughput for a short queue after a store recovers.** They held the rate where the pool's queued share stays under 0.25, about 100 calls a second here, so the backlog drained more slowly than with no limit, which reached 121 calls a second with a full queue.

### What they don't cover

- **One run per config.** `ctc` and `allc` differ in the other controllers and in how often a step saw 100 calls, and one run each can't separate the two. The [rerun](#rerun-of-the-coupled-slow-store-11-november) did: with the task rate moving on few calls, `allc` probed back as `ctc` did.
- **A static rate this low.** The stall needs history's calls under about 40 a second while the coupling binds. That took this rig's static rate of 300, whose floor, 5% of it, is 15 calls a second; at the 2,000 of the other scenarios, the floor is 100.
- **The store's own limits.** The throttled facts came from #108's pool gate, which refused 6,220 calls in `shadow` and 45–78 in the enabled configs. The servers logged no throttling error from DSQL.

## Rerun of the coupled slow store, 11 November

*Run on 11 November 2026 on the branch as of #119, which moves the task rate's mix on steps with fewer than `MinSamples` calls, after the follow-up runs' [finding 6](#what-they-found). It reruns the coupled slow store before `CoupledBindingShare` and `CoupledRetry` are enabled by default.*

**How it ran.** As the follow-up runs' [coupled slow store](#coupled-slow-store), on a new throwaway cluster, deleted after the last run, with a fresh schema for each run. There were five runs: `shadow`, `ct`, `ctc`, `allc`, and `ctc2`, which repeats `ctc`. In every run, the shards split 11 and 21 between the two hosts, as on 9 November. Replaying the 30 traces reproduced all 43,198 of the controllers' decisions.

![coupled slow store rerun, host with 11 shards · 5 runs](adaptive-behaviour/appreciation-rerun-h1.svg)

![coupled slow store rerun, host with 21 shards · 5 runs](adaptive-behaviour/appreciation-rerun-h2.svg)

![coupled slow store rerun, the task rate on the host with 11 shards · 5 runs](adaptive-behaviour/appreciation-rerun-tasks-h1.svg)

![coupled slow store rerun, workflows closed · 6 runs](adaptive-behaviour/appreciation-rerun-goodput.svg)

| Config | Completed | Running when the load ended | Running after the drain | Closed a second: while slow / from 240 s to 390 s | End-to-end p50 / p99 |
|---|---|---|---|---|---|
| `shadow` | 1,947 of 1,947 | 125 | None, 15 s in | 0.9 / 8.4 | 39 / 207 s |
| `ct` | 1,380 of 1,945 | 1,202 | 555, after 180 s | 0.6 / 1.3 | – |
| `ctc` | 1,945 of 1,945 | 848 | None, 154 s in | 0.7 / 3.7 | 119 / 419 s |
| `ctc2` | 1,947 of 1,947 | 817 | None, 136 s in | 0.6 / 4.0 | 109 / 403 s |
| `allc` | 1,948 of 1,948 | 531 | None, 103 s in | 0.6 / 5.8 | 76 / 364 s |

The counts come from visibility, which hadn't recorded 10 of `ct`'s starts, so its end-to-end times are left out, as on 9 November.

Capacity's rate in calls a second after the store recovered, on the host with 11 shards, then the one with 21, beside the 9 November runs. The longest pause is the longest time between two of capacity's rises before its highest rate:

| Run | At 240 s | Seconds from 240 s to 100 a second | Most from 240 s to 390 s | Longest pause | At 390 s | Task rate at 390 s | Queued cuts from 240 s to 390 s |
|---|---|---|---|---|---|---|---|
| `ct`, 9 November | 31 / 36 | – | 38 / 44 | Held | 38 / 44 | 7 / 6 | 0 / 0 |
| `ct` | 29 / 36 | – | 46 / 49 | Held | 46 / 49 | 16 / 10 | 0 / 0 |
| `ctc`, 9 November | 37 / 36 | 120 / 85 | 124 / 143 | 51 / 21 s | 107 / 109 | 53 / 31 | 1 / 3 |
| `ctc` | 28 / 40 | 67 / 89 | 123 / 141 | 14 / 36 s | 91 / 110 | 47 / 28 | 2 / 3 |
| `ctc2` | 37 / 29 | 42 / 79 | 132 / 130 | 6 / 9 s | 118 / 125 | 61 / 37 | 1 / 3 |
| `allc`, 9 November | 32 / 35 | – | 47 / 56 | Held | 47 / 56 | 7 / 6 | 0 / 0 |
| `allc` | 31 / 41 | 76 / 39 | 136 / 145 | 5 / 5 s | 101 / 93 | 50 / 42 | 3 / 6 |

The task rate's steps from 240 s to 390 s, both hosts together, 300 steps a run:

| Run | Steps under 100 calls | `no-data` | `partial` | `held` | `coupled` |
|---|---|---|---|---|---|
| `ct`, 9 November | 250 | 250 | – | 40 | 10 |
| `ct` | 147 | 0 | 49 | 206 | 45 |
| `ctc`, 9 November | 134 | 134 | – | 118 | 48 |
| `ctc` | 93 | 0 | 52 | 190 | 58 |
| `ctc2` | 77 | 0 | 35 | 209 | 56 |
| `allc`, 9 November | 292 | 292 | – | 3 | 5 |
| `allc` | 60 | 0 | 25 | 215 | 60 |

- **With every controller enabled** (`allc`), capacity probed back on both hosts, to 136 and 145 calls a second, and reached 100 a second 76 s and 39 s after the store recovered, with no pause longer than 5 s. Every workflow completed, 103 s into the drain, and from 240 s to 390 s, 5.8 workflows closed a second, where 0.7 did on 9 November. The task rate took no `no-data` step after the recovery, against 292 of 300 then, so on 9 November it was the task rate's hold, rather than the other controllers, that kept the probes back.
- **With capacity and the task rate** (`ctc` and `ctc2`), the probes went through in both runs. On the host with 11 shards, the longest pause fell from 51 s to 14 s and 6 s, and capacity reached 100 calls a second 67 s and 42 s after the recovery, against 120 s. On the other host, it took 89 s and 79 s, against 85 s. Both runs completed every workflow, 154 s and 136 s into the drain, against 144 s, with a p99 of 419 s and 403 s, against 410 s: once capacity reached about 100 calls a second, its queued cuts held it around there, as on 9 November ([finding 7](#what-they-found)).
- **At its floor, the task rate can still pause the probes.** In `ctc`, on the host with 21 shards, capacity held at 46 calls a second as `idle` from 243 s to 279 s. Following capacity with the mix it measured, 0.13–0.24 tasks a call, the task rate stayed at 6–10 tasks a second, at or near its floor of 6, and history offered 18–37 calls a second, under the 37.3 that `CoupledBindingShare` needs at 46. A step that offered 38 let capacity probe at 279 s, and the task rate followed. In `ctc2`, the same host paused for no more than 9 s.
- **Without the coupled settings** (`ct`), capacity held its cut again, as `idle`, and rose no further than 46 and 49 calls a second during the load, against 38 and 44 on 9 November. The task rate moved further, to 16 and 10 tasks a second by 390 s, against 7 and 6, and 555 workflows were still running after the drain, against 991. In `tools/adaptivesim`'s `coupled-slow-store`, every workflow completes without either setting since #119, so on this point the simulator doesn't match DSQL.
- **As the scheduler's backlog cleared, the task rate rose to its maximum.** On the host with 11 shards in `ctc`, `ctc2` and `allc`, task attempts outran calls, up to 3.2–4.6 attempts a call, so the task rate rose 25% a step to its maximum of 300 for 1–3 steps, then fell back with the mix. Meanwhile the scheduler's coupled refusals fell to none, calls stayed under capacity's rate, and capacity's limit rejected none. In 9 November's `ctc`, the same rise came in the drain, where the rate then stayed at 300.
- `shadow` matched 9 November's: capacity's shadow proposals held at 29 and 20 calls a second after the recovery, against 26 and 23, and 8.4 workflows closed a second from 240 s to 390 s, against 8.8. The pool gate refused 4,243 calls in `shadow` and 66–107 in the enabled configs, and the servers logged no throttling error from DSQL.
- With `CoupledRetry` at 200 ms, the task scheduler's throttled submissions were 161,129 in `ctc`, 158,907 in `ctc2` and 153,781 in `allc`, against 172,592 and 234,097 on 9 November.

### What it found

1. **Moving the mix on few calls lets the coupled probes through on DSQL.** With every controller enabled, capacity probed back and every workflow completed, where 847 were still running after the drain on 9 November. With capacity and the task rate, the longest pause on the host with 11 shards fell from 51 s to 14 s and 6 s.
2. **The coupled settings are still needed.** Without them, capacity held its cut after the store recovered, and 555 workflows were still running after the drain, though the simulator's `coupled-slow-store` completes without them.
3. **A task rate at its floor can still pause the probes**, for 36 s in one of the two `ctc` runs. At its floor, the task rate doesn't follow capacity's probes, and there history's calls stayed under `CoupledBindingShare` of capacity's rate until a step's calls passed it.
4. **Once capacity has probed back, its queued cuts set the drain's pace.** `ctc` and `ctc2` took about as long to drain as 9 November's `ctc`, with about the same p99.

### What it doesn't cover

- **Two runs of `ctc`, and one of each other config.** The floor's pause came in one of the two `ctc` runs.
- **Other rates and loads.** The floor's pause depends on the task rate's floor, 2% of the static rate, and on the mix. This rig's static rate of 300 and its workflows put the floor close to the rate capacity's probes needed.
