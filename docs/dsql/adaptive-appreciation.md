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
- **Load a laptop can't reach.** That's the last part of the appreciation, in [iw/tempdes](https://github.com/iw/tempdes), fed with the recorded traces. tempdes doesn't model the controllers yet.
- **One run per config.** Each config ran once, so a difference smaller than the one between a scenario's `shadow` and `shadow2` runs is noise.
- **Disabled.** Outside the baseline, `shadow` stood in for disabled.
- **One machine.** The four processes shared one laptop's CPU and memory, and one network path to the cluster.
- **Five minutes of load.** The memory budget and the partition scaler move over minutes, so the runs show their first steps more than where they settle.
- **Growth.** Only the short pool's schedulers queued, and there the worker controllers held while persistence backed off. No task queue's add rate reached 50 a second. So the worker controllers never grew workers, and the partition scaler ran no trials.
