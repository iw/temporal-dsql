# Temporal 1.32: from dynamic config to adaptive behaviour

2026-10-27 · Ian Wilkinson

## Summary

Temporal 1.32 does not need adaptivity invented; it needs an adaptive plane. The server already contains at least seven adaptive mechanisms, but each has its own sensor, thresholds held in dynamic config, and actuator. Most are off by default, and none share signals.

The proposal is to keep those mechanisms but change what dynamic config means. Today a setting is the value; in the target architecture it is the envelope around a value a controller computes from live signals: mode, bounds, step limits and kill switches.

Seven changes deliver this, each routed through existing seams in the code:

1. Dynamic config becomes an envelope, via a new `common/adaptive` package that emits the same function types config fields already use.
2. A per-host signal plane shares persistence, queueing, RPC and runtime signals with every controller.
3. One persistence capacity controller replaces the per-priority health limiters.
4. Task scheduler worker counts follow queueing delay.
5. Cache and pending-task limits follow a memory budget.
6. The partition scaler learns per-partition capacity instead of being told it.
7. Fair-share quotas follow demand rather than host count.

Proposal 3 has been prototyped: a tested controller, a simulation against faithful models of today's limiters, and an opt-in patch. In simulation it cut API errors to 0.5–3.4% where static limits gave 29–33%, with one config across three databases. One holdout case still favoured a hand-tuned legacy limiter, so the evidence is promising rather than decisive.

Scope is temporalio/temporal v1.32.1 (tag af08a5e, 7 Oct 2026). Every claim about current behaviour cites a file and line in that tag; the appendix lists them.

## Background: the dynamic config surface

Version 1.32.1 declares 628 dynamic config settings in `common/dynamicconfig/constants.go`. Values reach the server through a pluggable `Client` (`GetValue(key)`, plus an optional `NotifyingClient` for push updates), resolved by `Collection` against constraint precedence: namespace, task queue, shard, task type, destination.

**How values are consumed matters more than how many there are.** Config struct fields are plain function types, not values:

- `TypedPropertyFn[T]` is `func() T` (`setting_gen.go:907`); the caller re-reads on every call.
- `TypedSubscribable[T]` is `func(callback func(T)) (T, cancel)` (`setting_gen.go:935`); the caller is pushed changes.

Consumers never know whether a value came from YAML. That is the seam this proposal relies on: any function with the right signature, including a controller's output, can stand in for a setting. History's config uses `.Get` for 328 settings and `.Subscribe` for 8; matching uses 102 and 6.

**Not every knob should adapt.** Settings fall into three kinds:

| Kind | Examples | Count (by name) | Belongs in |
|---|---|---|---|
| Intent and policy | Retention, history size limits, feature flags, search attribute limits | Most of the 628 | Dynamic config |
| Capacity guesses | `*RPS`, `*MaxQPS`, `*WorkerCount`, `*Concurrency`, cache `MaxSize*`, `*BatchSize` | About 57 rate ceilings, 15 worker or concurrency limits, 12 cache sizes, 10 batch sizes | Controllers, bounded by an envelope |
| Controller tuning | `*Threshold`, `*Interval`, step sizes | 6 thresholds, 43 intervals | The envelope |

Capacity guesses are where tuning hurts. Their right value depends on hardware, database and workload mix, which the server can measure and an operator can only estimate. Fourteen settings are documented as needing a restart or reload, so a wrong guess is also slow to correct.

## What 1.32 already does adaptively

Seven mechanisms already close a loop on a live signal; each is an island with its own sensor and its own static thresholds.

| Mechanism | Where | Signal → actuation | What is still a guess | Default |
|---|---|---|---|---|
| `goro.AdaptivePool` | `common/goro/adaptive_pool.go` | Time to hand off work → grow or shrink goroutines | Min, max and target delay, hardcoded at each call site | Used only by the finalizer and deadlock detector |
| `HealthRequestRateLimiterImpl` | `common/persistence/client/health_request_rate_limiter.go` | Average persistence latency and error ratio → rate multiplier | Latency threshold in ms, step sizes, bounds | Off; thresholds 0; bounds 0.8–1.0 |
| Queue monitor and mitigator | `service/history/queues/monitor.go`, `mitigator.go` | Pending tasks, stuck readers, slice count → split and unload slices | `QueuePendingTaskCriticalCount` 9000 and other critical counts | On |
| `PartitionScaler` | `service/matching/partition_scaler_interface.go` | Task add rate and backlog → partition count, persisted in user data | Per-partition `TargetRate` | Off (`Enabled: false`) |
| Fairness auto-enable | `service/matching/task_queue_partition_manager.go:211, :525` | Fairness or priority keys seen → matcher mode, persisted | Nothing; it is a state switch | Off (`MatchingAutoEnableV2`) |
| Ownership-aware quotas | `service/history/shard/ownership_based_quota_scaler.go` | Shards owned → share of the cluster quota | The cluster totals | On |
| Poller scaling hints | `service/matching/physical_task_queue_manager.go:880` | Backlog age, add-to-dispatch ratio → hint to SDK pollers | Wait and backlog-age thresholds | On |

Two of these are templates for the rest. Fairness auto-enable observes traffic at the root partition, rate-limits itself to one change a minute, persists the decision in task queue user data, and every partition derives its effective config from dynamic config plus that state (`computeEffectiveConfig`). The partition scaler already has shadow mode, hysteresis, change-rate limits and propagation to clients through a gRPC trailer.

## Diagnosis: why tuning hurts

Five structural problems make capacity knobs painful. Each is visible in the 1.32.1 source.

**1. Sensors exist but don't reach controllers.** `HealthSignalAggregator` keeps a windowed t-digest and exposes `LatencyQuantile`, yet the health limiter's `refreshRate()` (`health_request_rate_limiter.go:109`) uses `AverageLatency()`. The average sits in a 5000-sample buffer, so at 3000 ops/s the nominal 10s window is about 1.7s. The quantile, when enabled, merges all ten 5s windows: a 50s signal, too slow for control. Meanwhile `executable.go:257` measures `scheduleLatency`, the exact queueing delay a worker pool should be sized on, and only emits it as a metric.

**2. Controllers don't coordinate.** The history task scheduler falls back to persistence QPS as a static number (`scheduler_quotas.go`; TODO at `queue_factory_base.go:168`). When the persistence limiter brakes, the scheduler keeps dispatching, tasks fail with `ResourceExhausted`, and the resubmit and rescheduler path absorbs it (`resourceExhaustedResubmitMaxAttempts = 1`). Wasted work becomes the control mechanism. Each priority also runs its own health limiter on its own ticker (TODO at `persistence/client/quotas.go:238`).

**3. Headroom above the static value is structurally impossible.** `NewPriorityRateLimiter` chains the dynamic limiter and a basic static limiter at `hostMaxQPS` through `MultiRequestRateLimiter`, and every chained limiter must allow. Raising `RateMultiMax` above 1.0 therefore has no effect. The default bounds of 0.8–1.0 also mean the limiter can cut at most 20%, too little for a real degradation.

**4. Fair share assumes uniform load.** `getQuota` (`cluster_aware_quota_calculator.go:34`) returns `clusterLimit / clusterSize`. Long-lived gRPC connections skew traffic, so a namespace landing on 3 of 12 frontends gets about 25% of its global quota.

**5. Nothing knows how much memory it has.** `HistoryCacheHostLevelMaxSizeBytes` is fixed and needs a restart, and `cache.New(maxSize)` (`lru.go:137`) cannot resize. Nothing reads `GOMEMLIMIT` or the cgroup memory limit. `QueuePendingTaskCriticalCount` (9000) is a memory bound expressed as a task count.

**A correction from implementation.** An earlier draft claimed per-priority limiters cut operator and background traffic alike. They don't: `PriorityRateLimiterImpl` decides each request with its own bucket and charges every lower-priority bucket, so background work is already shed first. Priority-ordered shedding is a property to preserve, not a gap to fill.

## Target architecture

![target architecture · 4 layers, 7 proposals](adaptive-behaviour/target-architecture.svg)

Sensors feed one host-local snapshot; controllers act only inside the envelope dynamic config sets, and mostly drive actuators that already exist.

Four principles shape every proposal:

- **Demote, don't delete.** Each capacity setting survives as the fallback value and as a bound.
- **One signal, many controllers.** Controllers share a snapshot instead of each keeping private averages.
- **Reuse actuators.** Live resize, `SetRPS` and persisted user data already exist; new code goes into deciding, not doing.
- **Separate timescales.** Fast inner loops settle before slow outer loops act (see Safety, operability and testing).

## Proposal 1: dynamic config becomes an envelope

A new `common/adaptive` package returns the same function types config fields already hold, so no consumer changes. The old setting becomes the fallback value; a new envelope setting governs the controller.

```go
// common/adaptive (sketch)
type Mode int
const (
	ModeOff    Mode = iota // exactly today's static value
	ModeShadow             // controller runs and reports; static value applied
	ModeOn                 // controller output applied, clamped to bounds
)

type Envelope[T Number] struct {
	Mode        Mode
	Min, Max    T
	MaxStepFrac float64 // largest relative change per tick
	Interval    time.Duration
}

type Controller[T Number] interface {
	Next(current T, s signals.Snapshot) (T, Reason) // pure, so testable
}

func Subscribable[T Number](
	static dynamicconfig.TypedSubscribable[T],
	env dynamicconfig.TypedPropertyFn[Envelope[T]],
	ctrl Controller[T], plane signals.Plane,
	ts clock.TimeSource, mh metrics.Handler,
) dynamicconfig.TypedSubscribable[T]
```

Wiring happens at the factory, not in `configs.NewConfig`, so config stays pure. For example, `transfer_queue_factory.go:55` passes `WorkerCount: params.Config.TransferProcessorSchedulerWorkerCount` to the scheduler; it would pass the wrapped subscribable instead, with the signal plane injected through fx. `FIFOScheduler` already resizes live when that subscription fires (`fifo_scheduler.go:64, :111`).

Every envelope field has a precedent in 1.32:

| Envelope field | Existing precedent |
|---|---|
| Shadow mode | `PartitionScaleManagerSettings.ShadowModeLogInterval` |
| Step limit | `PartitionScaleManagerSettings.MaxRate` |
| Bounds | `Min` / `Max` in `SimplePartitionScalerSettings` |
| Hysteresis | The Ups / Downs deadband in the same struct |
| Explainable decisions | `recordPollerScaleDecision` with reason tags |
| Staged rollout | `RolloutAccepts` and `GradualChange` in `common/dynamicconfig` |

`ModeOff` must be bit-identical to today's behaviour, so the default is safe to ship.

On this branch, `common/adaptive` holds the modes, `RolloutPercent`, the freeze key, the decisions metric and a generic `Envelope` with bounds and a step limit. The persistence capacity controller uses the first four. It keeps its own envelope, because its bounds are multiples of the static rate and it steps differently for cuts and probes. The worker counts in Proposal 4 add `Subscribable`, as `signals.Drive`.

## Proposal 2: a per-host signal plane

Generalise `persistence.HealthSignalAggregator` into a `common/signals` package that every controller on a host reads. It publishes one immutable snapshot per tick, so controllers see a consistent view and can be tested deterministically with a fake `clock.TimeSource`.

| Source | Where it comes from | Change needed |
|---|---|---|
| Persistence latency, errors, throughput | `persistence_metric_clients.go` already calls `Record` per request | Recent-window quantile and per-window counters (done in the Phase 1 patch) |
| Task queueing delay | `scheduleLatency` in `executable.go:257` | One `Record` call beside the metric at line 357 (done on this branch, as `RecordScheduleLatency`) |
| RPC handler latency | `rpc/interceptor/health_check.go` | Feed the existing aggregator into the plane |
| Go scheduler and heap | `runtime/metrics`: `/sched/latencies:seconds`, `/gc/heap/goal:bytes` | New source |
| Memory limit | cgroup `memory.max`, `GOMEMLIMIT` | New source |

The building blocks exist: `stats.NewWindowedTDigest` and `aggregate.MovingWindowAverage`. Two fixes matter for control. The quantile must cover only the newest windows, so the Phase 1 patch adds `QuantileRecent(q, n, now)`. The latency window should be 1s rather than the default 5s.

**Keep the plane host-local.** Ringpop labels look like a free gossip channel, but a label change flips `labelsChecksum` and raises a `HostsChanged` membership event (`service_resolver.go:457`). Shard ownership listeners react to that, so churning a load label would churn membership. Cross-host demand belongs in Proposal 7's slower, dedicated exchange.

On this branch, `common/signals` is the plane. Each service has one, off by default. While the `adaptive.signals` setting enables it, it takes a snapshot every `Interval`, 1s by default:

- persistence health over the newest `Windows` latency windows, 3 by default, with the p90 of task queueing delay on history hosts;
- on history hosts, the p90 of how long shard operations waited for their shard's I/O semaphore, which lets `history.shardIOConcurrency` of a shard's writes reach persistence at once. No controller reads it yet; traces record it, so a run can show whether history's per-shard writes, rather than the store, hold back its throughput;
- history's RPC handler latency and error ratio over its health check aggregator's windows: the mean, and the p99 when `system.historyHealthSignalUsePercentiles` is set, which also makes history's deep health check use the p99;
- the Go scheduler's p99 wait since the previous snapshot, goroutines, live heap and heap goal;
- the memory limit and use of the process's cgroup, v1 or v2, and `GOMEMLIMIT`.

Controllers report each step to the plane through `adaptive.Report`, and the next snapshot carries each controller's latest decision. The persistence capacity controller reports its steps, with the envelope and signals it stepped on. It still reads the persistence aggregator itself, on its own step.

With `TraceDirectory` set, each service writes its snapshots and decisions to `<service>-<host>-<pid>.jsonl` in that directory, up to `TraceMaxBytes`, 1 GiB by default. Each line is one JSON object: a header that starts the file, a snapshot or a decision. Durations are in nanoseconds. The traces feed the [offline replay](#offline-replay) and the [system appreciation](adaptive-appreciation.md).

```yaml
adaptive.signals:
  - value:
      Enabled: true
      TraceDirectory: /var/tmp/temporal-signals
```

## Proposal 3: one persistence capacity controller

Replace the per-priority health limiters with one controller per host that computes an admitted rate C. All seven priority buckets take C (the operator bucket C × `operatorRPSRatio`), so the existing cascade still sheds background work first. When the controller is on, the basic static limiter becomes a safety ceiling at `MaxMultiplier × hostMaxQPS`, which removes the structural cap described in the diagnosis.

The control law has no latency threshold in milliseconds. It borrows from TCP BBR and Cubic:

1. **Target.** Learn a baseline as the 10-minute minimum of p99; aim for p99 at baseline × 2, capped at an absolute ceiling (750 ms) that guards against slow baseline drift.
2. **Cut on overload.** When p99 exceeds the target or unhealthy errors exceed 1%, cut to 90% of the throughput the database actually delivered. Never cut more than half in one step.
3. **Act once per signal.** The cooldown between cuts is at least twice the signal span, so the controller never acts twice on the same stale data.
4. **Probe only when binding.** Increase only while the limiter is rejecting requests, so the limit cannot inflate while idle. Near the last overload point, start at 0.5% per step and ramp to 5% over 30s. On this branch, the task scheduler's limit, which is coupled to the controller's rate, can bind for it too, as *Count the task scheduler's refusals* below describes.

The Phase 1 patch implements this as an opt-in mode of the existing struct-valued setting, so it reaches history, matching, frontend and worker with no new wiring:

```yaml
system.persistenceHealthSignalPercentilesEnabled:
  - value: true
system.persistenceHealthSignalLatencyWindowSize:
  - value: 1s
history.persistenceDynamicRateLimitingParams:
  - value:
      Enabled: true
      Capacity:
        Mode: shadow   # disabled | shadow | enabled
        RolloutPercent: 100   # with Mode: enabled, the share of hosts that apply it
```

**Return to what the store delivered, and say when the baseline moves.** On this branch, two additions answer the first two [risks](#risks-and-open-questions):

- **After a cut.** During a timeout storm, the throughput the store delivers collapses, so a cut based on it can go well below what the store can do. For `DeliveryWindow`, 30s, after a cut, the controller keeps the most the store delivered over signal windows that start after the cut, as BBR's max-filter keeps a path's bandwidth. Once the store is healthy and the limit binds, the rate returns at once to `DeliveryBackoff`, 90%, of that, never above the rate before the cut, and probes slowly from there. The step's reason is `recover`, and the limiter logs it.
- **The baseline.** A store that slows down raises the learned baseline, and the target with it, up to `LatencyCeiling`. The controller settles on the first baseline it learns over a whole `BaselineWindow`. When the baseline moves by `BaselineAlert`, 2 times, up or down, the limiter logs a warning and counts a `baseline-up` or `baseline-down` decision, and the controller measures from the new baseline. A gap in the samples longer than the window starts the settling again.

Setting either to 0 turns it off:

```yaml
history.persistenceDynamicRateLimitingParams:
  - value:
      Enabled: true
      Capacity:
        Mode: shadow
        DeliveryWindow: 30s
        BaselineAlert: 2
```

**Couple the task scheduler to it next.** `QueueSchedulerRateLimiterProvider` should derive the scheduler's host rate from the controller's admitted rate divided by measured persistence calls per task, instead of a static fallback. Tasks then wait in the scheduler, which is cheap, instead of failing mid-execution and bouncing through the rescheduler.

On this branch, `common/adaptive/taskrate` does this:

- **The rate** is the persistence rate times `Headroom`, 0.9, times the task attempts per persistence call that the persistence snapshot measures. API calls count among the calls, so they keep their share of the persistence rate.
- **The persistence rate** is the capacity controller's while it's enabled and its decision is less than `DecisionHold`, 10s, old. Otherwise it's the static limit.
- **Bounds.** The rate stays between `MinMultiplier`, 2%, of the static persistence rate and the scheduler's fallback, which is that rate. It moves at most `MaxStepFrac`, 25%, in a step.
- **Smoothing.** The rate keeps the attempts per call it was set from while the measured mix stays within `Deadband`, a tenth, of it, with the reason `held`. It still follows the persistence rate on every step, so the capacity controller's cuts and probes reach the scheduler as before. Averaging the mix over the newest steps as well was tried in `tools/adaptivesim`: it lags a falling mix, which keeps the rate too high, and it doubled the workflows' failures in the slow store. A trace recorded before the deadband replays as recorded, with `Deadband` at 0.
- **How it limits.** The scheduler's own limiter refreshes its rate once a minute. So while the controller is enabled, it limits the scheduler with its own buckets, on top of the scheduler's limiter. Disabled, it passes every request to the scheduler's limiter.

It acts only where the scheduler's rate limiter is on and out of shadow mode:

```yaml
history.taskSchedulerEnableRateLimiter:
  - value: true
history.taskSchedulerEnableRateLimiterShadowMode:
  - value: false
history.taskSchedulerRateController:
  - value:
      Mode: enabled
```

**Count the task scheduler's refusals.** With both enabled, the task scheduler rate keeps persistence's calls at its `Headroom`, 90%, of the capacity controller's rate. Once the scheduler's limit binds, the persistence limiter denies nothing, so after a cut the controller holds its rate as `idle` while tasks wait in the scheduler. In the simulator's slow store, capacity held its cut for the rest of the load after the store recovered, and 7,224 tasks were waiting when the load ended. On this branch, `CoupledBindingShare` counts the scheduler's refusals as the controller's limit binding:

- **The count.** The task scheduler's limiter records each task its coupled buckets hold back, refused or made to wait, while their rate is below the static rate. It records them in the persistence health signals, beside the task attempts, with the `Headroom` it coupled at. At the static rate, the task rate's ceiling, a higher persistence rate wouldn't let more tasks through, and the scheduler's own limit's refusals aren't the coupling's, so neither counts.
- **The law.** The controller counts the refusals as its limit binding while the calls offered, made and rejected, reach `CoupledBindingShare` of the calls the scheduler lets through: `Headroom` times the controller's rate. Calls offered well below that say that something other than the rate held the tasks back, such as a mix of calls the task rate hasn't caught up with yet. The store's facts and latency still cut as before.
- **Replay.** Each decision's detail carries the refusals, their `Headroom`, the rejected calls and the share, so a trace replays the step, and a replay can set the share on a trace recorded with it at 0.
- **Off by default.** At 0, its default, the controller ignores the refusals, so enabling both controllers keeps the behaviour above until it's set. It applies in shadow mode too, so a shadow run shows the probes.

```yaml
history.persistenceDynamicRateLimitingParams:
  - value:
      Enabled: true
      Capacity:
        Mode: enabled
        CoupledBindingShare: 0.9
```

In `tools/adaptivesim`, at 0.9, the slow store's capacity probes from about 100 a second to about 200 within 35 s of the store recovering, 1,513 tasks are waiting when the load ends, and all but 9 of the 19,200 complete by the end of the drain. The task rate still follows each cut while the store is slow. After the recovery, the queued share cuts the rate three times during the load as the rescheduler's retries release the waiting tasks in batches. Through a surge, capacity probes to about half of its ceiling. Where the task rate's step limit lags a surge of tasks while the calls stay far below capacity's rate, the calls offered don't reach the share, and capacity stays at its static rate. [The simulator's README](../../tools/adaptivesim/README.md#what-the-runs-show) has the runs.

**Retry the coupling's refusals sooner.** History's rescheduler waits 2 s ± 1 s after the scheduler refuses a task, which suits a busy scheduler. The coupled buckets hold 2 s of their rate, so after a coupled refusal that wait leaves them to fill and then release tasks in batches. On this branch, `CoupledRetry` sets how soon the rescheduler retries after the coupled buckets refuse a task below the static rate, jittered by half either way. The scheduler's other refusals, a full task channel or its own limit, keep the 2 s. Each retry the buckets refuse costs a `TrySubmit`, a throttle metric and a counted refusal, so it's off by default, at 0:

```yaml
history.taskSchedulerRateController:
  - value:
      Mode: enabled
      CoupledRetry: 200ms
```

In `tools/adaptivesim`'s slow store, with `CoupledBindingShare` at 0.9, a `CoupledRetry` of 200ms leaves 16 tasks waiting when the load ends, against 1,513 with the share alone, and every task completes. In the other scenarios, the outcomes stay as without it.

**On DSQL, hear the plugin's facts.** The design for passing the DSQL plugin's throttling, contention and pool pressure to the controller, rather than inferring the store's health from latency, is in [Store facts for the capacity controller](adaptive-store-facts.md).

## Proposal 4: worker counts follow queueing delay

The `*ProcessorSchedulerWorkerCount` settings (transfer, timer, memory timer, visibility, archival, replication, low-priority replication) become controller outputs with the old values as their fallback. The actuator already exists: `FIFOScheduler.updateWorkerCount` resizes the pool live from a `TypedSubscribable[int]`.

The signal is `scheduleLatency`. Its start time is set when the reader submits a task (`reader.go:525`) or the rescheduler resubmits it (`rescheduler.go:230`), so it measures pure wait inside the scheduler.

The control law is `goro.AdaptivePool`'s, lifted into a controller:

- **Grow** when p90 `scheduleLatency` exceeds the target and the persistence controller is not backing off.
- **Shrink** slowly when workers sit idle, with jitter so the pool shrinks slower than it grows.

The backoff guard is the important part. Adding workers while persistence is the bottleneck only manufactures more `ResourceExhausted` errors, which `executable.go` then resubmits.

On this branch, `common/adaptive/workers` drives the worker counts of history's transfer, timer, visibility and archival queues. The memory timer and replication queues record no queueing delay, so they keep their settings. Each queue's controller steps on the signal plane's snapshots, which carry each queue's p90 delay over the newest `Windows` seconds:

- **Grow** by `GrowFrac`, 25%, while the p90 delay is above `TargetDelay`, 100 ms, at most once per `Cooldown`, 3s.
- **Back off** while persistence is backing off: hold, or return to the configured count if the pool grew. Persistence is backing off while any of these holds:
  - the capacity controller found the store overloaded in the last `BackoffHold`, 10s;
  - errors, throttled calls and the persistence rate limiter's rejections pass `MaxErrorRatio`, 1%, of calls;
  - more than `MaxQueuedRatio`, 25%, of the store's connection acquires waited.
- **Shrink** by `ShrinkFrac`, 5%, after `IdleSteps`, 30, idle steps in a row. A step is idle when the delay is under a quarter of the target or covers fewer than `MinSamples`, 20, tasks.
- **Bounds** are `MinMultiplier`, 0.5, and `MaxMultiplier`, 2, of each queue's worker count setting.

`signals.Drive` turns a controller into a subscribable setting. While the controller is disabled, the scheduler gets its setting's value as before. In shadow, the controller reports what it would do. Enabled, it applies its proposals within the envelope. A controller in shadow or enabled runs the plane even when `adaptive.signals` doesn't enable it. The backoff guard reads persistence health, so it needs `system.persistenceHealthSignalPercentilesEnabled`.

```yaml
history.workerCountController:
  - value:
      Mode: shadow
```

## Proposal 5: a memory budget instead of counts and bytes

Read the cgroup `memory.max` or `GOMEMLIMIT` at startup and divide it into budgets: the history cache, the events cache, and pending tasks held in memory by queue readers. The settings that hold those sizes today become fractions of the budget, with the old absolute values as caps.

Three code changes are needed:

1. **Make caches resizable.** Add `SetMaxSize` to `common/cache/lru.go`, evicting on shrink. Today `New(maxSize)` fixes the size for the cache's life, which is why `HistoryCacheHostLevelMaxSizeBytes` needs a restart.
2. **Derive the pending-task limit.** Compute `QueuePendingTaskCriticalCount` from the budget divided by observed bytes per executable. The monitor reads it through an `IntPropertyFn` (`monitor.go:130`), so the swap is local. The mitigator's `targetLoadFactor` of 0.8 stays as it is.
3. **Resize on a slow loop.** Re-evaluate every few minutes from heap goal and live heap in `runtime/metrics`, so caches give way before the garbage collector thrashes.

This turns a question operators cannot answer well ("how many bytes of cache for this pod?") into one they can ("what share of this pod's memory is cache?").

On this branch, `common/adaptive/memory` sizes history's two host-level caches, and the tasks its queues hold, from the process's memory limit, the smaller of its cgroup's limit and `GOMEMLIMIT`. Each controller steps on the signal plane's snapshots. A cache's controller acts on a cache limited in bytes: the history cache while `history.cacheSizeBasedLimit` is set, and the events cache while `history.enableHostLevelEventsCache` is. A cache limited by entry count keeps its setting, as do the shard-level events caches.

- **Budget.** Each cache's budget is its share of the limit, `HistoryCacheShare`, 0.3, or `EventsCacheShare`, 0.1, capped at its size setting. A cache over its budget moves to it.
- **Pressure** is how close the heap is to the most the limit allows. Under `GOMEMLIMIT`, the runtime holds the heap goal under the limit, so the pressure is the live heap's share of the goal: a half at the default `GOGC`, and near 1 as collections run back to back. A cgroup limit alone is unknown to the runtime, so there the pressure is the heap goal's share of the limit.
- **Shrink** by `ShrinkFrac`, 25%, while the pressure is above `HighWater`, 0.85, at most once per `Interval`, a minute.
- **Grow** back towards the budget by `GrowFrac`, 10%, while the pressure is below `LowWater`, 0.6, at most once per `Interval`.
- **Bounds** are `MinMultiplier`, 0.05, of each cache's size setting, and the setting itself.

The queues' pending-task limits follow the same law, as one multiplier of their settings, at most 1:

- **What the queues may hold.** Each shard has its own queues, so at their settings the queues may hold the host's shard count, from the shard controller, times the sum of each queue's max count. That's `history.queuePendingTasksMaxCount` for the transfer, timer, visibility and archival queues, and `history.outboundQueuePendingTasksMaxCount` for the outbound queue.
- **Budget.** The multiplier's budget is the share of that which `PendingTaskShare`, 0.2 of the limit, holds at `BytesPerTask` each.
- **Limits.** Each queue's max and critical counts are its settings times the multiplier, so they keep the settings' ratio: the mitigator starts unloading tasks a little below the count at which a queue stops loading them. The mitigator's `targetLoadFactor` of 0.8 stays as it is.
- **Floor.** The multiplier stays at `MinMultiplier` or more, and never takes a queue's critical count below 1,250. A queue's readers after its first stop loading at 0.8 of the critical count, or less, but at no fewer than 1,000 tasks. With a critical count of 1,000 or less, they load past it, and the mitigator unloads their tasks over and over, as a smoke run showed. If a queue's critical count is set below 1,250, the queues keep their settings. With many shards on a host, the floor can leave what the queues may hold above their budget: 512 shards, each with five queues of 1,389 tasks at 2 KiB, may hold 6.8 GiB.
- **Bytes per task.** Go's runtime doesn't measure the heap by type, so `BytesPerTask` is a setting, 2 KiB. A pending task is the task, its executable and its entry in its slice's tracker. It measured 1.5 KiB with OpenTelemetry's metrics handler and 0.9 KiB with tally's. A test in `service/history/queues` fails if it grows past the setting.

`cache.Resizable` lets the LRU change its size limit while it runs. Shrinking evicts the least recently used entries that aren't pinned, and a pinned entry goes once it's released. `signals.DriveFunc` drives each cache's limit from the size it was created with, so while the controller is disabled the cache keeps that size. The two cache settings still take effect at startup. The multiplier's static value is 1, so while the controller is disabled the queues keep their settings.

```yaml
history.cacheSizeBasedLimit:
  - value: true
history.enableHostLevelEventsCache:
  - value: true
history.memoryBudgetController:
  - value:
      Mode: shadow
```

## Proposal 6: the partition scaler learns capacity

Matching's partition scaler already has the hard parts: persisted `PartitionScaleState`, propagation to callers through the partition-count gRPC trailer (`sendPartitionCountTrailer`), drain handling, shadow mode and hysteresis. What it lacks is a saturation signal. `PartitionScalerInput` carries only `NumTasks`, `CurrentTarget`, `BacklogCounts` and `PrivateState`, so `SimplePartitionScalerThreshold.TargetRate` (tasks per second per partition) is an operator's capacity guess.

Three changes close the gap:

1. **Enrich the input.** Add sync-match ratio, backlog age, and add rate against dispatch rate. Matching already computes these for poller scaling via `GetPhysicalQueueAdjustedStats`. Add the owning host's persistence latency from the signal plane.
2. **Learn capacity per partition.** When add rate exceeds dispatch rate and backlog age grows while the host is healthy, record the observed dispatch rate as the partition's capacity in the `anypb` `PrivateState`, which exists for exactly this. Target partitions = add rate ÷ learned capacity, with headroom.
3. **Expose the factory.** `PartitionScalerFactory` is provided through fx (`matching/fx.go:268`) but absent from `temporal/server_option.go`. A `WithPartitionScalerFactory` option lets operators experiment without forking (done on this branch).

The proto already reserves room for more learned state: `task_queues.proto:147` reads "For future use: description, rate limits, manual partition control, etc."

On this branch, `common/adaptive/partitions` learns each task queue's capacity, the tasks a second one partition dispatches, and sizes its partitions from it. Matching's default partition scaler factory gives each root partition a scaler built around the simple scaler, which decides while the controller is disabled. A factory set with `WithPartitionScalerFactory` replaces it.

- **Input.** For a scaler that asks for them, the scale manager describes the task queue's partitions every `BackgroundInterval`, 23s, with their stats. It passes their sums: the add and dispatch rates over the partitions' last 30 seconds, the backlog count and its oldest age, and whether a partition's dispatch is rate limited. While managed scaling is off, it describes the write partitions dynamic config gives. The partitions the controller sizes are managed scaling's target while it's on, and those write partitions while it's off. Persistence health comes from the signal plane, by the rule history's worker counts use, rather than from latency.
- **Size.** The partitions grow to dispatch the add rate at the capacity with `Headroom`, 20%, to spare. They shrink while they'd keep twice that, but only `DownHold`, 5 minutes, after the task queue was last behind or they last changed. A step changes them by at most `MaxStepFrac`, 25%, or one partition, within `MinPartitions`, 1, and `MaxPartitions`, 64.
- **Settle.** The partitions grow, the capacity learns from their rates, and a trial is judged, only `Settle`, 30s, after the partitions last changed, or the partitions described did. Each partition's rates cover its last 30 seconds, so until then the partitions there before the change still count the tasks they took then. The sum reads high while the partitions grow, and low once a partition that drained stops being described. Without the wait, a smoke run grew a task queue to 13 partitions where 10 were enough. Waiting only for the partitions tasks are added to, a trial that started just after a partition drained took the recovery from that low reading for a gain.
- **Capacity** starts at `InitialCapacity`, 1,000 tasks a second, and rises to what the partitions dispatch whenever they dispatch more without falling behind.
- **Behind.** A task queue is behind while its add rate passes its dispatch rate by `BehindRatio`, 10%, at an add rate of `MinRate`, 50 tasks a second, or more.
- **Trials.** When a task queue is behind on two steps in a row, and the capacity says its partitions are enough, the controller adds a step of partitions and judges it after `TrialWindow`, 2 minutes. A trial starts from the highest settled dispatch rate since the partitions last changed. If dispatch rose from it by `MinGain`, half, of the rise in proportion to the partitions added, the partitions were the limit, and that rate per partition is the capacity. Otherwise, if the task queue is still behind, the controller undoes the step. It then runs no trials for `NoGainHold`, 10 minutes, which doubles with each such trial in a row, up to 80 minutes.
- **No trials** while a partition's dispatch is rate limited, while persistence is backing off, or while fewer than `MinRate` tasks a second are dispatched. While persistence is backing off, the partitions don't grow at all.

The proposal above records the dispatch rate as the capacity whenever adds outrun dispatch. Adds also outrun dispatch when the task queue's workers hold it back. Each capacity recorded then would be lower than the last, and the partitions would grow to the maximum without helping. A trial tells the two apart, because more partitions raise dispatch only when the partitions were the limit.

The controller keeps its capacity in the scaler's private state, so a task queue's root partition loads with it. While the controller is disabled, the simple scaler decides, and the scale manager describes nothing more than before. In shadow, the simple scaler's decisions apply, and the controller reports what it would do. It runs no trials there, since its proposals can't change dispatch. Enabled, it sets the partitions through managed scaling, in place of `matching.partitionScaler`. `RolloutPercent` is the share of task queues, picked by name, that apply an enabled mode. As for history's worker counts, persistence health needs `system.persistenceHealthSignalPercentilesEnabled`.

Turning managed scaling off is a clean break to dynamic config, which leaves the tasks in partitions beyond its count unpolled until they time out. So when the controller stops applying after it added partitions beyond `matching.numTaskqueueWritePartitions`, it keeps managed scaling on at that count until those partitions drain, and then turns it off.

```yaml
matching.partitionScalerController:
  - value:
      Mode: shadow
    constraints:
      namespace: my-namespace
```

## Proposal 7: fair share follows demand

This is the hardest change and should come last. Replace the `MemberCounter` dependency in `common/quotas/calculator` with a `ShareProvider` interface whose default implementation is today's `clusterLimit / clusterSize`, so behaviour is unchanged until a new provider is chosen.

A demand-weighted provider gives each host a share proportional to its recent per-namespace demand. That needs a low-frequency exchange of demand between frontends, on the order of seconds, through a dedicated internal RPC. It must not ride on ringpop labels, for the membership-churn reason in Proposal 2.

History's `OwnershipAwareQuotaCalculator` is the precedent: it already scales a host's share by the fraction of shards it owns, an observed quantity rather than a head count.

On this branch, `common/adaptive/fairshare` splits a namespace's limits across the cluster between frontends by their demand. The quota calculator takes an optional `ShareProvider` beside its `MemberCounter`, so every existing calculator is unchanged. Without a provider, or when the provider has no share for a namespace, the calculator divides the limit by the member count as before. The frontend's three namespace limits, `frontend.globalNamespaceRPS`, its `.visibility` limit and its `.namespaceReplicationInducingAPIs` limit, take their shares from the fair-share exchange, as do the internal frontend's.

- **Demand.** Each frontend counts the tokens each namespace's requests ask of each limit, admitted or not, and measures their rate over the last `Window`, 30s.
- **Exchange.** Every `Interval`, 5s, each frontend sends its rates to the other frontends that take requests, and each reply carries the other's latest rates. The exchange is its own gRPC service on the frontend's port, `temporal.server.adaptive.v1.FairShareService`, whose messages are `structpb.Struct`s, so there's no proto change, and nothing rides on ringpop labels. A frontend counts another's report for `StaleAfter`, 15s. A reply says how long ago its report was measured, so a frontend whose rounds stop goes stale.
- **Share.** A frontend keeps an equal share for each frontend whose demand it doesn't know, and the frontends it knows split the rest. Each keeps `MinShare`, 20%, of an equal share, so it can take requests that arrive before the next exchange, and the remainder follows their demand. While the demand with `Headroom`, 20%, fits the remainder, each gets its demand with headroom and an even part of what's spare. Otherwise each gets a part in proportion to its demand, so a request is as likely to be admitted at one frontend as at another. Without demand, each gets an equal share. When the frontends see the same demand, their shares add up to the limit. They agree within a round or two, rather than through consensus on the totals.
- **Deadband.** A frontend applies a higher share once it rises `Deadband`, 10% of an equal share, above the share it applies, and a lower share at once. Each limiter logs every change to its quota, so smaller rises would only fill the logs. A frontend never holds a share above the one its view of the demand gives it, so once the frontends see the same demand, their shares add up to at most the limit, and leave less than `Deadband` of it unused. While their views differ, as while the demand moves, the shares can still add up to more than the limit. A deadband both ways could hold them there: in the [system appreciation](adaptive-appreciation.md#uneven-frontends), two frontends set their shares from windows still filling, a round apart, and held them at 105% of the limit for the rest of the run.
- **Filling windows.** A namespace's first shares wait for the frontends' windows to fill. A frontend whose requests for a namespace start after one of its rounds measures them from that round, so their rate rises until the round leaves the window, and it reads its own rate fresher than the others' reports. Each report names the namespaces whose window still reaches back to before their first requests. Until no frontend's does, the frontends keep a namespace without shares at equal shares, with the reason `filling`. Then a share that falls applies at once, and one that rises waits a round more, so the shares that fall have fallen first. In `tools/adaptivesim`'s uneven frontends, the shares stay within the limit, where before they reached 79.7 a second against 60 while the windows filled, and they settle as soon as before, about 37 s after the requests start. Shares that already follow the demand don't go back to equal shares when another frontend's window starts to fill.
- **Lag.** Each limiter reads its rate once a minute, so a share takes up to a minute to apply. A frontend that starts to receive a namespace's requests while the namespace is over its limit holds them to its floor until then.

The authorizer sees the exchange as a cluster-scoped admin API, as it sees the admin service. Under the default authorizer, frontends need system admin claims to call each other; without them, the exchange is refused and each frontend keeps an equal share. A frontend calls the others as the system worker calls a frontend, and an internal frontend calls the others as the other services call it.

While the controller is disabled, nothing is counted or exchanged, and every limit takes an equal share. In shadow, frontends exchange their rates and report the shares they'd take, while the equal shares apply. `RolloutPercent` is the share of namespaces, picked by name, that apply an enabled mode, and the others run in shadow. `adaptive.frozen` holds the shares applied.

```yaml
frontend.fairShareController:
  - value:
      Mode: shadow
```

## Safety, operability and testing

Every controller ships disabled, is proven in shadow, and stays inside bounds an operator sets. The rules below apply to all seven proposals.

- **Three modes.** Disabled returns the static value exactly. Shadow runs the controller and reports proposals while the static value applies. Enabled applies the output, clamped. Shadow is open-loop: proposals never feel their own effect, so they show direction and timing, not a settled value.
- **Bounds and step limits.** Every output is clamped to a minimum and maximum and to a largest change per tick.
- **Staged rollout.** `RolloutAccepts` keyed on host or shard ID enables a controller on a stable, growing cohort. On this branch, the persistence controller has it as `Capacity.RolloutPercent`, keyed on the hostname.
- **One freeze key.** An `adaptive.frozen` setting holds every controller at its current value during an incident. Modes still change while it's set, so a controller can still be disabled. On this branch, the persistence controller honours it.
- **Explainable decisions.** Each decision carries a reason (latency, errors, cooldown, idle, probe), emitted as a metric tag and logged on decreases, as matching already does for poller scaling. On this branch, `adaptive_controller_decisions` counts every step by `controller` and `reason`. With the tally metrics framework, a reason's hyphens become underscores, so `baseline-up` reads `baseline_up`.

**Separate the timescales** so loops don't chase each other. Inner loops must settle before outer loops act on them.

| Loop | Step | Settles in |
|---|---|---|
| Worker concurrency | 1s | Seconds |
| Persistence capacity | 1s, at least 6s between cuts | Tens of seconds |
| Memory budget | Minutes | Minutes |
| Partition count | Rate-limited by `MaxRate` | Minutes |
| Demand-aware share | Seconds-scale exchange | Tens of seconds |

**Testing.** Controllers are pure functions of state, envelope and signals, so unit tests run on a fake clock. Recorded signal traces replay through them offline, as the next section describes. Interactions between controllers need a discrete-event simulator before any canary. `tools/adaptivesim` runs them closed-loop on simulated time, as [Closed-loop simulation](#closed-loop-simulation) describes.

## Offline replay

Each decision in a trace carries, as its detail, what its law stepped on: the parameters, the signals and, for the controllers the plane drives, the envelope's step limit. `tools/adaptivereplay` feeds those details back through the laws. With the parameters a trace recorded, it gives back the recorded proposals step for step, which shows the trace holds everything the laws read. With other parameters, it shows what they would have proposed on the same signals.

```bash
go run ./cmd/tools/adaptivereplay -set worker_count.TargetDelay=250ms /var/tmp/temporal-signals/history-*.jsonl
```

For each controller, the report gives its steps, how many replayed as recorded, how often the proposal changed and turned back, its range as a share of the static value, and its reasons. `-csv` writes every step. [The tool's README](../../tools/adaptivereplay/README.md) describes its flags and output.

**Keeping replay exact.** A law is a pure function of its state, parameters and signals, so a replay matches when it rebuilds the law's state as the host did. The trace records each time the host changed that state other than by a step:

- The plane resets a controller it drives to the static value on each tick while the controller is off, and records nothing then. A tick without the controller's decision means its law started again. A controller turned off keeps the plane ticking until a step has reset it. So a law that comes back on starts from the static value, even when nothing else kept the plane running.
- The persistence rate limiter starts its law at the static rate it reads as it starts, and holds the law at the static rate while the controller is off. The next decision records that rate as its detail's `Start`. A frozen decision has a detail only for `Start`.
- Matching makes a new partition scaler each time it loads a task queue. When the controller is first enabled for the task queue, the scaler starts its law again from the capacity the task queue's state kept. The scaler's first step after either is marked `Started`, with that capacity as `Loaded`.
- A frozen or held step keeps the value its law held, and the reason the host recorded.
- While the plane is enabled, it opens its trace as soon as the service creates it, before its first tick. So the trace has the decisions controllers make while the host starts.

Proposals match within a relative 1e-9. A law built where the compiler fuses multiply-adds can differ in the last bits from one built where it doesn't.

**Limits.**

- Replay is open-loop. A proposal under other parameters doesn't change the signals that follow. So a what-if shows direction and timing, as shadow mode does, but not where the system would have settled. Closing the loop needs the [discrete-event simulator](#closed-loop-simulation).
- A recording opened on a running host, by setting the trace directory then, starts each law from its static value. Its first steps can differ from the recorded ones until the law's state catches up. To record from the start, enable the plane and set the directory before the host starts.
- Fair share records a namespace's decision only when its mode, its reason or the share it applies changes, or the share it proposes moves by `Deadband`. So its series holds those changes, not every step.

## Closed-loop simulation

`tools/adaptivesim` closes the loop that replay leaves open. It's a discrete-event simulator of history on its DSQL store, kept coarse: the plugin's pool, gate and connection pacing, DSQL's connection admission and limits, history's task queues, their schedulers and its writes to each shard, its workflow cache and heap, matching's task queues and their partitions, the workers' pollers, and the frontends' namespace rate limits. On that model it runs the signal plane and the controllers on simulated time, so a controller's decisions change the limits the simulated service runs under, and the signals that follow. The signal plane, the controllers, the fair share exchange, the frontends' quota calculator and the gate's admission policy are the real code. It runs every controller: the persistence capacity controller, the task scheduler rate, the worker counts, the memory budget, the partition scaler and fair share, each disabled, in shadow or enabled.

```bash
go run ./cmd/tools/adaptivesim -scenario startup -variant no-grace -trace /tmp/adaptivesim
```

Its scenarios reproduce the appreciation's [short pool](adaptive-appreciation.md#short-pool), start-up, uneven frontends and ramp findings, run the ramp in each of the appreciation's configs and with every controller disabled, and show each controller it runs act in the loop and recover, as seeded tests that run in seconds. Its traces replay through `tools/adaptivereplay`. [The tool's README](../../tools/adaptivesim/README.md) describes what it models and what it leaves out, its calibration against the short pool's local rig, and a cross-check with `tools/poolsim`.

## What stays in dynamic config

If the right value depends on the hardware, take it out of dynamic config; if it depends on what you have promised your users, keep it in. Controllers must never change behaviour a user can observe as semantics.

Staying in dynamic config:

- Correctness and policy limits: history size and event count limits, blob size limits, retention, search attribute limits.
- Feature flags and migrations, including fairness and matcher versions.
- Per-namespace quotas an operator has promised, as distinct from host capacity.
- Every controller's envelope: mode, bounds, step limits, targets, and the freeze key.

## The zero-fork path available today

You can prototype control laws against an unmodified 1.32 server. `WithDynamicConfigClient` (`temporal/server_option.go:141`) accepts any `Client`; implement `NotifyingClient` as well and call `PublishUpdates` when a computed value changes. Feed it signals by teeing metrics through `WithCustomMetricsHandler` (`server_option.go:232`).

It works for settings read with `.Get` on every call and for the subscribed settings, such as history's scheduler worker counts. Three limits make it a prototyping tool rather than the end state:

- Settings read once at construction, and the 14 that need a restart or reload, won't move.
- The controller sees only what is exported as metrics, after aggregation.
- Every adaptive value looks like configuration, so "who set this?" becomes hard to answer in an incident.

## Phasing and roadmap

![roadmap · 5 phases, 4 gates](adaptive-behaviour/roadmap.svg)

Phase 1 goes first because it carries the most operator pain and is already prototyped. Phases 2 and 3 build the shared core and couple the loops; Phase 4 touches matching and cross-host state, which carry the most risk.

Every phase was built first, off by default, and then the [system appreciation](adaptive-appreciation.md) ran the controllers together on a throwaway DSQL cluster, in place of a gate after each phase.

## Evidence from the Phase 1 prototype

With one default config, the proposed controller kept simulated API errors at 0.5–3.4% across six scenarios, where static limits gave 29–33%. The evidence is a fluid simulation of one history host, so it supports building Phase 1, not skipping its shadow period.

**Protocol.** The simulator reproduces today's limiter semantics from the source: the seven-bucket priority cascade, the dynamic and static limiters chained through `MultiRequestRateLimiter`, one health limiter per priority on its own 10s ticker, and the 5000-sample average buffer.

- **Simplified:** the database is a fluid queue with lognormal service times, a utilisation knee and a 2s client deadline with abandonment. One host; no shard movement.
- **Fair to today's code:** oracle-tuned legacy gets the best of six latency thresholds (10–500 ms), picked per scenario with hindsight, and `RateMultiMin` widened to 0.1.
- **Holdout:** trace A was used to diagnose and to tune two parameters (signal window 3s, cooldown 6s). Trace B was not used for any decision.

![simulation runs, adaptive-persistence-sim · 6 scenarios × 4 limiters](adaptive-behaviour/simulation-api-errors.svg)

Oracle-tuned legacy won only on trace B with the fast 3 ms database. Legacy with default settings matches static on trace A, because its 20% maximum cut cannot reach 1,800 ops/s, a 40% cut from the 3,000 static limit. A threshold that suits one database breaks another: 10 ms works on the 3 ms database but throttles the 15 and 60 ms databases to 10% of static, giving 79% API errors. The legacy points leave the limiter's own rejections out of its average, though v1.32.1 records them. With them recorded, the proposed controller is lower in all six scenarios ([Store facts](adaptive-store-facts.md#simulator-runs)).

![simulation run, trace A, 15 ms DB · 1,200 s in 20 s means](adaptive-behaviour/simulation-trace-a-goodput.svg)

The timeline shows three failure modes. Static admits past capacity, every queued request waits past its deadline, and goodput collapses to zero; this FIFO-with-deadline model is harsher than some real stores, but the direction holds. Oracle-tuned legacy cannot settle: steps of 0.3 and 0.1 against a 3000 ceiling reach only multiples of 300, so it alternates between too low (rejections) and too high (timeouts). The proposed controller tracks capacity and used the headroom to clear the 250k-task burst in 137s, against about 1,120s for static.

**With the max-filter.** On 3 November 2026 the simulator was rerun with the branch's controller, which returns to what the store delivered after a cut ([Proposal 3](#proposal-3-one-persistence-capacity-controller)). Across the six scenarios, API errors were 0.5–3.1%, against 0.5–3.4%:

| Scenario | Before | With the max-filter |
|---|---|---|
| Trace A, 3 ms | 0.53% | 0.48% |
| Trace A, 15 ms | 0.68% | 0.61% |
| Trace A, 60 ms | 3.43% | 3.06% |
| Trace B, 3 ms | 1.76% | 1.76% |
| Trace B, 15 ms | 2.01% | 2.01% |
| Trace B, 60 ms | 2.30% | 1.99% |

- On trace A with the 15 ms database, the controller returned to 1,620 calls a second, 90% of the 1,800 the store delivered, 17 s after the drop. API errors stopped at 418 s, rather than 424 s.
- On trace A with the 60 ms database, rejections fell from 2.46 to 1.84% and timeouts rose from 0.97 to 1.23%.
- Over 30 variants of each scenario, with the drop's level and its timing against the controller's steps varied, mean API errors fell or held for every database: on trace A from 2.15 to 2.11%, 2.26 to 2.24% and 3.33 to 3.22%, and on trace B from 1.75 to 1.75%, 1.92 to 1.92% and 2.22 to 2.06%. Of the 180 variants, 101 did better and 22 worse, and the rest moved by less than 0.005 points.
- The gain is in the minute after the drop. On trace A, mean API errors there fell from 14.8 to 14.0%, 17.3 to 16.4% and 21.2 to 19.7%.
- A `DeliveryWindow` of 10s gave 0.01–0.02 points less on trace A and the same on trace B, and one of 60s gave the same as 30s. The default keeps 30s, which leaves room for a store that takes longer to recover.
- The figures above show the controller before the max-filter. With store facts, the rerun is in [Store facts](adaptive-store-facts.md#simulator-runs).

**Limitations.** On a hard step drop the controller briefly over-cuts, because successful throughput collapses during the timeout storm. The max-filter on this branch shortens the over-cut, but doesn't prevent it. It settles at about 85–90% of database capacity by design. The controller, windowed counter and `QuantileRecent` are compiled and tested; the integration files in the patch are syntax-checked and symbol-checked against the source, not compiled. Production traces replayed in a discrete-event simulator are the next test.

## Risks and open questions

| Risk | Why it matters | Mitigation |
|---|---|---|
| Baseline drift | A slowly degrading database raises the learned minimum and the target with it | Absolute latency ceiling in the envelope; on this branch, a warning and a `baseline-up` or `baseline-down` decision when the baseline moves by `BaselineAlert` |
| Over-cut on a step drop | Successful throughput collapses during a timeout storm, so the cut overshoots | Cooldown at least 2× signal span; on this branch, a BBR-style max-filter returns the rate to 90% of what the store delivered after the cut |
| A coupled limit binds instead | The task scheduler rate keeps persistence's calls under the capacity controller's rate, so after a cut the persistence limit stops binding and the cut stays | On this branch, `CoupledBindingShare`, off by default, counts the scheduler's refusals as the limit binding while the calls offered come close to what it lets through |
| Controllers fighting | Worker, persistence and partition loops act on overlapping resources | Timescale separation; workers back off when persistence does. The [system appreciation](adaptive-appreciation.md#loops-and-timescales) saw no two loops chase each other |
| Open-loop shadow | Shadow proposals never feel their own effect | Treat shadow as a direction check; canary small cohorts in enabled mode |
| Hidden state | Learned values are not in YAML, so incidents need a new way to see them | Decision reasons as metrics; a describe API for current controller state |
| Static ceiling lag | The basic limiter refreshes its rate once a minute, so a raised ceiling applies late | Acceptable for a safety ceiling; document it |

Open questions:

- [ ] Should the per-namespace and per-shard persistence limiters also become controller outputs, or stay as promises?
- [ ] What tolerance and ceiling defaults suit Cassandra, Postgres and MySQL deployments? The simulation used one generic database model.
- [ ] Does demand-aware share need consensus on totals, or is eventual agreement within a few seconds enough?
- [ ] How should learned state be shown to operators: `tdbg`, a describe endpoint, or metrics only?

## Appendix: code references (v1.32.1)

All paths are in [temporalio/temporal at v1.32.1](https://github.com/temporalio/temporal/tree/v1.32.1), read from a clone of that tag.

| Symbol | File and line | Why it matters |
|---|---|---|
| `TypedPropertyFn`, `TypedSubscribable` | [common/dynamicconfig/setting_gen.go:907, :935](https://github.com/temporalio/temporal/blob/v1.32.1/common/dynamicconfig/setting_gen.go#L907) | Config fields are function types: the seam for controllers |
| `Client`, `NotifyingClient` | [common/dynamicconfig/client.go](https://github.com/temporalio/temporal/blob/v1.32.1/common/dynamicconfig/client.go) | Pluggable config source for the zero-fork path |
| `WithDynamicConfigClient` | [temporal/server_option.go:141](https://github.com/temporalio/temporal/blob/v1.32.1/temporal/server_option.go#L141) | Public extension point |
| `FIFOScheduler.updateWorkerCount` | [common/tasks/fifo_scheduler.go:111](https://github.com/temporalio/temporal/blob/v1.32.1/common/tasks/fifo_scheduler.go#L111) | Live pool resize: the worker-count actuator |
| `AdaptivePool` | [common/goro/adaptive_pool.go](https://github.com/temporalio/temporal/blob/v1.32.1/common/goro/adaptive_pool.go) | Existing delay-target control law |
| `HealthRequestRateLimiterImpl.refreshRate` | [common/persistence/client/health_request_rate_limiter.go:109](https://github.com/temporalio/temporal/blob/v1.32.1/common/persistence/client/health_request_rate_limiter.go#L109) | Additive steps on average latency |
| Per-priority TODO | [common/persistence/client/quotas.go:238](https://github.com/temporalio/temporal/blob/v1.32.1/common/persistence/client/quotas.go#L238) | One limiter per priority |
| `NewPriorityRateLimiter` | [common/persistence/client/quotas.go:63](https://github.com/temporalio/temporal/blob/v1.32.1/common/persistence/client/quotas.go#L63) | Dynamic and static limiters chained |
| `MultiRequestRateLimiterImpl.Allow` | [common/quotas/multi_request_rate_limiter_impl.go](https://github.com/temporalio/temporal/blob/v1.32.1/common/quotas/multi_request_rate_limiter_impl.go) | Every chained limiter must allow |
| `PriorityRateLimiterImpl.getRateLimiters` | [common/quotas/priority_rate_limiter_impl.go](https://github.com/temporalio/temporal/blob/v1.32.1/common/quotas/priority_rate_limiter_impl.go) | Cascade: lower priorities are charged too |
| `DefaultDynamicRateLimitingParams` | [common/dynamicconfig/shared_constants.go:82](https://github.com/temporalio/temporal/blob/v1.32.1/common/dynamicconfig/shared_constants.go#L82) | Off; thresholds 0; bounds 0.8–1.0 |
| `HealthSignalAggregator` | [common/persistence/health_signal_aggregator.go](https://github.com/temporalio/temporal/blob/v1.32.1/common/persistence/health_signal_aggregator.go) | Average, quantile and error-ratio signals |
| `timeWindowedTDigest.Quantile` | [common/stats/windowed_tdigest.go:107](https://github.com/temporalio/temporal/blob/v1.32.1/common/stats/windowed_tdigest.go#L107) | Merges every window |
| `scheduleLatency` | [service/history/queues/executable.go:257](https://github.com/temporalio/temporal/blob/v1.32.1/service/history/queues/executable.go#L257) | Queueing delay, emitted only as a metric |
| Scheduler rate fallback TODO | [service/history/queue_factory_base.go:168](https://github.com/temporalio/temporal/blob/v1.32.1/service/history/queue_factory_base.go#L168) | Scheduler not coupled to persistence |
| Queue monitor thresholds | [service/history/queues/monitor.go:130](https://github.com/temporalio/temporal/blob/v1.32.1/service/history/queues/monitor.go#L130) | Pending-task limit read via a property function |
| `QueuePendingTaskCriticalCount` | [common/dynamicconfig/constants.go:2024](https://github.com/temporalio/temporal/blob/v1.32.1/common/dynamicconfig/constants.go#L2024) | 9000: a memory bound as a count |
| `cache.New` | [common/cache/lru.go:137](https://github.com/temporalio/temporal/blob/v1.32.1/common/cache/lru.go#L137) | Fixed size, no resize |
| `getQuota` | [common/quotas/calculator/cluster_aware_quota_calculator.go:34](https://github.com/temporalio/temporal/blob/v1.32.1/common/quotas/calculator/cluster_aware_quota_calculator.go#L34) | Share = limit ÷ host count |
| Label change → `HostsChanged` | [common/membership/ringpop/service_resolver.go:457](https://github.com/temporalio/temporal/blob/v1.32.1/common/membership/ringpop/service_resolver.go#L457) | Why load must not ride on labels |
| `PartitionScaler` | [service/matching/partition_scaler_interface.go](https://github.com/temporalio/temporal/blob/v1.32.1/service/matching/partition_scaler_interface.go) | Scaler input lacks saturation signals |
| `simplePartitionScalerFactoryProvider` | [service/matching/fx.go:268](https://github.com/temporalio/temporal/blob/v1.32.1/service/matching/fx.go#L268) | Factory not exposed as a server option |
| `computeEffectiveConfig`, `autoEnableIfNeeded` | [service/matching/task_queue_partition_manager.go:211](https://github.com/temporalio/temporal/blob/v1.32.1/service/matching/task_queue_partition_manager.go#L211) | Template: observed state persisted in user data |
| `TaskQueueUserData` | [proto/internal/temporal/server/api/persistence/v1/task_queues.proto:147](https://github.com/temporalio/temporal/blob/v1.32.1/proto/internal/temporal/server/api/persistence/v1/task_queues.proto#L147) | Reserved room for partition control |

The Phase 1 controller is in `common/adaptive/capacity`, wired in through `common/persistence/client/capacity_rate_limiter.go`. Its simulator, `adaptive-persistence-sim.zip`, is kept outside this repository.
