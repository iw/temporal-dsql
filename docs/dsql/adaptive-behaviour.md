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

## Proposal 2: a per-host signal plane

Generalise `persistence.HealthSignalAggregator` into a `common/signals` package that every controller on a host reads. It publishes one immutable snapshot per tick, so controllers see a consistent view and can be tested deterministically with a fake `clock.TimeSource`.

| Source | Where it comes from | Change needed |
|---|---|---|
| Persistence latency, errors, throughput | `persistence_metric_clients.go` already calls `Record` per request | Recent-window quantile and per-window counters (done in the Phase 1 patch) |
| Task queueing delay | `scheduleLatency` in `executable.go:257` | One `Record` call beside the metric at line 357 |
| RPC handler latency | `rpc/interceptor/health_check.go` | Feed the existing aggregator into the plane |
| Go scheduler and heap | `runtime/metrics`: `/sched/latencies:seconds`, `/gc/heap/goal:bytes` | New source |
| Memory limit | cgroup `memory.max`, `GOMEMLIMIT` | New source |

The building blocks exist: `stats.NewWindowedTDigest` and `aggregate.MovingWindowAverage`. Two fixes matter for control. The quantile must cover only the newest windows, so the Phase 1 patch adds `QuantileRecent(q, n, now)`. The latency window should be 1s rather than the default 5s.

**Keep the plane host-local.** Ringpop labels look like a free gossip channel, but a label change flips `labelsChecksum` and raises a `HostsChanged` membership event (`service_resolver.go:457`). Shard ownership listeners react to that, so churning a load label would churn membership. Cross-host demand belongs in Proposal 7's slower, dedicated exchange.

## Proposal 3: one persistence capacity controller

Replace the per-priority health limiters with one controller per host that computes an admitted rate C. All seven priority buckets take C (the operator bucket C × `operatorRPSRatio`), so the existing cascade still sheds background work first. When the controller is on, the basic static limiter becomes a safety ceiling at `MaxMultiplier × hostMaxQPS`, which removes the structural cap described in the diagnosis.

The control law has no latency threshold in milliseconds. It borrows from TCP BBR and Cubic:

1. **Target.** Learn a baseline as the 10-minute minimum of p99; aim for p99 at baseline × 2, capped at an absolute ceiling (750 ms) that guards against slow baseline drift.
2. **Cut on overload.** When p99 exceeds the target or unhealthy errors exceed 1%, cut to 90% of the throughput the database actually delivered. Never cut more than half in one step.
3. **Act once per signal.** The cooldown between cuts is at least twice the signal span, so the controller never acts twice on the same stale data.
4. **Probe only when binding.** Increase only while the limiter is rejecting requests, so the limit cannot inflate while idle. Near the last overload point, start at 0.5% per step and ramp to 5% over 30s.

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
```

**Couple the task scheduler to it next.** `QueueSchedulerRateLimiterProvider` should derive the scheduler's host rate from the controller's admitted rate divided by measured persistence calls per task, instead of a static fallback. Tasks then wait in the scheduler, which is cheap, instead of failing mid-execution and bouncing through the rescheduler.

**On DSQL, hear the plugin's facts.** A draft for passing the DSQL plugin's throttling and contention to the controller, rather than inferring the store's health from latency, is in [Store facts for the capacity controller](adaptive-store-facts.md).

## Proposal 4: worker counts follow queueing delay

The `*ProcessorSchedulerWorkerCount` settings (transfer, timer, memory timer, visibility, archival, replication, low-priority replication) become controller outputs with the old values as their fallback. The actuator already exists: `FIFOScheduler.updateWorkerCount` resizes the pool live from a `TypedSubscribable[int]`.

The signal is `scheduleLatency`. Its start time is set when the reader submits a task (`reader.go:525`) or the rescheduler resubmits it (`rescheduler.go:230`), so it measures pure wait inside the scheduler.

The control law is `goro.AdaptivePool`'s, lifted into a controller:

- **Grow** when p90 `scheduleLatency` exceeds the target and the persistence controller is not backing off.
- **Shrink** slowly when workers sit idle, with jitter so the pool shrinks slower than it grows.

The backoff guard is the important part. Adding workers while persistence is the bottleneck only manufactures more `ResourceExhausted` errors, which `executable.go` then resubmits.

## Proposal 5: a memory budget instead of counts and bytes

Read the cgroup `memory.max` or `GOMEMLIMIT` at startup and divide it into budgets: the history cache, the events cache, and pending tasks held in memory by queue readers. The settings that hold those sizes today become fractions of the budget, with the old absolute values as caps.

Three code changes are needed:

1. **Make caches resizable.** Add `SetMaxSize` to `common/cache/lru.go`, evicting on shrink. Today `New(maxSize)` fixes the size for the cache's life, which is why `HistoryCacheHostLevelMaxSizeBytes` needs a restart.
2. **Derive the pending-task limit.** Compute `QueuePendingTaskCriticalCount` from the budget divided by observed bytes per executable. The monitor reads it through an `IntPropertyFn` (`monitor.go:130`), so the swap is local. The mitigator's `targetLoadFactor` of 0.8 stays as it is.
3. **Resize on a slow loop.** Re-evaluate every few minutes from heap goal and live heap in `runtime/metrics`, so caches give way before the garbage collector thrashes.

This turns a question operators cannot answer well ("how many bytes of cache for this pod?") into one they can ("what share of this pod's memory is cache?").

## Proposal 6: the partition scaler learns capacity

Matching's partition scaler already has the hard parts: persisted `PartitionScaleState`, propagation to callers through the partition-count gRPC trailer (`sendPartitionCountTrailer`), drain handling, shadow mode and hysteresis. What it lacks is a saturation signal. `PartitionScalerInput` carries only `NumTasks`, `CurrentTarget`, `BacklogCounts` and `PrivateState`, so `SimplePartitionScalerThreshold.TargetRate` (tasks per second per partition) is an operator's capacity guess.

Three changes close the gap:

1. **Enrich the input.** Add sync-match ratio, backlog age, and add rate against dispatch rate. Matching already computes these for poller scaling via `GetPhysicalQueueAdjustedStats`. Add the owning host's persistence latency from the signal plane.
2. **Learn capacity per partition.** When add rate exceeds dispatch rate and backlog age grows while the host is healthy, record the observed dispatch rate as the partition's capacity in the `anypb` `PrivateState`, which exists for exactly this. Target partitions = add rate ÷ learned capacity, with headroom.
3. **Expose the factory.** `PartitionScalerFactory` is provided through fx (`matching/fx.go:268`) but absent from `temporal/server_option.go`. A `WithPartitionScalerFactory` option lets operators experiment without forking.

The proto already reserves room for more learned state: `task_queues.proto:147` reads "For future use: description, rate limits, manual partition control, etc."

## Proposal 7: fair share follows demand

This is the hardest change and should come last. Replace the `MemberCounter` dependency in `common/quotas/calculator` with a `ShareProvider` interface whose default implementation is today's `clusterLimit / clusterSize`, so behaviour is unchanged until a new provider is chosen.

A demand-weighted provider gives each host a share proportional to its recent per-namespace demand. That needs a low-frequency exchange of demand between frontends, on the order of seconds, through a dedicated internal RPC. It must not ride on ringpop labels, for the membership-churn reason in Proposal 2.

History's `OwnershipAwareQuotaCalculator` is the precedent: it already scales a host's share by the fraction of shards it owns, an observed quantity rather than a head count.

## Safety, operability and testing

Every controller ships disabled, is proven in shadow, and stays inside bounds an operator sets. The rules below apply to all seven proposals.

- **Three modes.** Disabled returns the static value exactly. Shadow runs the controller and reports proposals while the static value applies. Enabled applies the output, clamped. Shadow is open-loop: proposals never feel their own effect, so they show direction and timing, not a settled value.
- **Bounds and step limits.** Every output is clamped to a minimum and maximum and to a largest change per tick.
- **Staged rollout.** `RolloutAccepts` keyed on host or shard ID enables a controller on a stable, growing cohort.
- **One freeze key.** An `adaptive.frozen` setting holds every controller at its current value during an incident.
- **Explainable decisions.** Each decision carries a reason (latency, errors, cooldown, idle, probe), emitted as a metric tag and logged on decreases, as matching already does for poller scaling.

**Separate the timescales** so loops don't chase each other. Inner loops must settle before outer loops act on them.

| Loop | Step | Settles in |
|---|---|---|
| Worker concurrency | 1s | Seconds |
| Persistence capacity | 1s, at least 6s between cuts | Tens of seconds |
| Memory budget | Minutes | Minutes |
| Partition count | Rate-limited by `MaxRate` | Minutes |
| Demand-aware share | Seconds-scale exchange | Tens of seconds |

**Testing.** Controllers are pure functions of state, envelope and signals, so unit tests run on a fake clock. Recorded signal traces replay through them offline. Interactions between controllers need a discrete-event simulator, such as iw/tempdes, fed with production traces before any canary.

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

## Evidence from the Phase 1 prototype

With one default config, the proposed controller kept simulated API errors at 0.5–3.4% across six scenarios, where static limits gave 29–33%. The evidence is a fluid simulation of one history host, so it supports building Phase 1, not skipping its shadow period.

**Protocol.** The simulator reproduces today's limiter semantics from the source: the seven-bucket priority cascade, the dynamic and static limiters chained through `MultiRequestRateLimiter`, one health limiter per priority on its own 10s ticker, and the 5000-sample average buffer.

- **Simplified:** the database is a fluid queue with lognormal service times, a utilisation knee and a 2s client deadline with abandonment. One host; no shard movement.
- **Fair to today's code:** oracle-tuned legacy gets the best of six latency thresholds (10–500 ms), picked per scenario with hindsight, and `RateMultiMin` widened to 0.1.
- **Holdout:** trace A was used to diagnose and to tune two parameters (signal window 3s, cooldown 6s). Trace B was not used for any decision.

![simulation runs, adaptive-persistence-sim · 6 scenarios × 4 limiters](adaptive-behaviour/simulation-api-errors.svg)

Oracle-tuned legacy won only on trace B with the fast 3 ms database. Legacy with default settings matches static on trace A, because its 20% maximum cut cannot reach 1,800 ops/s, a 40% cut from the 3,000 static limit. A threshold that suits one database breaks another: 10 ms works on the 3 ms database but throttles the 15 and 60 ms databases to 10% of static, giving 79% API errors.

![simulation run, trace A, 15 ms DB · 1,200 s in 20 s means](adaptive-behaviour/simulation-trace-a-goodput.svg)

The timeline shows three failure modes. Static admits past capacity, every queued request waits past its deadline, and goodput collapses to zero; this FIFO-with-deadline model is harsher than some real stores, but the direction holds. Oracle-tuned legacy cannot settle: steps of 0.3 and 0.1 against a 3000 ceiling reach only multiples of 300, so it alternates between too low (rejections) and too high (timeouts). The proposed controller tracks capacity and used the headroom to clear the 250k-task burst in 137s, against about 1,120s for static.

**Limitations.** On a hard step drop the controller briefly over-cuts, because successful throughput collapses during the timeout storm. It settles at about 85–90% of database capacity by design. The controller, windowed counter and `QuantileRecent` are compiled and tested; the integration files in the patch are syntax-checked and symbol-checked against the source, not compiled. Production traces replayed in a discrete-event simulator are the next test.

## Risks and open questions

| Risk | Why it matters | Mitigation |
|---|---|---|
| Baseline drift | A slowly degrading database raises the learned minimum and the target with it | Absolute latency ceiling in the envelope; alert when the baseline moves |
| Over-cut on a step drop | Successful throughput collapses during a timeout storm, so the cut overshoots | Cooldown at least 2× signal span (in the patch); next, a BBR-style max-filter on delivered rate |
| Controllers fighting | Worker, persistence and partition loops act on overlapping resources | Timescale separation; workers back off when persistence does |
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
