# Adaptive behaviour: changes to upstream code

The `adaptive-behaviour` branch carries main's patch set, which [Changes to upstream code](upstream-changes.md) records, and the changes below. They support the persistence capacity controller in [Proposal 3](adaptive-behaviour.md#proposal-3-one-persistence-capacity-controller) and its [store facts](adaptive-store-facts.md), the task queueing delay that [Proposal 2](adaptive-behaviour.md#proposal-2-a-per-host-signal-plane) adds to the host's signals, and the partition scaler option from [Proposal 6](adaptive-behaviour.md#proposal-6-the-partition-scaler-learns-capacity).

The branch takes main's changes by merge or rebase, and reaches a new release tag through main. When that happens, check the files below against the new tag first.

## Changed upstream files

| File | Change and reason |
| --- | --- |
| `common/dynamicconfig/shared_constants.go` | `DynamicRateLimitingParams` gains `Capacity`, the controller's envelope. Its `Mode` is disabled by default, and its `RolloutPercent`, 100 by default, is the share of hosts that apply an enabled `Mode`. |
| `common/persistence/client/quotas.go` | `NewPriorityRateLimiter` wraps the per-priority health limiters in the capacity-aware limiter. The basic limiter's rate comes from `capacityCeilingFn`. Both take the host's rollout key, its hostname. |
| `common/persistence/client/factory.go` | `NewFactory` hands the health signal aggregator to a data store factory that accepts store facts. |
| `common/persistence/health_signal_aggregator.go` | The aggregator gains `RecentSnapshot`, `RecordCall` and `RecordScheduleLatency`, and implements `StoreFacts`. The snapshot leaves out the persistence limiter's own rejections, counts a call that failed after its deadline as a timeout, and leaves calls the store reports as throttled out of delivered throughput. It also reports the p90 of how long history task attempts waited in their schedulers. |
| `common/persistence/noop_health_signal_aggregator.go` | The no-op aggregator implements the new methods. |
| `common/persistence/persistence_metric_clients.go` | Each call records its own context through `RecordCall`, so a call that fails after its deadline counts as a timeout, whatever error the store wrapped it in. |
| `common/persistence/sql/factory.go` | `DbConn` keeps the store-facts sink and passes it to the plugin's database when that's created. |
| `common/stats/windowed_tdigest.go` | `QuantileRecent` reads a quantile over the newest windows only. |
| `common/metrics/metric_defs.go` | Adds `persistence_store_facts` and `persistence_capacity_decisions`. |
| `service/history/queues/executable.go` | Each task attempt also passes its `scheduleLatency` to an optional `ScheduleLatencyRecorder`, beside the existing metric. |
| `service/history/queues/executable_factory.go` | `NewExecutableFactory` takes the recorder and passes it to each executable. |
| `service/history/queue_factory_base.go` | `QueueFactoryBaseParams` gains `HealthSignals`, the persistence health signal aggregator. |
| `service/history/transfer_queue_factory.go`, `timer_queue_factory.go`, `visibility_queue_factory.go`, `archival_queue_factory.go`, `outbound_queue_factory.go` | Each passes `HealthSignals` to `NewExecutableFactory` as the recorder. |
| `service/history/queues/queue_base_test.go`, `queue_scheduled_test.go` | Pass no recorder to `NewExecutableFactory`. |
| `temporal/server_option.go` | Adds `WithPartitionScalerFactory`, which sets the factory matching uses to scale task queue partitions. |
| `temporal/server_options.go` | `serverOptions` keeps that factory. |
| `temporal/fx.go` | Passes the factory to the matching service, whose graph uses it in place of its default when one is set. |
| `temporal/server_test.go` | `runAndTestServer` takes extra server options. |
| `AGENTS.md` | Lists the adaptive behaviour documents, beside main's DSQL documents. |

## Fork-added files inside upstream packages

These files extend existing packages without editing upstream files:

- `common/adaptive/capacity/controller.go`: the controller.
- `common/persistence/client/capacity_rate_limiter.go`: the capacity-aware limiter.
- `common/persistence/store_facts.go`: the store-facts types.
- `common/persistence/windowed_counter.go`: request, error and fact counts aligned with the latency windows.
- `common/persistence/sql/store_facts.go`: passes the sink through the SQL factory and `DbConn`.
- Their tests, and `common/persistence/health_signal_aggregator_snapshot_test.go`, `common/persistence/client/store_facts_test.go`, `common/stats/windowed_tdigest_recent_test.go`, `service/history/queues/executable_schedule_latency_test.go` and `temporal/partition_scaler_option_test.go`.

The DSQL plugin's store facts live in its own directory, `common/persistence/sql/sqlplugin/dsql`.

## Listing the branch's changes

With `origin/main` and the base tag available locally, this Bash command lists the branch's changes to upstream files on top of main:

```bash
base=origin/main
paths=()
while IFS= read -r -d '' path; do
  if git cat-file -e "v1.32.1:$path" 2>/dev/null; then
    paths+=("$path")
  fi
done < <(git diff --name-only -z "$base"...HEAD)
if ((${#paths[@]})); then
  git diff --stat "$base"...HEAD -- "${paths[@]}"
fi
```

Remove `--stat` for the patch. To list the branch's new files instead, run `git diff --diff-filter=A --name-only origin/main...HEAD`.
