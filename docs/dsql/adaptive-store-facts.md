# Store facts for the capacity controller

*Draft for review. It applies to the Phase 1 controller in `common/adaptive/capacity`, described in [Proposal 3](adaptive-behaviour.md#proposal-3-one-persistence-capacity-controller).*

## Summary

The capacity controller sets how fast a host admits persistence requests. Today it judges what the database can take from latency and timeouts. On DSQL, the plugin knows more precise facts:
- when it held back a new connection;
- when DSQL refused one;
- when conflicting writes used up their retries.

None of those facts reach the controller.

This draft proposes that Temporal adapts only what it owns, its admission rate, and that the plugin tells it what the store did, as typed facts. It also fixes a gap in what the controller measures today: the persistence limiter's own rejections are counted as work the database delivered.

## What the controller hears today

Every persistence call passes through `Retryable(Metrics(RateLimited(Store)))`, as `common/persistence/client/factory.go` builds it.
- The metrics client records each call's latency and error in the health signal aggregator.
- `RecentSnapshot` turns the newest windows into the controller's signals: p99, the unhealthy error ratio and throughput.

| Outcome | Returned as | Counted today as | Effect on the controller |
|---|---|---|---|
| The persistence limiter rejected the call | `ResourceExhausted`, cause `PERSISTENCE_LIMIT`, after about 0 ms | A completed request | Throughput includes calls that never reached the store, and p99 gains near-zero samples |
| Temporal's deadline expired, or the call was cancelled | Deadline exceeded, `TimeoutError` | Unhealthy | Counts toward the error ratio |
| The plugin held back a new connection until the call's deadline | `ResourceExhausted`, cause `SYSTEM_OVERLOADED` | A completed request | Not heard |
| DSQL refused a connection (53400, 53300) | `ResourceExhausted`, cause `SYSTEM_OVERLOADED` | A completed request | Not heard |
| Conflict retries ran out (40001) | `ResourceExhausted`, cause `SYSTEM_OVERLOADED`, from `txExecute` | A completed request | Not heard |
| The transaction exceeded DSQL's limits (54000) | `TransactionSizeLimitError` | A completed request | None, which is right: load doesn't cause it |

This has two consequences:

- **Cuts that don't take effect.** On overload, the controller cuts to 90% of delivered throughput, and never above its current rate.
  - While the limit binds, delivered throughput includes the limiter's own rejections, so 90% of it can exceed the current rate, and the cut does nothing.
  - The simulator assumed rejections aren't recorded, so it doesn't show this.
- **Inference instead of facts.** On DSQL, latency is the controller's only overload signal. So it infers the store's health from latency, while the plugin, which knows DSQL's rules, stays silent.

## The principle

- Temporal adapts what it owns. Here, that is the rate at which a host admits persistence requests.
- Knowledge of the store lives in its plugin. The DSQL plugin knows the cluster's 100 new connections a second, its connection limit, and its optimistic concurrency.
- The plugin passes typed facts across the boundary. Core doesn't infer a store's health from latency or from generic errors.

## Proposal

### 1. Leave the limiter's rejections out

This applies to any store.
- **What changes:** `Record` stops putting calls that ended in `ResourceExhausted`, cause `PERSISTENCE_LIMIT`, into the latency distribution and the windowed counter, because they never reached the store. The average and error ratio that the per-priority health limiters read stay as they are.
- **Why the controller doesn't need them:** the capacity limiter learns from its own denials that its limit binds.
- **When it can land:** it fixes Phase 1 on its own, needs no new types, and can land first.

### 2. Classify what the store did

`isUnhealthyError` becomes a classification of each outcome:

| Class | Outcome | Used for |
|---|---|---|
| Rejected | Temporal's own persistence limiter refused the call | Nothing: it never reached the store |
| Timeout | Temporal's deadline expired, or the call was cancelled | Overload, as today: Temporal owns its deadlines |
| Throttled | The store's plugin reports that capacity held the call back | Overload |
| Contention | The store's plugin reports that conflicting writes used up their retries | Reported, not an overload signal (decision 2) |
| Limit | The request exceeds a fixed store limit | Neither: load doesn't change it |
| Done | Anything else the store answered, errors included | Throughput and latency |

The class names belong to Temporal, not DSQL, so another store's plugin can report the same classes.

### 3. Report facts beside the error

The facts shouldn't change the errors callers see. Today `ResourceExhausted` drives client backoff and history's task rescheduling, and a new type or cause would change both.

So the plugin reports each fact to a small sink when it happens, and returns the same error as now:

```go
// common/persistence
type StoreFact int

const (
	StoreFactThrottled StoreFact = iota + 1
	StoreFactContention
	StoreFactLimit
)

// StoreFacts receives typed facts from a persistence plugin.
type StoreFacts interface {
	Report(fact StoreFact)
}
```

The health signal aggregator implements `StoreFacts` and counts facts in the same windows as requests. The snapshot then gains three ratios, for throttled, contention and limit facts per store request.

How the sink reaches the plugin is still open (decision 1):

- **Inject it once (recommended).** `client.NewFactory` hands the aggregator to the data store factory through an optional interface, and the SQL factory passes it to the plugin. That's one wiring point, with no change to the persistence interfaces.
- **Per call, through the context.** The metrics client puts a recorder in each call's context, and the plugin reports into it. Facts belong to the exact call, but every metrics client method changes.
- **A process-wide sink.** This is the smallest change, but it's global state that tests have to manage.

### 4. What the DSQL plugin reports

| Where | Fact |
|---|---|
| `beforeConnect`: the call's deadline expired while waiting for a connection-attempt slot or the connection rate limiter | Throttled |
| `maybeConvertError`: DSQL returned 53400 (connection rate exceeded) or 53300 (too many connections) | Throttled |
| `dsqlTxRetryPolicy.TxRetryBackoff`: retries on 40001 ran out | Contention |
| `maybeConvertError`: DSQL returned 54000 | Limit |

`txExecute` is core code from the fork's patch set, and its retry policy belongs to the plugin. The plugin reports contention where it already counts exhausted retries, in `IncTxExhausted`.

### 5. How the controller uses the facts

- **Throttled facts are overload.** The snapshot's error ratio becomes timeouts plus throttled facts, divided by store requests, and the existing `MaxErrorRatio` still applies. A cut for throttling reports a new reason, `throttled`.
- **Contention is reported, not acted on.** Conflicts concentrate on hot keys, so cutting the host's rate would shed unrelated work. Contention appears as a metric and as a decision reason.
- **Latency for a store that reports facts is open** (decision 3). The controller can keep the learned p99 target, or it can drop latency as a trigger and rely on facts and Temporal's own timeouts. Dropping it follows the principle. Shadow mode can compare the two on the same traffic before either is enabled.

## What stays the same

- The errors returned to callers stay the same, and so do client backoff and task rescheduling.
- With `Capacity.Mode` disabled, every decision still goes to the per-priority health limiters.
- Stores whose plugins report no facts keep the latency and timeout signals they have now.

## Testing and rollout

- **Unit tests:**
  - the classification, for every row above;
  - the aggregator's fact counts per window;
  - the controller cutting on throttled facts but not on contention.
- **Plugin tests:** each DSQL path reports its fact once, using the overlay and fake-driver technique from the October review.
- **Simulator:** record the limiter's rejections as the host does, add a source of throttled facts, and rerun the six scenarios.
- **Live:** run in shadow mode on a throwaway cluster, with `maxConns` and the connection rate set low enough to cause throttling. The decisions it logs show whether the controller hears DSQL.

## Decisions for review

1. **The boundary:** inject the sink once (recommended), pass it per call through the context, or use a process-wide sink?
2. **Contention:** only report it (recommended), or also cut the rate when it's widespread?
3. **Latency on DSQL:** keep the learned p99 target, or rely on facts and timeouts?
4. **Order:** land fix 1 first on its own (recommended), or together with the facts?
