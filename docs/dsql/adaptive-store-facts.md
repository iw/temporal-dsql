# Store facts for the capacity controller

*Agreed on 29 October 2026, and implemented in steps on the `adaptive-behaviour` branch. It applies to the Phase 1 controller in `common/adaptive/capacity`, described in [Proposal 3](adaptive-behaviour.md#proposal-3-one-persistence-capacity-controller).*

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
- **Landed** in #85, ahead of the facts.

### 2. Classify what the store did

`isUnhealthyError` becomes a classification of each outcome:

| Class | Outcome | Used for |
|---|---|---|
| Rejected | Temporal's own persistence limiter refused the call | Nothing: it never reached the store |
| Timeout | Temporal's deadline expired, or the call was cancelled | Overload, as today: Temporal owns its deadlines |
| Throttled | The store's plugin reports that capacity held the call back | Overload |
| Contention | The store's plugin reports that conflicting writes used up their retries | Reported, not an overload signal (decision 2) |
| Limit | The request exceeds a fixed store limit | Neither: load doesn't change it |
| Queued | The store's plugin reports that a caller waited for one of its connections | An early overload signal: the plugin's own pool is short before any deadline expires |
| Done | Anything else the store answered, errors included | Throughput and latency |

The class names belong to Temporal, not DSQL, so another store's plugin can report the same classes.

**Timeouts are recognised by the call's deadline.** SQL stores return a query that its deadline interrupted as `Unavailable`, with the deadline only in the message, for example `GetWorkflowExecution: failed to get timer info. Error: timeout: context deadline exceeded`. So the type of the error doesn't show the timeout.
- The persistence metrics client records each call with its own context, through `RecordCall`. A call that fails after its deadline has passed counts as a timeout in the controller's windows, whatever error the store returned.
- The average and error ratio that the per-priority health limiters and the deep health check read still go by the error itself.

### 3. Report facts beside the error

The facts shouldn't change the errors callers see. Today `ResourceExhausted` drives client backoff and history's task rescheduling, and a new type or cause would change both.

So the plugin reports each fact to a small sink when it happens, and returns the same error as now:

```go
// common/persistence
type StoreFact int

const (
	StoreFactThrottled StoreFact = iota
	StoreFactContention
	StoreFactLimit
	StoreFactQueued
	StoreFactAcquired // the base that queued facts are a share of
)

// StoreFacts receives typed facts from a persistence plugin.
type StoreFacts interface {
	DeclareStoreFacts() // once, by a plugin that reports facts
	ReportStoreFact(fact StoreFact, count int64)
}

// StoreFactsReceiver is implemented by what passes the sink on to a plugin.
type StoreFactsReceiver interface {
	SetStoreFacts(StoreFacts)
}
```

The health signal aggregator implements `StoreFacts`.
- It counts facts in the same windows as requests, so the snapshot gains a count of each fact.
- It also emits them as `persistence_store_facts`, tagged by fact, so shadow runs show them.

**The sink is injected once.** `client.NewFactory` hands the aggregator to the data store factory, which keeps it on its connection and passes it to the plugin's database when that is created. The other options were a recorder in each call's context, which would change every metrics client method, and a process-wide sink, which tests would have to manage. Visibility has its own factory, so its facts don't reach the persistence controller.

**A plugin declares its facts.** A plugin that reports facts calls `DeclareStoreFacts` when it receives the sink. A healthy store can go a long time without reporting a fact, so the controller can't wait for one to learn the store reports them.

### 4. What the DSQL plugin reports

| Where | Fact |
|---|---|
| `maybeConvertError`: the call's deadline expired while `beforeConnect` waited for a connection-attempt slot or the connection rate limiter | Throttled |
| `maybeConvertError`: DSQL returned 53400 (connection rate exceeded) or 53300 (too many connections) | Throttled |
| `TxRetryBackoff`: retries on 40001 ran out | Contention |
| `maybeConvertError`: DSQL returned 54000 (transaction, row, key or age limits), 53200 (a transaction's memory) or 53100 (storage) | Limit |
| A once-a-second sample of the pool's acquires that found no idle connection | Queued |
| The same sample's count of all the pool's acquires | Acquired |

The plugin reports each failed call once, where it already converts the error. Its existing pool collector samples every 15 seconds, which is too coarse for the controller's windows, so queued and acquired facts have a sampler of their own.

`txExecute` is core code from the fork's patch set, and its retry policy belongs to the plugin. The plugin reports contention where its retries run out.

AWS's quotas page lists two throttling codes for DSQL, 53400 for the connection rate and 53300 for the connection count. Its other codes are fixed limits. Nothing throttles query throughput, so on DSQL these facts and Temporal's timeouts cover every documented constraint.

**The plugin's own pacing shows up as queued facts.** The first row rarely fires. pgx's pool (puddle) builds a new connection in a goroutine whose context the caller's deadline doesn't cancel. So `beforeConnect` waits on the pacing without that deadline.
- A caller that waits and then gets a connection counts as queued. One whose deadline passes first gives up on the pool, and its call counts as a timeout.
- The pacing wait fails only if the pool closes during it, at shutdown.
- On DSQL, throttled facts come from DSQL's own refusals.

### 5. How the controller uses the facts

- **Throttled facts are overload.**
  - Timeouts plus throttled facts, divided by store requests, are held to the existing `MaxErrorRatio`. A cut for throttling reports the reason `throttled`.
  - Delivered throughput, which a cut is based on, leaves out calls reported as throttled, because they never reached the store.
- **Contention is reported, not acted on.** Conflicts concentrate on hot keys, so cutting the host's rate would shed unrelated work. Contention appears in `persistence_store_facts`.
- **Queued facts are an early overload signal.** The pool belongs to the plugin, so callers queueing for its connections is a fact, not an inference.
  - It warns before deadlines expire, as latency did before.
  - Queued facts are read as a share of acquired facts, and that share is held to `MaxQueuedRatio`, 0.25 by default. At 0.25, a quarter of the pool's acquires found no idle connection. A cut reports the reason `queued`.
  - They aren't divided by store requests. One request can take several connections in turn, one for each transaction and one for each query outside a transaction, so that ratio has no fixed scale.
- **On a store that reports facts, latency isn't a trigger.** The controller relies on facts and Temporal's own timeouts.
  - The learned p99 law keeps running in shadow for comparison. When it would have cut, the step is logged, and counted as `latency-shadow` in `persistence_capacity_decisions`, which records every step's reason.
  - If shadow runs show the facts miss nothing it catches, it goes for those stores.
  - Stores whose plugins report no facts keep it.

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
- **Simulator:**
  - Without facts, the controller's simulator results match the original controller byte for byte.
  - The simulator doesn't yet model the host recording the limiter's rejections, or a source of throttled facts. Once it does, rerun the six scenarios.
- **Live:** run in shadow mode on a throwaway cluster, with `maxConns` and the connection rate set low enough to cause throttling. The decisions it logs show whether the controller hears DSQL.

## Shadow runs

In October 2026 the controller ran in shadow mode three times: it made and recorded its decisions while the existing limiter stayed in charge. Each run used a throwaway DSQL cluster in eu-west-1 and a single-process server on a laptop, so every query paid a round trip to the region. A driver started simple workflows at a fixed rate.

**29 October: 4 shards at 10 starts a second.** One phase capped history's `maxConns` at 4, and another paced new connections at 1 a second with 2-minute lifetimes.
- Queued facts reached the controller, which decided to cut for `queued` 54 and 48 times.
- There were no throttled facts. The plugin's pacing showed up as queued facts, as [section 4](#4-what-the-dsql-plugin-reports) describes.
- Timeouts weren't counted, because SQL stores return them as `Unavailable`. #88 fixed that.
- 4 shards couldn't keep up, and the phases shared a database, so later phases started with a backlog.

**30 October, after #88: 32 shards at 5 starts a second, `maxConns` 20.** From here on, each phase had its own schemas and server.
- History's pool was the limit, at about 250 calls a second.
- The controller's first `queued` decision came 10 seconds into the load. The latency law never fired, because it learned its baseline while the pool was already saturated.
- Queued facts came to about 1.55 per request. As a share of acquires, which is how #89 reads them, about 0.93 of acquires waited, going by the next run's 1.66 acquires per call.
- The run stopped there, since the baseline itself was pool-bound.

**30 October, after #89: 32 shards, `maxConns` 64.**

| Phase | Load | History calls a second | Queued share | Cuts | Latency-law shadow cuts |
|---|---|---|---|---|---|
| Baseline | 5 starts a second for 5 minutes | 247 | 0 | 0 | history 2 |
| Ramp | 5, 10, 20 and 30 starts a second, 2 minutes each | 380 | 0 | 0 | history 4, matching 8 |
| Connection storm | 5 starts a second, plus 300 connection attempts a second for a minute | 287 | 0 | 0 | history 11, matching 5 |

- No acquire waited. Each history call took about 1.66 of the pool's connections in turn.
- History's latency-law shadow cuts came at the start of load, when p99 rose past twice a baseline learned while the server was idle, and late in the drains. Matching's came during the ramp's top steps and just after the storm. None came with persistence errors or waits for connections.
- The ramp's limit was above persistence. 4,196 of its 7,800 workflows were still running after a 5-minute drain, while persistence took about 170 writes a second across 32 shards, with no waits for connections and 4 errors.
  - History writes to each shard one at a time (`history.shardIOConcurrency` is 1), and each write takes several round trips from the laptop.
  - The updates that failed ended with their context cancelled before they reached the store, so the controller doesn't see them, by design.
- DSQL refused about 60% of the storm's connection attempts with 53400. The server's pools were full and needed no new connections during the storm, so no caller saw a refusal and no throttled facts were reported.

**What they show:** queued facts catch a short pool within seconds, where the latency law can miss it. With a healthy pool, the facts made no cuts.

**What they don't cover:** store-side overload. From a laptop, history's shard writes reach their limit long before DSQL does, so throttled facts have no live evidence yet. Whether the latency law can go for stores that report facts, as [section 5](#5-how-the-controller-uses-the-facts) proposes, waits on the simulator testing them.

## Decisions

Agreed on 29 October 2026:

1. **The boundary:** the sink is injected once, through the persistence factory.
2. **Contention:** it's reported, not acted on.
3. **Latency on DSQL:** the controller relies on facts and timeouts. The learned p99 law runs in shadow for comparison, and queued facts give the early warning.
4. **Order:** fix 1 landed first, in #85, then the sink and the plugin's facts, in #86, then the controller acting on them.
