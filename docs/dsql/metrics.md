# DSQL Metrics Reference

The DSQL plugin emits metrics for monitoring database operations, connection pool health, and retry behavior.

## Transaction Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dsql_tx_error_class_total` | Counter | `operation`, `error_class` | Classification recorded when the transaction policy schedules a retry |
| `dsql_tx_exhausted_total` | Counter | `operation` | Retry budget exhausted |
| `dsql_tx_conflict_total` | Counter | `operation` | Serialization conflicts (SQLSTATE 40001) |
| `dsql_tx_retry_total` | Counter | `operation`, `attempt` | Retry attempts |
| `dsql_tx_backoff` | Timer | `operation` | Backoff delay durations |

The policy retries only SQLSTATE 40001, so `error_class` is always `retryable`. The conflict and retry counters count scheduled retries. When the retry budget runs out, the exhaustion counter increments instead.

## Connection Pool Metrics

The connector samples `pgxpool.Stat()` every 15 seconds. Gauges are snapshots; counters record increases since the last sample, including the first sample from zero. Collection stops and is joined when the DB closes, after one last sample, so a pool that closes between samples still reports its counts.

Every metric a pool records carries `db_kind`: `main` for the persistence store and `visibility` for the visibility store, so a host's two pools report apart. That includes the gate metrics and the cancel and connection counters below. With the `service_name` a service's metrics carry, `db_kind` names each pool in a process that runs several services.

| Metric | Type | pgxpool source / meaning |
|---|---|---|
| `dsql_pool_open` | Gauge | `TotalConns`: idle + acquired + constructing |
| `dsql_pool_in_use` | Gauge | `AcquiredConns` |
| `dsql_pool_idle` | Gauge | `IdleConns` |
| `dsql_pool_max_open` | Gauge | `MaxConns` |
| `dsql_pool_constructing` | Gauge | `ConstructingConns` |
| `dsql_pool_acquires_total` | Counter | Increase in `AcquireCount`: successful acquires |
| `dsql_pool_wait_total` | Counter | Increase in `EmptyAcquireCount`: successful acquires that found no idle resource and waited |
| `dsql_pool_wait_milliseconds_total` | Counter | Increase in `EmptyAcquireWaitTime`, in milliseconds: the time those acquires waited for a connection |
| `dsql_pool_new_connections_total` | Counter | Increase in `NewConnsCount` |
| `dsql_pool_lifetime_destroys_total` | Counter | Increase in `MaxLifetimeDestroyCount` |
| `dsql_pool_canceled_acquires_total` | Counter | Increase in `CanceledAcquireCount` |

`NewConnsCount` increments before `BeforeConnect`, so it counts connection attempts, failed ones included. `TotalConns` includes constructing connections; subtract `dsql_pool_constructing` to estimate established connections. Canceled acquires are counted separately from successful waits. The wait time leaves out the ping pgxpool sends on a connection that has been idle for over a second.

## Connection Metrics

These are recorded as each connection attempt ends, not sampled.

| Metric | Type | Meaning |
|---|---|---|
| `dsql_pool_connect_latency` | Timer | A connection's handshake: TCP, TLS and IAM authentication. It leaves out the wait for a slot, the rate limiter and the token |
| `dsql_pool_connect_failures_total` | Counter | Failed connection attempts: a rate limiter or token error, or a failed handshake |

pgxpool drops the errors of its background fill, so the failure counter is the record of them. When a connection fails, pgxpool cancels the rest of its fill; canceled attempts, and those of a closing pool, aren't counted.

Service pools retain their full configured size. Ephemeral startup pools have max 5/min 0. The sum of every service pool's `maxConns` across all Temporal processes, including deploy overlap, must fit the cluster limit (10,000 by default). `database/sql` statistics describe only the outer adapter, so they are not the source for these DSQL pool metrics.

## Gate Metrics

The [gate](connection-pool.md#the-gate) in front of the pool records these as they happen. `kind` is `read` or `write`.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `dsql_pool_gate_waits_total` | Counter | `kind` | Callers admitted after waiting for a slot |
| `dsql_pool_gate_wait` | Timer | `kind` | How long callers admitted after waiting waited |
| `dsql_pool_gate_refusals_total` | Counter | `kind` | Callers refused because they couldn't be served before their deadline, at once or while waiting |

Callers wait at the gate rather than in pgxpool, so `dsql_pool_wait_total` counts the acquires that waited while pgxpool opened or replaced a connection.

## Cancel Request Metrics

`dsql_cancel_requests_skipped_total` counts the cancel requests the plugin skips. pgx would send one on a new connection for each statement it cuts off at its deadline, and for each connection it closes after an error. DSQL answers them with `0A000`, so the plugin doesn't dial; see [connection pool](connection-pool.md#cancel-requests). The counter is recorded when a dial is refused, not sampled. Each skipped request also means a closed connection that pgxpool replaces, so a sustained rate shows in `dsql_pool_new_connections_total` too.

## Rate Limiter Metrics

`dsql_rate_limiter_fallbacks_total` counts calls routed to the local limiter after a DynamoDB failure, including calls during the 30-second cooldown.

Use limiter logs and connection errors alongside the fallback counter. The eight-slot process-wide semaphore bounds limiter/token calls, not handshakes.

The token bucket falls back to local pacing for 30 seconds after DynamoDB API, throttling or network failures. A warning is logged at most once a minute. During an outage, each process applies `connectAttributes.connection_rate_limit` and `connectAttributes.connection_burst_limit` from the first pool; DSQL rejects fleet excess with 53400 (not attempted). Lower the local burst to reduce excess. Horizon rejection, condition failures and cancellation do not increment the fallback counter. Configuration errors fail startup, and a later pool with different limiter settings warns once without changing the shared limiter. Alert on sustained fallback activity:

```promql
rate(dsql_rate_limiter_fallbacks_total[5m]) > 0
```

## Monitoring and alerts

```promql
# Saturation: alert on a sustained ratio above your service's baseline.
dsql_pool_in_use / dsql_pool_max_open

# Contention at the gate.
rate(dsql_pool_gate_waits_total[5m])
rate(dsql_pool_gate_refusals_total[5m])

# Contention in the pool: the share of acquires that waited, their mean wait in
# milliseconds, how many acquires waited at once on average, and canceled acquires.
rate(dsql_pool_wait_total[5m]) / rate(dsql_pool_acquires_total[5m])
rate(dsql_pool_wait_milliseconds_total[5m]) / rate(dsql_pool_wait_total[5m])
rate(dsql_pool_wait_milliseconds_total[5m]) / 1000
rate(dsql_pool_canceled_acquires_total[5m])

# Failed connection attempts, and handshake latency where timers are histograms.
rate(dsql_pool_connect_failures_total[5m])
histogram_quantile(0.95, sum by (le, service_name, db_kind) (rate(dsql_pool_connect_latency_bucket[5m])))

# Established connections and attempts to refill.
dsql_pool_open - dsql_pool_constructing
rate(dsql_pool_new_connections_total[5m])
rate(dsql_pool_lifetime_destroys_total[5m])

# Statements cut off mid-flight, and connections closed after errors.
rate(dsql_cancel_requests_skipped_total[5m])

# OCC conflicts, retries and exhaustion.
rate(dsql_tx_conflict_total[5m])
rate(dsql_tx_retry_total[5m])
rate(dsql_tx_exhausted_total[5m])

```

Investigate sustained acquisition cancellation, waits with a saturated pool, or an established-connection deficit after startup. A pool whose acquires rarely wait, and whose busiest moments leave connections idle, has room to shrink; one whose waits grow with load needs more connections or less work. Failed connections, or construction attempts without growth, can indicate throttling, IAM failures or network failures. Check logs and the cluster-wide connection budget before increasing pool size. A full pool with rising waits suggests workload pressure; additional replicas also consume their full pool allocation.

```yaml
- alert: DSQLPoolSaturation
  expr: dsql_pool_in_use / dsql_pool_max_open > 0.9
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "DSQL pool above 90% utilization"
- alert: DSQLGateRefusals
  expr: rate(dsql_pool_gate_refusals_total[5m]) > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "DSQL pool refusing calls it can't serve before their deadlines"
- alert: DSQLAcquireCancellation
  expr: rate(dsql_pool_canceled_acquires_total[5m]) > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "DSQL pool acquisition cancellations"
- alert: DSQLConnectFailures
  expr: rate(dsql_pool_connect_failures_total[5m]) > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "DSQL connection attempts failing"
- alert: DSQLRetryExhaustion
  expr: rate(dsql_tx_exhausted_total[5m]) > 0
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "DSQL transaction retry budget exhausted"
```

Metrics use Temporal's configured backend and inherited service tags. Review [connection pool](connection-pool.md) for sizing and expiry limitations.
