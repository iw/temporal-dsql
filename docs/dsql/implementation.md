# DSQL Implementation Details

This document covers the technical implementation of Aurora DSQL support in Temporal, including the schema, transaction retries and connection management.

## Table of Contents

- [Code Structure](#code-structure)
- [Schema Layout](#schema-layout)
  - [UUID Identifiers](#uuid-identifiers)
  - [Buffered Event IDs](#buffered-event-ids)
  - [Schema Constraints](#schema-constraints)
- [Locking](#locking)
  - [Single-Table Locking](#single-table-locking)
  - [FOR SHARE Not Supported](#for-share-not-supported)
- [Optimistic Concurrency Control (OCC)](#optimistic-concurrency-control-occ)
  - [Retry Logic](#retry-logic-retrygo)
  - [Error Classification](#error-classification-retrygo-errorsgo)
  - [Transaction Retry Policy](#transaction-retry-policy-tx_retry_policygo)
- [Shard and Task Queue Ownership](#shard-and-task-queue-ownership)
- [Batched Range Deletes](#batched-range-deletes)
- [IAM Authentication](#iam-authentication)
  - [BeforeConnect](#beforeconnect)
  - [Token Cache](#token-cache-tokencachego)
- [Connection Rate Limiting](#connection-rate-limiting)
- [Connection Pool](#connection-pool)
- [Pool Metrics Collection](#pool-metrics-collection)
- [Pool Simulator](#pool-simulator)
- [Testing](#testing)

## Code Structure

```
common/persistence/sql/sqlplugin/dsql/
├── plugin.go                      # Plugin registration, IAM auth, token refresh
├── db.go                          # Database handle, transaction management
├── execution.go                   # Workflow execution operations
├── execution_store.go             # Workflow read transaction wrapper
├── execution_maps.go              # Execution map operations (activity, timer, child, signal)
├── shard.go                       # Shard locks and updates
├── retry.go                       # OCC retry logic with backoff
├── tx_retry_policy.go             # Transaction-level retry policy
├── errors.go                      # Error classification (retryable vs permanent)
├── metrics.go                     # DSQL-specific metrics
├── token_cache.go                 # IAM token caching
├── connection_pool.go             # pgxpool config, hooks, connector ownership
├── connection_rate_limiter.go     # Local (per-instance) rate limiting
├── token_bucket_limiter.go        # GCRA reservations for distributed burst/rate control
├── rate_limiter_fallback.go       # Local fallback on DynamoDB errors
├── uuid.go                        # UUID string conversion utilities
├── typeconv.go                    # DateTime conversion
├── types.go                       # Shared type definitions
├── query_converter.go             # Visibility query dialect
├── admin.go                       # Admin operations
├── cluster_metadata.go            # Cluster metadata operations
├── namespace.go                   # Namespace operations
├── nexus_endpoints.go             # Nexus endpoint operations
├── events.go                      # History node/tree operations
├── queue.go                       # Queue operations
├── queue_v2.go                    # Queue v2 operations
├── task_v1.go                     # Task v1 operations
├── task_v2.go                     # Task v2 operations
├── task_queues.go                 # Task queue operations
├── task_queue_id.go               # Task queue ID handling
├── task_user_data.go              # Task queue user data
├── visibility.go                  # Visibility store, adapted from the PostgreSQL plugin
├── driver/
│   ├── interface.go               # Driver interface abstraction
│   └── pgx.go                     # pgx driver backend
└── session/
    └── session.go                 # DSN construction

tools/poolsim/                     # pgxpool and DSQL simulation

schema/dsql/temporal/
├── schema.sql                     # Full baseline
└── versioned/v1.3/
    ├── manifest.json              # Applies the full baseline at 1.3
    └── schema.sql                 # Same schema for the update path
```

## Schema Layout

### UUID Identifiers

Identifier columns in composite primary keys use `UUID`, which DSQL can index.
The plugin converts Go UUID values to strings for SQL parameters and converts
query results back to Temporal's identifier types.

**Application-level handling** (`uuid.go`):

```go
// Convert primitives.UUID to string for DSQL queries
func UUIDToString(u primitives.UUID) string {
    return formatUUID(u) // Returns "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}

// Usage in execution.go
namespaceIDStr := row.NamespaceID.String()
runIDStr := row.RunID.String()
```

### CHASM path limits

`chasm_node_maps.chasm_path` is `VARCHAR(1536)` rather than upstream's `BYTEA`,
because DSQL can't index `BYTEA` and the path is part of the primary key. 1,536
is the width of upstream's MySQL column.

- **NUL bytes:** the default CHASM path encoder prefixes `\`, `$`, and characters
  up to `#` with a backslash, keeping the original character, so a NUL in a
  node's key stays in its path. Text columns can't hold NUL, so the plugin stores
  0x00 as 0x01 0x01 and 0x01 as 0x01 0x02, and reverses this when it reads.
  Stored paths keep the encoder's byte order and prefixes.
- **Key size:** DSQL limits a key to 2,000 bytes. When a table is created, it
  adds up the declared widths of the key's columns, so beside the shard ID, the
  two UUIDs and a `VARCHAR(255)` workflow ID, the path could be at most
  `VARCHAR(1709)`. When a row is written, it checks the actual bytes. A path of
  1,536 ASCII characters fits even beside a 255-character workflow ID, but a
  path of multi-byte characters can be too long. That write fails with SQLSTATE
  54000, which the plugin reports as a `TransactionSizeLimitError`. Core wraps
  any error from this write as `Unavailable`, so Temporal retries it.

### Buffered Event IDs

`buffered_events.id` is a `BIGINT` allocated by the plugin.

**Per-run IDs** (`InsertIntoBufferedEvents` in `execution.go`): the ID only orders a run's buffered events, so the plugin numbers them per run. Inside the caller's transaction, it reads `COALESCE(MAX(id), 0)` for the run and numbers the new rows from there. The same transaction updates the run's executions row, so two transactions appending to the same run conflict at commit and one of them is retried. The composite primary key includes the run identifiers and event ID; IDs are not globally unique.

### Schema Constraints

| Schema choice | Reason |
|--------|--------|
| `CHECK` constraints omitted | Supported by DSQL; this schema relies on application validation |
| Simple `DEFAULT` expressions | Limited DEFAULT expression support |
| Separate unique indexes | Better DSQL compatibility |
| `CREATE INDEX ASYNC` | Non-blocking index creation |

## Locking

Core owns transaction boundaries. The plugin uses single-table locking queries.

### Single-Table Locking

`LockCurrentExecutionsJoinExecutions` locks `current_executions` and then reads
`executions.last_write_version` in the same transaction:

```go
// execution.go - LockCurrentExecutionsJoinExecutions
func (pdb *db) LockCurrentExecutionsJoinExecutions(ctx context.Context, filter ...) {
    // 1) Lock current_executions (single-table FOR UPDATE)
    err := pdb.GetContext(ctx, &row, lockCurrentExecutionQuery, ...)
    
    // 2) Read executions.last_write_version separately (no FOR UPDATE)
    err := pdb.GetContext(ctx, &lastWriteVersion, getExecutionLastWriteVersionQuery, ...)
    
    row.LastWriteVersion = lastWriteVersion
    return []sqlplugin.CurrentExecutionsRow{row}, nil
}
```

### FOR SHARE Not Supported

DSQL doesn't support `FOR SHARE`. `ReadLockExecutions` delegates to `WriteLockExecutions`, and `ReadLockShards` uses `FOR KEY SHARE`.

**ReadLockExecutions** (`execution.go`):

```go
// DSQL doesn't support FOR SHARE, delegate to WriteLockExecutions
// Safe because: method is unused by core, WriteLockExecutions provides
// stronger guarantees
func (pdb *db) ReadLockExecutions(ctx context.Context, filter ...) (int64, int64, error) {
    return pdb.WriteLockExecutions(ctx, filter)
}
```

**ReadLockShards** (`shard.go`):

```go
const readLockShardQry = `SELECT range_id FROM shards WHERE shard_id = $1 FOR KEY SHARE`
```

Every workflow write reads the shard row with this query before it writes. `UpdateShard` takes `FOR UPDATE` on the same row, and the two conflict at commit. If a new owner renews the range while an old owner's write is in flight, whichever transaction commits second fails with SQLSTATE 40001. When the write is retried, it reads the new range and fails with `ShardOwnershipLostError`. Key-share readers don't conflict with each other.

`TestReadLockShardsForKeyShareIntegration` checks both behaviours against a live cluster; its doc comment gives the command to run it.

## Optimistic Concurrency Control (OCC)

DSQL uses OCC instead of pessimistic locking. Concurrent transactions that conflict receive `SQLSTATE 40001` (serialization failure).

### Retry Logic (`retry.go`)

`RetryManager` retains configuration, error classification, backoff helpers and metrics. It does not open or replay transactions. The defaults are five retries, a 100 ms base delay, a 5 s maximum delay, and a 0.25 jitter factor.

### Error Classification (`retry.go`, `errors.go`)

The classification helpers also recognize OCC data/schema codes, condition failures, unsupported features and connection limits. The production transaction policy is narrower: it retries a PostgreSQL error with SQLSTATE 40001 and never retries a condition failure. SQLSTATE 54000 is converted to `TransactionSizeLimitError` by the database wrapper. Classification alone does not enable a retry.

### Transaction Retry Policy (`tx_retry_policy.go`)

The DSQL `db` implements `sqlplugin.TxRetryPolicy`, an opt-in hook that Temporal's SqlStore uses to retry a whole transaction:

```go
type TxRetryPolicy interface {
    IsRetryableTxError(err error) bool
    TxRetryBackoff(operation string, attempt int, err error) (time.Duration, bool)
}
```

`txExecute` checks the raw error from `BeginTx`, the transaction body and `Commit` before converting it. On SQLSTATE 40001 it runs the whole transaction again, with exponential backoff and jitter from `DefaultRetryConfig`: 100 ms base delay, 5 retries. When the retries run out, or the context ends while waiting, it returns `ResourceExhausted` with cause `SYSTEM_OVERLOADED`, which history treats as not committed. Connection throttling and SQLSTATE 53300/53400 return the same error.

```go
var _ sqlplugin.TxRetryPolicy = (*db)(nil)
```

## Shard and Task Queue Ownership

Core calls the shard, task queue and namespace metadata locks inside transactions. Shard ownership is checked by core's `lockShard`: the stored range must match the expected range before the update. `TestUpdateShardFencing` covers the stale-owner failure. Task queue updates likewise follow core's range check. The plugin performs plain updates within those transactions; the locks and DSQL's commit conflict detection protect the checked rows. It does not start nested lock transactions or infer an expected range for a standalone update.

## Batched Range Deletes

`deleteInBatches` uses primary-key subqueries with a final `LIMIT` parameter and accumulates affected rows until a batch is smaller than the limit. Task and queue deletes start at 1,000 rows; history-node deletes start at 100. A SQLSTATE 54000 halves the batch size until a single-row failure is reported as a size-limit error. Each batch must run in autocommit mode so it has its own transaction quota.

V1 queue deletion preserves an exclusive lower and inclusive upper bound. V2 includes both bounds. Core computes the v2 range before deletion and always retains the last physical message so enqueue IDs remain increasing. After deletion, a transaction re-reads queue metadata and advances the partition minimum only when the computed minimum is higher. Readers skip missing rows if the metadata update fails; retrying completes the update. Overlapping deletes cannot move the minimum backwards. Large deletes perform more statements as the range grows, without growing the per-transaction row count.

Matching's task cleanup deletes one batch per call: core passes an exclusive upper `task_id` and a limit, `matching.maxTaskDeleteBatchSize` (100 by default), under a 5-second timeout. The statement reads the batch's lowest and highest `task_id` with one limited scan, then deletes that key range, so its plan has no join. With upstream's `task_id IN (subquery)` form, DSQL can plan a nested loop that reruns the subquery for every row in the queue. It does so while a queue is missing from the table statistics, as a new worker's sticky queue is. Each rerun is a storage round trip, and a 100-row cleanup on a 5,000-row queue took 8 seconds.

Fair queues, in `tasks_v2` when `matching.enableFairness` is on, order tasks by `(pass, task_id)`. Their cleanup finds the batch's first and last task in the same way and deletes the tasks between them. DSQL applies row comparisons as filters, so plain bounds on `pass` limit both scans. The delete reads and skips any later tasks that share the batch's last pass.

## IAM Authentication

### BeforeConnect

`connection_pool.go` configures pgxpool's `BeforeConnect` for every physical connection. A process-wide semaphore admits eight hook calls at a time; each waits on the existing rate limiter, obtains an IAM token for the configured `user` (default `admin`) from the cache, and sets `ConnConfig.Password`. Slots are released before the handshake. Limiter failures wrap `errConnectThrottled`; token errors remain unchanged.

### Token Cache (`token_cache.go`)

The token provider uses the typed settings resolved once by `settings.go`: `connectAttributes.cluster_endpoint` (defaulting to the host in `connectAddr`), `region` (inferred from a DSQL endpoint unless explicit), and `token_duration`. Tokens are cached by endpoint, region, user and duration, with expiry taken from the signed token. Expired or malformed tokens are never served, and credentials refresh before expiry. Concurrent refreshes for a key are coalesced without blocking valid cache hits. Token generation is logged at Debug; failures retain their existing logging.

Connections use `DbConnectAdmin` for `admin` and `DbConnect` for other roles. Password settings are ignored with a warning. TLS defaults to `verify-full`, including hostname verification, unless `connectAttributes.sslmode` explicitly overrides it. See [authentication and pacing](connection-pool.md#authentication-and-connection-pacing).

## Connection Rate Limiting

DSQL has cluster-wide connection limits (100 new connections/sec, 1000 burst, 10000 max). Rate limiting only applies to NEW connection establishment (TCP/TLS handshake + IAM authentication), not to queries.

The plugin supports two rate limiting modes, selected via `connectAttributes.rate_limiter`. All new connections wait on the selected limiter in `BeforeConnect`.

| Mode | When Used | Key Config |
|------|-----------|------------|
| Token Bucket (recommended) | `rate_limiter: "dynamodb_token_bucket"` | DynamoDB-backed, supports burst capacity (1000) |
| Local | `rate_limiter: "local"` (default) | Per-instance `rate.Limiter`, requires manual budget partitioning |

Token bucket mode requires `rate_limiter_table`. Token bucket tuning uses `token_bucket_rate`, `token_bucket_capacity` and `token_bucket_max_wait`. Local pacing and outage fallback use `connection_rate_limit` and `connection_burst_limit`. All keys belong in the datastore's `connectAttributes` map. The first pool's limiter settings remain shared process-wide; later differences warn once without replacing it. The DynamoDB client uses the resolved region.

Settings come from datastore attributes or defaults; the resolver reads no environment variables. See [settings and validation](deployment.md#datastore-settings). Invalid settings fail `CreateDB`; valid unused mode settings warn.

Token bucket mode stores `tat_us` (the next theoretical arrival time in Unix microseconds) and `ttl_epoch` in one `dsql_connect_gcra#<endpoint>` item, using the existing table's string `pk`. With emission interval `T = 1 second / rate`, burst tolerance `tau = (capacity - 1) * T`, and horizon `tau + maxWait`:

1. A busy `UpdateItem` atomically adds `T` when `tat_us BETWEEN t AND t + horizon`, returning `UPDATED_OLD` and `ALL_OLD` on condition failure. The caller sleeps until `max(t, old tat_us - tau)`, respecting cancellation.
2. A missing or older TAT uses an idle conditional update to `t + T`. If another host wins, retry the busy update. At most four busy/idle attempts are made.
3. A TAT beyond the horizon rejects the reservation. `BeforeConnect` wraps that failure as connection throttling.

Non-divisor rates round the interval up to a microsecond to avoid exceeding the configured rate; rates above one million/sec and overflowing horizons are rejected. Each successful update increases TAT, including same-microsecond writes. Clock skew shifts admission times with a one-time allowance bounded by the clock spread times the rate. Canceled reservations remain spent; reclaiming them could admit twice. A crash therefore wastes only outstanding reservations until their times pass. Busy callers perform one atomic write and sleep locally; initialization and idle races require extra conditional writes. The table remains a single hot key and DynamoDB throttling is still possible at high load.

`rate_limiter_fallback.go` wraps the token bucket. Only failures originating from DynamoDB calls can open the 30-second local fallback window; condition failures and context cancellation are excluded. Limiter policy errors, including the horizon limit, propagate unchanged. Calls during the window use `ConnectionRateLimiter`; after expiry DynamoDB is tried again. Logging is limited to once a minute and `dsql_rate_limiter_fallbacks_total` counts calls routed locally. Startup fallback on client creation failure is unchanged. During a DynamoDB outage, each process applies its own rate and burst; DSQL rejects fleet-wide excess with 53400 (not attempted). Lower the local burst to reduce that excess.

See [deployment](deployment.md#5-distributed-coordination-multi-service) for settings and table permissions.

## Connection Pool

For a visibility database, `CreateDB` sets `search_path` to `temporal_visibility` unless the datastore's `connectAttributes` set it. It supports the `cfg.Connect` test hook. Otherwise it resolves settings before creating the handle, removes plugin attributes from a copy of the config, builds a DSN with `session.BuildDSN`, parses a pgxpool config, creates a pool and pings once with a 30-second timeout. Default database names are tried in order. A failed ping closes the new pool. There is no startup sleep or wait for the background fill.

Service pools use `MinConns == MaxConns == maxConns` (default 100). Ephemeral pools use max 5/min 0. Lifetime is capped at 50 minutes, with four minutes of jitter and one-minute health checks; shorter configured lifetimes apply. No acquire/release hooks are installed. `maxIdleConns` is ignored. The sum of `maxConns` across every Temporal process, including deploy overlap, must fit the cluster connection limit, 10,000 by default, with headroom for ephemeral pools and administration.

The `sql.DB` uses a closing wrapper around `stdlib.GetPoolConnector`, zero idle connections and unlimited open connections. It retains pgx driver fast paths and sqlx's snake-case mapper. The connector cancels hook waits, joins metrics collection, and closes pgxpool when the handle closes. Stock pgxpool internal health-check notifications can finish asynchronously. See [connection pool](connection-pool.md) for lifecycle and expiry limitations.

`DatabaseHandle` receives an always-false refresh predicate. `maybeConvertError` performs its own mapping without calling the upstream `ConvertError`: limiter failures and 53300/53400 become overload; socket errors, `pgconn.ConnectError`, class 08 and IAM access denied become unavailable; 40001/0A000 remain raw; 54000 becomes a size-limit error. Other errors pass through. pgxpool replaces broken connections without rebuilding the pool.

## Pool Metrics Collection

A collector samples `pool.Stat()` every 15 seconds, reporting current gauges and deltas of cumulative counters. It belongs to the connector and is joined on close, including failed startup. See [metrics](metrics.md) for names and precise semantics.

## Pool Simulator

`tools/poolsim` models pgxpool lifecycle behavior, a DSQL cluster, connection limiters and phased workloads. Scenarios exercise fleet startup, pool sizing, demand spikes, mass drops, rolling deployments and limiter outages. See [the simulator README](../../tools/poolsim/README.md) for commands and model limitations.

## Testing

```bash
# Run DSQL-specific tests
go test ./common/persistence/sql/sqlplugin/dsql/...

# Run with verbose output
go test -v ./common/persistence/sql/sqlplugin/dsql/...

# Integration tests (requires DSQL cluster)
export DSQL_TEST_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
go test ./common/persistence/sql/sqlplugin/dsql/... -tags integration
```

The GCRA unit tests use atomic in-memory conditional updates and injected clocks/sleeps. `TestGCRAClockSkew` simulates 40 hosts at offsets from -50 ms to +50 ms over 10 seconds. The bound is 100/sec × 10 seconds + 1000 burst + 10 connections for the full 100 ms clock spread.

The DynamoDB live test needs a table with a `pk` string key and TTL on `ttl_epoch`, plus normal AWS credentials/region configuration. It writes an isolated random test key and deletes it afterward; it does not open DSQL connections:

```bash
DSQL_TEST_DDB_TABLE=<table-name> AWS_REGION=<region> go test -tags integration,disable_grpc_modules,test_dep ./common/persistence/sql/sqlplugin/dsql -run '^TestGCRADynamoDBIntegration$' -count=1 -v
```

It skips when `DSQL_TEST_DDB_TABLE` is unset.
