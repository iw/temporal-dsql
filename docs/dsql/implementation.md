# DSQL Implementation Details

This document covers the technical implementation of Aurora DSQL support in Temporal, including schema changes, OCC handling, and the major contributions to achieve DSQL compatibility.

## Table of Contents

- [Code Structure](#code-structure)
- [Schema Changes](#schema-changes)
  - [BYTEA to UUID in Composite Primary Keys](#bytea-to-uuid-in-composite-primary-keys)
  - [BIGSERIAL Removal](#bigserial-removal)
  - [Other Schema Changes](#other-schema-changes)
- [FOR UPDATE and Locking Changes](#for-update-and-locking-changes)
  - [FOR UPDATE on Single Table Only](#for-update-on-single-table-only)
  - [FOR SHARE Not Supported](#for-share-not-supported)
- [Optimistic Concurrency Control (OCC)](#optimistic-concurrency-control-occ)
  - [Retry Logic](#retry-logic-retrygo)
  - [Error Classification](#error-classification-errorsgo)
  - [Transaction Retry Policy](#transaction-retry-policy-tx_retry_policygo)
- [Compare-And-Swap (CAS) Updates](#compare-and-swap-cas-updates)
  - [Shard Updates](#shard-updates-shardgo-cas_updatesgo)
  - [Auto-Fenced Updates](#auto-fenced-updates)
  - [Generic CAS Helper](#generic-cas-helper)
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
├── execution_store.go             # Execution store wrapper with DSQL retry logic
├── execution_maps.go              # Execution map operations (activity, timer, child, signal)
├── shard.go                       # Shard management with CAS updates
├── cas_updates.go                 # Compare-And-Swap update patterns
├── fenced_updates.go              # Fenced update helpers
├── retry.go                       # OCC retry logic with backoff
├── tx_retry_policy.go             # Transaction-level retry policy
├── tx_retry.go                    # Transaction retry helpers
├── errors.go                      # Error classification (retryable vs permanent)
├── metrics.go                     # DSQL-specific metrics
├── token_cache.go                 # IAM token caching
├── connection_pool.go             # pgxpool config, hooks, connector ownership
├── connection_rate_limiter.go     # Local (per-instance) rate limiting
├── distributed_rate_limiter.go    # DynamoDB-backed distributed rate limiting
├── token_bucket_limiter.go        # GCRA reservations for distributed burst/rate control
├── rate_limiter_fallback.go       # Local fallback on DynamoDB errors
├── uuid.go                        # UUID string conversion utilities
├── typeconv.go                    # DateTime conversion
├── types.go                       # Shared type definitions
├── query_converter.go             # Query parameter conversion
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
├── visibility.go                  # Visibility operations (stub — visibility uses separate store)
├── driver/
│   ├── interface.go               # Driver interface abstraction
│   └── pgx.go                     # pgx driver backend
└── session/
    └── session.go                 # Connection session management

tools/poolsim/                     # Historical pool simulation (see below)

schema/dsql/temporal/
└── schema.sql                     # DSQL-compatible schema
```

## Schema Changes

### BYTEA to UUID in Composite Primary Keys

DSQL has limitations with BYTEA columns in composite primary keys. All identifier columns were changed from `BYTEA` to `UUID`:

```sql
-- PostgreSQL (original)
CREATE TABLE executions(
  namespace_id BYTEA NOT NULL,
  run_id BYTEA NOT NULL,
  ...
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);

-- DSQL (modified)
CREATE TABLE executions(
  namespace_id UUID NOT NULL,
  run_id UUID NOT NULL,
  ...
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);
```

**Tables affected:**
- `executions`, `current_executions`, `buffered_events`
- `tasks`, `task_queues`, `tasks_v2`, `task_queues_v2`
- `activity_info_maps`, `timer_info_maps`, `child_execution_info_maps`
- `request_cancel_info_maps`, `signal_info_maps`, `signals_requested_sets`
- `history_node`, `history_tree`, `cluster_membership`
- `namespaces`, `nexus_endpoints`, `build_id_to_task_queue`

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

`chasm_node_maps.chasm_path` uses `VARCHAR(255)` instead of upstream's `BYTEA`
because DSQL cannot index `BYTEA` and the path is part of the primary key. The
**encoded path must be at most 255 characters and contain no NUL characters**.
The default CHASM path encoder prefixes `\`, `$`, and characters up to `#` with a
backslash, retaining the original character. Escaping therefore increases the
length and does not remove NUL characters, which text columns reject.

A write outside these limits returns DSQL's own error. Core wraps errors from
this writer as `Unavailable`, so Temporal retries the write. A plugin-side check
alone would not make the failure final.

DSQL has no `ALTER COLUMN ... TYPE` operation and caps the total primary key size
across all columns at 1 KiB. Widening the path would require a separate design
decision, rebuilding the table and copying rows in batches of fewer than 3,000;
it cannot be done by widening the column in place.

### BIGSERIAL Removal

The `buffered_events.id` column was changed from `BIGSERIAL` to `BIGINT`, and the plugin allocates the IDs itself:

```sql
-- PostgreSQL
id BIGSERIAL NOT NULL

-- DSQL
id BIGINT NOT NULL
```

**Per-run IDs** (`InsertIntoBufferedEvents` in `execution.go`): the ID only orders a run's buffered events, so the plugin numbers them per run. Inside the caller's transaction, it reads `COALESCE(MAX(id), 0)` for the run and numbers the new rows from there. The same transaction updates the run's executions row, so two transactions appending to the same run conflict at commit and one of them is retried. Schema 1.2 dropped the global unique index on `buffered_events(id)`.

### Other Schema Changes

| Change | Reason |
|--------|--------|
| `CHECK` constraints removed | DSQL doesn't support CHECK |
| Complex `DEFAULT` removed | Limited DEFAULT expression support |
| `UNIQUE` → separate indexes | Better DSQL compatibility |
| `CREATE INDEX ASYNC` | Non-blocking index creation |

## FOR UPDATE and Locking Changes

DSQL has significant limitations with locking clauses that required major changes.

### FOR UPDATE on Single Table Only

DSQL only allows `FOR UPDATE` on a single base table per statement. JOINs with `FOR UPDATE` fail with `SQLSTATE 0A000`.

**Problem:** `LockCurrentExecutionsJoinExecutions` used a JOIN with FOR UPDATE.

**Solution:** Split into two queries:

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

DSQL doesn't support `FOR SHARE`. `ReadLockExecutions` delegates to `WriteLockExecutions`, and `ReadLockShards` uses `FOR KEY SHARE`, which DSQL has supported since August 2026.

**ReadLockExecutions** (`execution.go`):

```go
// DSQL doesn't support FOR SHARE, delegate to WriteLockExecutions
// Safe because: method is unused in codebase, WriteLockExecutions provides
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

```go
type RetryConfig struct {
    MaxRetries   int           // Default: 5
    BaseDelay    time.Duration // Default: 100ms
    MaxDelay     time.Duration // Default: 5s
    JitterFactor float64       // Default: 0.25
}

func (r *RetryManager) RunTx(ctx context.Context, op string, 
    fn func(*sql.Tx) (interface{}, error)) (interface{}, error) {
    
    for attempt := 1; attempt <= maxAttempts; attempt++ {
        tx, err := r.db.BeginTx(ctx, nil)
        if err != nil {
            return nil, err
        }
        
        result, err := fn(tx)
        if err != nil {
            tx.Rollback()
            
            cls := classifyError(err)
            r.metrics.IncTxErrorClass(op, cls.String())
            
            if cls == ErrorTypeRetryable && attempt < maxAttempts {
                r.metrics.IncTxConflict(op)
                r.metrics.IncTxRetry(op, attempt)
                
                delay := r.calculateBackoff(attempt - 1)
                r.metrics.ObserveTxBackoff(op, delay)
                time.Sleep(delay)
                continue
            }
            return nil, err
        }
        
        if err := tx.Commit(); err != nil {
            // Handle commit-time conflicts
            if isRetryable(err) && attempt < maxAttempts {
                delay := r.calculateBackoff(attempt - 1)
                time.Sleep(delay)
                continue
            }
            return nil, err
        }
        
        return result, nil
    }
    
    r.metrics.IncTxExhausted(op)
    return nil, fmt.Errorf("max retries exceeded")
}
```

### Error Classification (`errors.go`)

```go
type ErrorType int

const (
    ErrorTypeUnknown ErrorType = iota
    ErrorTypeRetryable          // SQLSTATE 40001 - retry
    ErrorTypeConditionFailed    // CAS failure - don't retry
    ErrorTypePermanent          // Other errors - don't retry
    ErrorTypeUnsupportedFeature // SQLSTATE 0A000
)

func classifyError(err error) ErrorType {
    var pgErr *pgconn.PgError
    if errors.As(err, &pgErr) {
        switch pgErr.SQLState() {
        case "40001":
            return ErrorTypeRetryable
        case "0A000":
            return ErrorTypeUnsupportedFeature
        }
    }
    
    if IsConditionFailedError(err) {
        return ErrorTypeConditionFailed
    }
    
    return ErrorTypePermanent
}
```

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

## Compare-And-Swap (CAS) Updates

Traditional locking is replaced with conditional updates using fencing tokens.

### Shard Updates (`shard.go`, `cas_updates.go`)

```go
// UpdateShardsWithFencing performs a fenced update using range_id
func (pdb *db) UpdateShardsWithFencing(ctx context.Context, row *sqlplugin.ShardsRow, 
    expectedRangeID int64) (sql.Result, error) {
    
    const query = `UPDATE shards 
        SET range_id = $1, data = $2, data_encoding = $3 
        WHERE shard_id = $4 AND range_id = $5`

    result, err := pdb.ExecContext(ctx, query,
        row.RangeID, row.Data, row.DataEncoding, row.ShardID, expectedRangeID)
    if err != nil {
        return nil, err
    }

    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return nil, NewConditionFailedError(ConditionFailedShard,
            fmt.Sprintf("shard %d range_id changed from expected %d", 
                row.ShardID, expectedRangeID))
    }

    return result, nil
}
```

### Auto-Fenced Updates

For non-transaction contexts, automatically read current fencing token and apply CAS:

```go
func (pdb *db) UpdateShardsAutoFenced(ctx context.Context, row *sqlplugin.ShardsRow) (sql.Result, error) {
    // Read current range_id
    var currentRangeID int64
    err := pdb.GetContext(ctx, &currentRangeID, 
        `SELECT range_id FROM shards WHERE shard_id = $1`, row.ShardID)
    if err != nil {
        return nil, err
    }

    // Fenced update with current range_id as expected value
    return pdb.UpdateShardsWithFencing(ctx, row, currentRangeID)
}
```

### Generic CAS Helper

```go
func (pdb *db) GenericCASUpdate(ctx context.Context, query string, args []interface{},
    entityDesc string, fencingToken interface{}) error {
    
    result, err := pdb.ExecContext(ctx, query, args...)
    if err != nil {
        return err
    }

    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return NewConditionFailedError(ConditionFailedUnknown,
            fmt.Sprintf("%s fencing token changed from expected %v", 
                entityDesc, fencingToken))
    }

    return nil
}
```

## IAM Authentication

### BeforeConnect

`connection_pool.go` configures pgxpool's `BeforeConnect` for every physical connection. A process-wide semaphore admits eight hook calls at a time; each waits on the existing rate limiter, obtains an admin IAM token from the cache, and sets `ConnConfig.Password`. Slots are released before the handshake. Limiter failures wrap `errConnectThrottled`; token errors remain unchanged.

### Token Cache (`token_cache.go`)

The existing cache and `GetConfiguredTokenDuration()` are unchanged. The token provider uses `CLUSTER_ENDPOINT` (or the host in `ConnectAddr`) and `REGION` (falling back to `AWS_REGION`). Token expiry behavior remains tracked in #32; user selection and TLS changes remain tracked in #39.

## Connection Rate Limiting

DSQL has cluster-wide connection limits (100 new connections/sec, 1000 burst, 10000 max). Rate limiting only applies to NEW connection establishment (TCP/TLS handshake + IAM authentication), not to queries.

The plugin supports three rate limiting modes, selected via environment variables. All new connections wait on the selected limiter in `BeforeConnect`.

| Mode | When Used | Key Config |
|------|-----------|------------|
| Token Bucket (recommended) | `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=true` + `DSQL_TOKEN_BUCKET_ENABLED=true` | DynamoDB-backed, supports burst capacity (1000) |
| Per-Second Counter | `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=true` + `DSQL_TOKEN_BUCKET_ENABLED=false` | DynamoDB-backed, no burst support |
| Local | `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=false` (default) | Per-instance `rate.Limiter`, requires manual budget partitioning |

Token bucket mode stores `tat_us` (the next theoretical arrival time in Unix microseconds) and `ttl_epoch` in one `dsql_connect_gcra#<endpoint>` item, using the existing table's string `pk`. With emission interval `T = 1 second / rate`, burst tolerance `tau = (capacity - 1) * T`, and horizon `tau + maxWait`:

1. A busy `UpdateItem` atomically adds `T` when `tat_us BETWEEN t AND t + horizon`, returning `UPDATED_OLD` and `ALL_OLD` on condition failure. The caller sleeps until `max(t, old tat_us - tau)`, respecting cancellation.
2. A missing or older TAT uses an idle conditional update to `t + T`. If another host wins, retry the busy update. At most four busy/idle attempts are made.
3. A TAT beyond the horizon rejects the reservation. `BeforeConnect` wraps that failure as connection throttling.

Non-divisor rates round the interval up to a microsecond to avoid exceeding the configured rate; rates above one million/sec and overflowing horizons are rejected. Each successful update increases TAT, including same-microsecond writes. Clock skew shifts admission times with a one-time allowance bounded by the clock spread times the rate. Canceled reservations remain spent; reclaiming them could admit twice. A crash therefore wastes only outstanding reservations until their times pass. Busy callers perform one atomic write and sleep locally; initialization and idle races require extra conditional writes. The table remains a single hot key and DynamoDB throttling is still possible at high load.

`rate_limiter_fallback.go` wraps either distributed mode. Only failures originating from DynamoDB calls can open the 30-second local fallback window; condition failures and context cancellation are excluded. Limiter policy errors, including the horizon limit, propagate unchanged. Calls during the window use `ConnectionRateLimiter`; after expiry DynamoDB is tried again. Logging is limited to once a minute and `dsql_rate_limiter_fallbacks_total` counts calls routed locally. Startup fallback on client creation failure is unchanged. During a DynamoDB outage, each process applies its own rate and burst; DSQL rejects fleet-wide excess with 53400 (not attempted). Lower the local burst to reduce that excess.

The old bucket item is never read and expires through TTL. Mixed-version rollout or rollback uses two independent items and can temporarily double the configured fleet rate and burst; DSQL rejects excess with 53400. See [deployment](deployment.md#5-distributed-coordination-multi-service) for settings and table permissions.

## Connection Pool

`CreateDB` preserves the `cfg.Connect` test hook. Otherwise it builds a DSN with `session.BuildDSN`, parses a pgxpool config, creates a pool and pings once with a 30-second timeout. Default database names are still tried in order. A failed ping closes the new pool. There is no startup sleep or wait for the background fill.

Service pools use `MinConns == MaxConns == maxConns` (default 100). Ephemeral pools use max 5/min 0. Lifetime is capped at 50 minutes, with four minutes of jitter and one-minute health checks; shorter configured lifetimes apply. No acquire/release hooks are installed. `maxIdleConns` is ignored. The sum of `maxConns` across every Temporal process, including deploy overlap, must fit the cluster connection limit, 10,000 by default, with headroom for ephemeral pools and administration.

The `sql.DB` uses a closing wrapper around `stdlib.GetPoolConnector`, zero idle connections and unlimited open connections. It retains pgx driver fast paths and sqlx's snake-case mapper. The connector cancels hook waits, joins metrics collection, and closes pgxpool when the handle closes. Stock pgxpool internal health-check notifications can finish asynchronously. See [connection pool](connection-pool.md) for lifecycle and expiry limitations.

`DatabaseHandle` receives an always-false refresh predicate. `maybeConvertError` performs its own mapping without calling the upstream `ConvertError`: limiter failures and 53300/53400 become overload; socket errors, `pgconn.ConnectError`, class 08 and IAM access denied become unavailable; 40001/0A000 remain raw; 54000 becomes a size-limit error. Other errors pass through. pgxpool replaces broken connections without rebuilding the pool.

The connection lease was removed (#33). Enabling its retired flag still refuses startup until pools are sized for the fleet limit and the variable is unset. Its DynamoDB table can be deleted once no old deployment uses it. For removed settings, rollout and rollback by previous image (no schema change), see [migration notes](connection-pool.md#migration-from-the-old-layer).

## Pool Metrics Collection

A collector samples `pool.Stat()` every 15 seconds, reporting current gauges and deltas of cumulative counters. It belongs to the connector and is joined on close, including failed startup. See [metrics](metrics.md) for names and precise semantics.

## Pool Simulator

`tools/poolsim` deliberately models the historical reservoir, refiller and guard window. It is unchanged by the pgxpool migration; its results do not validate the current implementation. See [the simulator README](../../tools/poolsim/README.md) for scenarios and commands.

## Testing

```bash
# Run DSQL-specific tests
go test ./common/persistence/sql/sqlplugin/dsql/...

# Run with verbose output
go test -v ./common/persistence/sql/sqlplugin/dsql/...

# Integration tests (requires DSQL cluster)
export DSQL_TEST_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
export AWS_REGION="us-east-1"
go test ./common/persistence/sql/sqlplugin/dsql/... -tags integration
```

The GCRA unit tests use atomic in-memory conditional updates and injected clocks/sleeps. `TestGCRAClockSkew` simulates 40 hosts at offsets from -50 ms to +50 ms over 10 seconds and compares the old algorithm. The bound is 100/sec × 10 seconds + 1000 burst + 10 connections for the full 100 ms clock spread.

The DynamoDB live test needs a table with a `pk` string key and TTL on `ttl_epoch`, plus normal AWS credentials/region configuration. It writes an isolated random test key and deletes it afterward; it does not open DSQL connections:

```bash
DSQL_TEST_DDB_TABLE=<table-name> AWS_REGION=<region> go test -tags integration,disable_grpc_modules,test_dep ./common/persistence/sql/sqlplugin/dsql -run '^TestGCRADynamoDBIntegration$' -count=1 -v
```

It skips when `DSQL_TEST_DDB_TABLE` is unset.
