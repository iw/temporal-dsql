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
  - [Token-Refreshing Driver](#token-refreshing-driver-drivertokenrefreshinggo)
  - [Token Cache](#token-cache-tokencachego)
- [Connection Rate Limiting](#connection-rate-limiting)
- [Pool Metrics Collection](#pool-metrics-collection)
- [Non-Reservoir Mode (Fallback)](#non-reservoir-mode-fallback)
- [Connection Reservoir](#connection-reservoir)
  - [Reservoir Architecture](#reservoir-architecture)
  - [Reservoir Configuration](#reservoir-configuration)
  - [Reservoir Driver](#reservoir-driver)
  - [Reservoir Refiller](#reservoir-refiller)
  - [Distributed Connection Leasing](#distributed-connection-leasing)
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
├── connection_rate_limiter.go     # Local (per-instance) rate limiting
├── distributed_rate_limiter.go    # DynamoDB-backed distributed rate limiting
├── token_bucket_limiter.go        # Token bucket rate limiter (recommended for production)
├── slot_block_manager.go          # Block-based distributed connection leasing
├── reservoir_config.go            # Reservoir configuration from environment
├── conn_lease_config.go           # Connection lease configuration
├── distributed_conn_lease.go      # DynamoDB-backed global connection count limiting
├── pool_warmup.go                 # Connection pool pre-warming at startup (non-reservoir mode)
├── pool_keeper.go                 # Background pool maintenance (non-reservoir mode)
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
│   ├── token_refreshing_driver.go # Token-refreshing database driver
│   ├── connection_registry.go     # Connection tracking for lifecycle management
│   ├── interface.go               # Driver interface abstraction
│   ├── pgx.go                     # pgx driver backend
│   ├── pq.go                      # pq driver backend (legacy)
│   ├── reservoir.go               # Connection reservoir (channel-based buffer)
│   ├── reservoir_driver.go        # Reservoir-backed driver (returns ErrReservoirEmpty on empty)
│   ├── reservoir_conn.go          # Connection wrapper for reservoir
│   └── reservoir_refiller.go      # Background refiller with in-flight semaphore
└── session/
    └── session.go                 # Connection session management

tools/poolsim/                     # Discrete event simulator for reservoir behavior
├── main.go                        # Entry point and simulation runner
├── config.go                      # Configuration loading and defaults
├── reservoir.go                   # Connection reservoir model
├── refiller.go                    # Continuous refiller simulation
├── workload.go                    # Database workload simulation
├── limiter.go                     # Rate limiter model
├── metrics.go                     # Metrics collection
├── assertions.go                  # Validation assertions
├── scenario.go                    # Scenario configuration (cold_start, mass_drop, etc.)
├── service.go                     # Service abstraction
├── sim.go                         # Discrete event simulation engine
├── README.md                      # Documentation
└── scenarios/                     # Pre-configured test scenarios
    ├── local.yaml                 # Local development (200 connections)
    ├── ecs-150wps.yaml            # ECS 150 WPS (2,000 connections)
    ├── ecs-400wps.yaml            # ECS 400 WPS (22,000 connections)
    ├── mass-drop.yaml             # Mass connection drop recovery
    └── workload-test.yaml         # Workload simulation test

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

`txExecute` checks the raw error from `BeginTx`, the transaction body and `Commit` before converting it. On SQLSTATE 40001 it runs the whole transaction again, with exponential backoff and jitter from `DefaultRetryConfig`: 100 ms base delay, 5 retries. When the retries run out, or the context ends while waiting, it returns `ResourceExhausted` with cause `SYSTEM_OVERLOADED`, which history treats as not committed. An empty connection reservoir returns the same error.

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

### Token-Refreshing Driver (`driver/token_refreshing.go`)

Custom database driver that injects fresh IAM tokens for each new connection:

```go
// Token provider called for each new connection
tokenProvider := func(ctx context.Context) (string, error) {
    return tokenCache.GetToken(ctx, endpoint, region, user, duration)
}

// Register driver with token provider
driverName, err := driver.RegisterTokenRefreshingDriverWithLogger(
    adminUser, tokenProvider, logFunc)

// Open connection - driver calls tokenProvider automatically
db, err := sql.Open(driverName, baseDSN)
```

### Token Cache (`token_cache.go`)

```go
type TokenCache struct {
    credentials aws.CredentialsProvider
    cache       sync.Map  // endpoint -> cachedToken
}

type cachedToken struct {
    token     string
    expiresAt time.Time
}

func (tc *TokenCache) GetToken(ctx context.Context, endpoint, region, user string, 
    duration time.Duration) (string, error) {
    // Check cache first
    if cached, ok := tc.cache.Load(endpoint); ok {
        if time.Now().Before(cached.expiresAt) {
            return cached.token, nil
        }
    }
    
    // Generate new token
    token, err := dsql.GenerateDbConnectAdminAuthToken(ctx, endpoint, region, 
        tc.credentials, duration)
    if err != nil {
        return "", err
    }
    
    // Cache with expiry
    tc.cache.Store(endpoint, &cachedToken{
        token:     token,
        expiresAt: time.Now().Add(duration - 1*time.Minute), // Buffer
    })
    
    return token, nil
}
```

## Connection Rate Limiting

DSQL has cluster-wide connection limits (100 new connections/sec, 1000 burst, 10000 max). Rate limiting only applies to NEW connection establishment (TCP/TLS handshake + IAM authentication), not to queries.

The plugin supports three rate limiting modes, selected via environment variables. In reservoir mode, the rate limiter controls refiller pacing. In non-reservoir mode, it's integrated into the token-refreshing driver's `Open()` method.

| Mode | When Used | Key Config |
|------|-----------|------------|
| Token Bucket (recommended) | `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=true` + `DSQL_TOKEN_BUCKET_ENABLED=true` | DynamoDB-backed, supports burst capacity (1000) |
| Per-Second Counter | `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=true` + `DSQL_TOKEN_BUCKET_ENABLED=false` | DynamoDB-backed, no burst support |
| Local | `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=false` (default) | Per-instance `rate.Limiter`, requires manual budget partitioning |

See [Reservoir Design — Distributed Rate Limiting](reservoir-design.md#distributed-rate-limiting) for architecture details and configuration.

## Pool Metrics Collection

Background goroutine samples connection pool statistics:

```go
func (m *dsqlMetricsImpl) StartPoolCollector(db *sql.DB, interval time.Duration) {
    go func() {
        ticker := time.NewTicker(interval)
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                stats := db.Stats()
                m.poolMaxOpen.Record(float64(stats.MaxOpenConnections))
                m.poolOpen.Record(float64(stats.OpenConnections))
                m.poolInUse.Record(float64(stats.InUse))
                m.poolIdle.Record(float64(stats.Idle))
                // ... wait count and duration
            }
        }
    }()
}
```

## Non-Reservoir Mode (Fallback)

When reservoir mode is disabled (`DSQL_RESERVOIR_ENABLED=false`), the plugin falls back to pool warmup + pool keeper for connection management. This mode is retained for environments where the reservoir's DynamoDB dependencies are not available, but is not recommended for production.

- **Pool Warmup** (`pool_warmup.go`): Pre-warms the `database/sql` pool to `MaxConns` at startup by creating connections sequentially with per-connection retry and exponential backoff.
- **Pool Keeper** (`pool_keeper.go`): Background goroutine that runs every second, detects pool deficits caused by `MaxConnLifetime` expiry, and creates replacement connections (capped at `MaxConnsPerTick`).
- **`MaxConnLifetime`** is set (default 10 min) so Go's pool rotates connections. The keeper refills the deficit.
- **`MaxConnIdleTime`** must be 0 to prevent idle decay.

In reservoir mode, both are bypassed — the reservoir owns connection lifecycle and `MaxConnLifetime` / `MaxConnIdleTime` are set to 0.

## Connection Reservoir

The Connection Reservoir is an advanced connection management mode that addresses the fundamental mismatch between DSQL's cluster-wide connection rate limit (100 connections/second) and the bursty nature of connection pool refill when connections expire due to `MaxConnLifetime`.

### Problem Statement

When connections expire due to `MaxConnLifetime`, the pool needs to replace them. If many connections expire at once (burst expiry), the pool shrinks because the refill rate can't keep up with the rate limit. The reservoir solves this by maintaining a buffer of pre-created connections that can be instantly handed out without waiting for rate limiters.

### Reservoir Architecture

```
                    Global Rate Limit (100/sec)
                           │
                           ▼
┌──────────────────────────────────────────────────┐
│              RESERVOIR (per service)             │
│                                                  │
│  Continuously filled by background refiller      │
│  Maintains buffer of "ready" connections         │
│                                                  │
│  ┌─────────────────────────────────────────┐    │
│  │  Channel buffer (capacity = targetReady) │    │
│  │  [conn1] [conn2] [conn3] ... [connN]     │    │
│  └─────────────────────────────────────────┘    │
│                                                  │
└──────────────────────────────────────────────────┘
                           │
                           ▼ (instant - no rate limit)
┌──────────────────────────────────────────────────┐
│              POOL (Go's database/sql)            │
│                                                  │
│  Calls driver.Open() when it needs a connection  │
│  Driver returns connection from reservoir        │
│  No waiting for rate limit                       │
└──────────────────────────────────────────────────┘
```

### Key Components

#### Reservoir (`driver/reservoir.go`)

The reservoir is a channel-based buffer of physical connections:

```go
type Reservoir struct {
    ready       chan *PhysicalConn  // Buffered channel
    guardWindow time.Duration       // Discard if remaining lifetime within this window
    leaseRel    LeaseReleaser       // For releasing global leases
    logFunc     LogFunc             // Structured logging
    metrics     ReservoirMetrics
}

type PhysicalConn struct {
    Conn      driver.Conn
    CreatedAt time.Time     // When connection was established
    Lifetime  time.Duration // Total lifetime (base + jitter)
    LeaseID   string        // For global connection count tracking
}
```

**Key Operations:**

- `TryCheckout(now)` - Non-blocking checkout from channel
- `WaitCheckout(timeout)` - Brief blocking wait (available on the type, but not used by the driver)
- `Return(pc, now)` - Non-blocking return to channel (discard if full or expired)
- `ScanAndEvict(now)` - Drain, discard expired/expiring connections, return valid ones

**Design Decisions:**

1. **Channel-based**: Using a buffered channel provides natural FIFO ordering and thread-safe access without explicit locking for the hot path.

2. **Guard Window**: Connections within `guardWindow` of expiry are discarded on checkout/return. This prevents handing out connections that will expire mid-transaction.

3. **Non-blocking Return**: If the channel is full, returned connections are discarded. This prevents blocking the caller.

4. **Age Tracking**: Connection age is tracked from creation time rather than using a fixed expiry time. This allows computing remaining lifetime at checkout, which is more accurate when connections sit in the reservoir for varying amounts of time.

### Reservoir Configuration

The reservoir is configured entirely through environment variables, allowing operators to tune behavior without code changes.

#### Core Configuration

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `DSQL_RESERVOIR_ENABLED` | `false` | Boolean | Enable reservoir mode. When `false`, uses standard token-refreshing driver. |
| `DSQL_RESERVOIR_TARGET_READY` | `maxOpen` | Integer | Target number of connections to maintain in the reservoir. |
| `DSQL_RESERVOIR_LOW_WATERMARK` | `maxOpen` | Integer | Below this threshold, the refiller is considered in deficit. Used for initial fill synchronization. |
| `DSQL_RESERVOIR_BASE_LIFETIME` | `11m` | Duration | Base lifetime for connections before they are discarded. |
| `DSQL_RESERVOIR_LIFETIME_JITTER` | `2m` | Duration | Random jitter added to each connection's lifetime. |
| `DSQL_RESERVOIR_GUARD_WINDOW` | `45s` | Duration | Time before expiry when connections are considered too old. |
| `DSQL_RESERVOIR_INFLIGHT_LIMIT` | `8` | Integer | Max concurrent Open() calls in the refiller. Prevents handshake pile-ups. |
| `DSQL_RESERVOIR_INITIAL_FILL_TIMEOUT` | `30s` | Duration | Max time to wait for reservoir to reach low watermark at startup. |

#### Distributed Connection Lease Configuration

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED` | `false` | Boolean | Enable DynamoDB-backed global connection count limiting. |
| `DSQL_DISTRIBUTED_CONN_LEASE_TABLE` | - | String | DynamoDB table name for lease tracking. |
| `DSQL_DISTRIBUTED_CONN_LIMIT` | `10000` | Integer | Maximum connections allowed cluster-wide. |

#### Configuration Example

```bash
# Enable reservoir mode
export DSQL_RESERVOIR_ENABLED=true
export DSQL_RESERVOIR_TARGET_READY=50
export DSQL_RESERVOIR_LOW_WATERMARK=50
export DSQL_RESERVOIR_BASE_LIFETIME=11m
export DSQL_RESERVOIR_LIFETIME_JITTER=2m
export DSQL_RESERVOIR_GUARD_WINDOW=45s
export DSQL_RESERVOIR_INFLIGHT_LIMIT=8

# Optional: Enable distributed connection leasing
export DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true
export DSQL_DISTRIBUTED_CONN_LEASE_TABLE=temporal-dsql-conn-lease
export DSQL_DISTRIBUTED_CONN_LIMIT=10000
```

For detailed configuration guidance, see [Reservoir Design - Configuration](reservoir-design.md#configuration).

### Reservoir Driver

The reservoir driver implements `driver.Driver` and sources connections from the reservoir:

```go
// ErrReservoirEmpty is returned when the reservoir has no connections available.
// This is a transient backpressure signal, NOT a corrupted connection error.
// IMPORTANT: This error intentionally does NOT wrap driver.ErrBadConn to prevent
// DatabaseHandle.ConvertError from triggering a full pool recreation.
var ErrReservoirEmpty = errors.New("dsql reservoir empty: no connections available")

func (d *reservoirDriver) Open(_ string) (driver.Conn, error) {
    now := time.Now().UTC()
    
    // Strictly non-blocking checkout - never wait
    pc, ok := d.res.TryCheckout(now)
    if ok {
        return newReservoirConn(d.res, pc), nil
    }
    
    // Reservoir empty - return sentinel error (NOT driver.ErrBadConn)
    // This prevents DatabaseHandle.ConvertError from triggering pool recreation cascade
    return nil, ErrReservoirEmpty
}
```

**Design Decisions:**

1. **ErrReservoirEmpty (not ErrBadConn)**: When the reservoir is empty, we return a custom sentinel error instead of `driver.ErrBadConn`. This is critical because `ErrBadConn` signals a corrupted connection, which causes `DatabaseHandle.ConvertError` to recreate the entire connection pool — spawning a new reservoir and leaking the old refiller goroutines. `ErrReservoirEmpty` signals transient backpressure and is handled as a retryable error at the persistence layer.

2. **Strictly Non-Blocking**: `Open()` never blocks. It either returns a connection from the channel immediately or returns `ErrReservoirEmpty`. All potentially blocking work (rate limiting, TCP/TLS handshakes) happens in the background refiller.

3. **DSN Ignored**: The DSN parameter is ignored because connections are pre-created by the refiller.

### Reservoir Refiller

The refiller is a background goroutine that continuously fills the reservoir:

```go
func (r *reservoirRefiller) loop() {
    for {
        ready := r.res.Len()
        need := r.cfg.TargetReady - ready

        if need <= 0 {
            // At target capacity - brief check interval
            sleepOrStop(r.stopC, IdleCheckInterval)
            continue
        }

        // Create one connection - rate limiter controls pacing
        err := r.openOne(ctx)
        if err != nil {
            sleepOrStop(r.stopC, FailureBackoff)
            continue
        }
        // No delay - immediately try to create next connection
        // Rate limiter is the only throttle
    }
}
```

**Refiller Pacing:**

The refiller uses a simple loop where the rate limiter is the **only throttle**. There are no artificial delays, no warmup/steady-state modes. When the reservoir is below target, the refiller runs back-to-back `openOne()` calls as fast as the rate limiter allows. When at target, it checks every 100ms.

**Key constants:**

| Constant | Value | Purpose |
|----------|-------|---------|
| `IdleCheckInterval` | 100ms | How often to check when at target |
| `FailureBackoff` | 250ms | Backoff after a failed connection attempt |
| `ExpiryScanInterval` | 1s | How often the expiry scanner runs |

### Distributed Connection Leasing

The reservoir can optionally use DynamoDB-backed global connection count limiting to enforce DSQL's connection limit (default 10,000, can be raised) across all services.

**Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED` | `false` | Enable global conn limiting |
| `DSQL_DISTRIBUTED_CONN_LEASE_TABLE` | - | DynamoDB table name |
| `DSQL_DISTRIBUTED_CONN_LIMIT` | `10000` | Global connection limit |

**DynamoDB Schema:**

| Item Type | Partition Key | Attributes |
|-----------|---------------|------------|
| Counter | `dsqllease_counter#<endpoint>` | `active` (count), `updated_ms` |
| Lease | `dsqllease#<endpoint>#<leaseID>` | `ttl_epoch`, `service_name`, `created_ms` |

**Operations:**

- `Acquire(ctx)` - TransactWriteItems: increment counter (if < limit) + put lease item
- `Release(ctx, leaseID)` - TransactWriteItems: delete lease item + decrement counter

**Design Decisions:**

1. **Two-Item Approach**: Counter item for fast limit checking, lease items for TTL cleanup. This allows atomic acquire/release while enabling automatic cleanup of crashed services.

2. **TTL Cleanup**: Lease items have TTL (3 minutes). If a service crashes, its leases are automatically cleaned up.

3. **Conditional Update**: Counter increment uses `ConditionExpression: active < limit` to enforce the global limit atomically.

### Reservoir Connection Wrapper

The `reservoirConn` wraps a physical connection and returns it to the reservoir on `Close()`:

```go
func (c *reservoirConn) Close() error {
    if c.closed.Swap(true) {
        return nil  // Already closed
    }
    if c.bad.Load() {
        // Mark as expired to force discard
        c.pc.Lifetime = 0
    }
    c.r.Return(c.pc, time.Now().UTC())
    return nil
}
```

**Design Decisions:**

1. **Bad Connection Tracking**: If any operation on the underlying connection returns `driver.ErrBadConn` (from pgx), the `bad` flag is set. On `Close()`, bad connections have their lifetime zeroed so the reservoir discards them rather than returning them to the buffer.

2. **Interface Forwarding**: The wrapper forwards all optional driver interfaces (`ConnBeginTx`, `ExecerContext`, `QueryerContext`, `Pinger`, `SessionResetter`, `Validator`) to the underlying connection.

### Initial Fill Synchronization

When reservoir mode is enabled, the plugin waits for the reservoir to reach the low watermark before returning the connection:

```go
// In plugin.go, after registering reservoir driver:
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
for res.Len() < cfg.LowWatermark {
    select {
    case <-ctx.Done():
        logger.Warn("Reservoir initial fill timeout")
        break
    case <-time.After(100 * time.Millisecond):
    }
}
```

### Startup Logs

On successful reservoir startup, you'll see:

```
Waiting for reservoir initial fill  target=50 timeout=30s
Reservoir initial fill complete  current=50 target=50 elapsed=5.2s
DSQL reservoir mode enabled  reservoir_target_ready=50 reservoir_low_watermark=50 reservoir_base_lifetime=11m0s
Reservoir driver Open() - connection checked out  open_count=1 reservoir_ready=49
```

### When to Use Reservoir Mode

**Use reservoir mode when:**
- Running high-throughput workloads (>100 WPS)
- Multiple services share a DSQL cluster
- Connection expiry causes latency spikes
- Pool shrinkage is observed during burst expiry

**Stick with standard mode when:**
- Running low-throughput workloads
- Single service with dedicated DSQL cluster
- Connection pool is stable without intervention

## Pool Simulator

A discrete event simulator is available for validating reservoir behavior under various conditions without requiring a real DSQL cluster.

### Running the Simulator

```bash
# Run with default config (local development)
go run ./tools/poolsim

# Run with ECS 150 WPS scenario
go run ./tools/poolsim -config tools/poolsim/scenarios/ecs-150wps.yaml

# Output to CSV for analysis
go run ./tools/poolsim -config tools/poolsim/scenarios/local.yaml -out results.csv
```

### What It Models

- **Reservoir**: Channel-based buffer with guard window validation
- **Continuous Refiller**: Back-to-back connection creation with rate limiter as only throttle
- **Expiry Scanner**: Proactive eviction of expired/expiring connections
- **Rate Limiting**: DSQL's cluster-wide 100 connections/second limit
- **Workload Simulation**: Database queries consuming connections

### Assertions

The simulator validates behavior with configurable assertions:

| Assertion | Description |
|-----------|-------------|
| `assertMaxConnectsPerSec` | Global connection rate must not exceed 100/sec |
| `assertConvergeWithin` | All reservoirs must reach target within this duration |
| `assertStableFor` | After reaching target, reservoirs must stay stable |
| `assertZeroEmptyEvents` | No checkout attempts should find empty reservoir |

### Pre-Configured Scenarios

| Scenario | Services | Connections | Use Case |
|----------|----------|-------------|----------|
| `local.yaml` | 4 | 200 | Local development |
| `ecs-150wps.yaml` | 20 | 2,000 | ECS 150 WPS deployment |
| `ecs-400wps.yaml` | 44 | 22,000 | ECS 400 WPS deployment |

See `tools/poolsim/README.md` for full documentation.

## Testing

```bash
# Run DSQL-specific tests
go test ./common/persistence/sql/sqlplugin/dsql/...

# Run with verbose output
go test -v ./common/persistence/sql/sqlplugin/dsql/...

# Integration tests (requires DSQL cluster)
export CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
export REGION="us-east-1"
go test ./common/persistence/sql/sqlplugin/dsql/... -tags integration
```
