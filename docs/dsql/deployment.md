# Deploying Temporal with Aurora DSQL

This guide covers deploying Temporal with Aurora DSQL as the persistence layer, using the Connection Reservoir for production workloads.

## Prerequisites

- Aurora DSQL cluster (public endpoint enabled)
- IAM permissions: `dsql:DbConnectAdmin` on the cluster ARN
- AWS credentials configured (IAM role, environment variables, or credentials file)
- `temporal-dsql-tool` binary for schema setup

## 1. Schema Setup

**Current schema version: 1.2**

```bash
export CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
export REGION="us-east-1"

# Initial setup
./temporal-dsql-tool \
    --endpoint "$CLUSTER_ENDPOINT" \
    --region "$REGION" \
    setup-schema \
    --schema-name "dsql/temporal" \
    --version 1.2

# Full reset (drops and recreates — use with caution)
./temporal-dsql-tool \
    --endpoint "$CLUSTER_ENDPOINT" \
    --region "$REGION" \
    setup-schema \
    --schema-name "dsql/temporal" \
    --version 1.2 \
    --overwrite
```

The tool uses IAM authentication automatically and has the DSQL schema embedded.

### Upgrading from v1.1

Apply the schema change before you deploy the new server:

```bash
./temporal-dsql-tool \
    --endpoint "$CLUSTER_ENDPOINT" \
    --region "$REGION" \
    update-schema \
    --schema-name "dsql/temporal" \
    --target-version 1.2
```

v1.2 drops the global unique index on `buffered_events(id)`, so the server can number each run's buffered events from 1. The previous server version also runs on a 1.2 schema, and the new version refuses to start until the schema is at 1.2.

### Upgrading from v1.0

```bash
./temporal-dsql-tool \
    --endpoint "$CLUSTER_ENDPOINT" \
    --region "$REGION" \
    update-schema \
    --schema-name "dsql/temporal" \
    --target-version 1.2
```

v1.1 adds the `current_chasm_executions` table for CHASM support (standalone activities, schedulers). This upgrade currently fails at the v1.1 step ([#28](https://github.com/iw/temporal-dsql/issues/28)).

## 2. Static Configuration

```yaml
persistence:
  defaultStore: dsql-default
  datastores:
    dsql-default:
      sql:
        pluginName: "dsql"
        databaseName: "temporal"
        connectAddr: "${CLUSTER_ENDPOINT}:5432"
        maxConns: 50
        maxIdleConns: 50
```

`maxIdleConns` must equal `maxConns` to prevent pool decay. When using reservoir mode, the plugin automatically sets `MaxConnLifetime` and `MaxConnIdleTime` to 0 — the reservoir manages connection lifecycle instead of `database/sql`. See [Reservoir Design — Ephemeral Pool](reservoir-design.md#ephemeral-pool-design) for why.

Pool sizing can also be set via environment variables (`TEMPORAL_SQL_MAX_CONNS`, `TEMPORAL_SQL_MAX_IDLE_CONNS`), which is the typical approach for ECS/Docker deployments.

## 3. Dynamic Configuration

These are the validated dynamic config keys for DSQL deployments. Values shown are from the bench environment (200–800 WPS); scale down for lighter workloads.

```yaml
# A history batch is stored in one column, which DSQL caps at 1 MiB
system.transactionSizeLimit:
  - value: 1048576
    constraints: {}

# Reject oversized payloads at the frontend, leaving room under 1 MiB for the event envelope
limit.blobSize.error:
  - value: 1000000
    constraints: {}

# Persistence QPS per host (scale to workload)
history.persistenceMaxQPS:
  - value: 15000
    constraints: {}

matching.persistenceMaxQPS:
  - value: 15000
    constraints: {}

frontend.persistenceMaxQPS:
  - value: 15000
    constraints: {}

# History cache — critical for reducing persistence load
history.cacheSizeBasedLimit:
  - value: true
    constraints: {}

# Per-host cache size (2GB bench / 1GB prod — tune to available memory)
history.hostLevelCacheMaxSizeBytes:
  - value: 2147483648
    constraints: {}

history.cacheTTL:
  - value: 1h
    constraints: {}

history.cacheNonUserContextLockTimeout:
  - value: 500ms
    constraints: {}
```

See `temporal-dsql-deploy-ecs/docker/config/dynamicconfig-bench.yaml` for the complete bench configuration including matching tuning, frontend rate limits, and task queue partitioning.

## 4. Connection Reservoir (Production)

The Connection Reservoir is the recommended connection management mode for production. It maintains a buffer of pre-created connections so that `driver.Open()` never blocks on rate limiters.

See [Reservoir Design](reservoir-design.md) for architecture details.

### Core Settings

```bash
# Enable reservoir mode
DSQL_RESERVOIR_ENABLED=true

# Connections to keep ready (match maxConns)
DSQL_RESERVOIR_TARGET_READY=50

# Connection lifetime before replacement (must be < DSQL's 60min limit)
DSQL_RESERVOIR_BASE_LIFETIME=11m

# Jitter to prevent synchronized expiry (actual lifetime: 10m–12m)
DSQL_RESERVOIR_LIFETIME_JITTER=2m

# Discard connections with less than this remaining lifetime
DSQL_RESERVOIR_GUARD_WINDOW=45s
```

The in-flight semaphore (`DSQL_RESERVOIR_INFLIGHT_LIMIT`, default: 8) limits concurrent connection handshakes in the refiller. The default is appropriate for most deployments.

### IAM Token Settings

```bash
CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
REGION="us-east-1"
DSQL_TOKEN_DURATION="14m"   # Token validity (default: 14 minutes)
```

### Expected Startup Behavior

On startup, the refiller creates connections sequentially until the reservoir is full:

```
DSQL reservoir starting  target_ready=50 base_lifetime=11m0s jitter=2m0s guard_window=45s
DSQL reservoir refiller started
DSQL reservoir initial fill complete  ready=50 elapsed=5.2s
```

If these logs are missing, the pool will grow on-demand, causing rate limit pressure.

## 5. Distributed Coordination (Multi-Service)

For production deployments with multiple Temporal services sharing a DSQL cluster, enable DynamoDB-backed coordination.

### Token Bucket Rate Limiting

Coordinates the cluster-wide 100 connections/sec rate limit across all service instances. Takes advantage of DSQL's burst capacity (1,000 connections) for fast startup.

```bash
DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=true
DSQL_DISTRIBUTED_RATE_LIMITER_TABLE=temporal-dsql-rate-limiter

# Token bucket mode (recommended — enables burst capacity)
DSQL_TOKEN_BUCKET_ENABLED=true
DSQL_TOKEN_BUCKET_RATE=100       # Sustained rate (matches DSQL limit)
DSQL_TOKEN_BUCKET_CAPACITY=1000  # Burst capacity (matches DSQL burst)
```

Without token bucket mode, the distributed rate limiter falls back to a simple per-second counter (no burst support). The ECS terraform module currently uses the per-second counter with `DSQL_DISTRIBUTED_RATE_LIMITER_LIMIT=100`.

**DynamoDB table setup:**

```bash
aws dynamodb create-table \
  --table-name temporal-dsql-rate-limiter \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1

aws dynamodb update-time-to-live \
  --table-name temporal-dsql-rate-limiter \
  --time-to-live-specification Enabled=true,AttributeName=ttl_epoch \
  --region us-east-1
```

**IAM permissions required:**

```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:GetItem",
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem"
  ],
  "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/temporal-dsql-rate-limiter"
}
```

### Slot Block Connection Limiting

Coordinates the global connection count (DSQL default: 10,000) using block-based allocation to avoid DynamoDB hot partitions.

```bash
DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true
DSQL_DISTRIBUTED_CONN_LEASE_TABLE=temporal-dsql-conn-lease
DSQL_SLOT_BLOCK_SIZE=100    # Slots per block
DSQL_SLOT_BLOCK_COUNT=100   # Total blocks (100 × 100 = 10k slots)
DSQL_SLOT_BLOCK_TTL=3m      # TTL for crash recovery
DSQL_SLOT_BLOCK_RENEW_INTERVAL=1m
```

See [Reservoir Design — Slot Block Manager](reservoir-design.md#slot-block-manager-recommended) for architecture details.

## 6. Complete Production Configuration

Putting it all together for a multi-service deployment:

```bash
# DSQL connection
CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
REGION="us-east-1"
DSQL_TOKEN_DURATION="14m"

# Connection pool (per service instance — scale per service type)
TEMPORAL_SQL_MAX_CONNS=50
TEMPORAL_SQL_MAX_IDLE_CONNS=50

# Reservoir
DSQL_RESERVOIR_ENABLED=true
DSQL_RESERVOIR_TARGET_READY=50
DSQL_RESERVOIR_BASE_LIFETIME=11m
DSQL_RESERVOIR_LIFETIME_JITTER=2m
DSQL_RESERVOIR_GUARD_WINDOW=45s

# Distributed rate limiting (token bucket)
DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=true
DSQL_DISTRIBUTED_RATE_LIMITER_TABLE=temporal-dsql-rate-limiter
DSQL_TOKEN_BUCKET_ENABLED=true
DSQL_TOKEN_BUCKET_RATE=100
DSQL_TOKEN_BUCKET_CAPACITY=1000

# Distributed connection limiting (slot blocks)
DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true
DSQL_DISTRIBUTED_CONN_LEASE_TABLE=temporal-dsql-conn-lease
DSQL_SLOT_BLOCK_SIZE=100
DSQL_SLOT_BLOCK_COUNT=100
DSQL_SLOT_BLOCK_TTL=3m
```

### Per-Service Pool Sizing

Connection pool size should be tuned per service type. The bench environment (800 WPS target) uses:

| Service | `max_conns` | Replicas | Total Connections |
|---------|-------------|----------|-------------------|
| History | 220 | 16 | 3,520 |
| Matching | 100 | 16 | 1,600 |
| Frontend | 80 | 9 | 720 |
| Worker | 40 | 3 | 120 |
| **Total** | | | **~5,960** |

For lower throughput (100 WPS), 50 connections per service instance is a reasonable starting point.

### ECS Task Definition

This matches the actual terraform module (`modules/temporal-service`):

```hcl
environment = [
  # DSQL connection
  { name = "TEMPORAL_SQL_HOST",                       value = var.dsql_endpoint },
  { name = "TEMPORAL_SQL_PORT",                       value = "5432" },
  { name = "TEMPORAL_SQL_USER",                       value = "admin" },
  { name = "TEMPORAL_SQL_DATABASE",                   value = "postgres" },
  { name = "TEMPORAL_SQL_PLUGIN_NAME",                value = "dsql" },
  { name = "TEMPORAL_SQL_TLS_ENABLED",                value = "true" },
  { name = "TEMPORAL_SQL_IAM_AUTH",                   value = "true" },

  # Connection pool
  { name = "TEMPORAL_SQL_MAX_CONNS",                  value = "50" },
  { name = "TEMPORAL_SQL_MAX_IDLE_CONNS",             value = "50" },
  { name = "TEMPORAL_SQL_CONNECTION_TIMEOUT",          value = "30s" },

  # Reservoir
  { name = "DSQL_RESERVOIR_ENABLED",                  value = "true" },
  { name = "DSQL_RESERVOIR_TARGET_READY",             value = "50" },
  { name = "DSQL_RESERVOIR_BASE_LIFETIME",            value = "11m" },
  { name = "DSQL_RESERVOIR_LIFETIME_JITTER",          value = "2m" },
  { name = "DSQL_RESERVOIR_GUARD_WINDOW",             value = "45s" },

  # Distributed rate limiting
  { name = "DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED",   value = "true" },
  { name = "DSQL_DISTRIBUTED_RATE_LIMITER_TABLE",     value = var.dsql_rate_limiter_table },

  # Distributed connection leasing
  { name = "DSQL_DISTRIBUTED_CONN_LEASE_ENABLED",     value = "true" },
  { name = "DSQL_DISTRIBUTED_CONN_LEASE_TABLE",       value = var.dsql_conn_lease_table },
  { name = "DSQL_DISTRIBUTED_CONN_LIMIT",             value = "9500" },
]
```

### Docker Compose

```yaml
services:
  temporal-history:
    environment:
      DSQL_RESERVOIR_ENABLED: "true"
      DSQL_RESERVOIR_TARGET_READY: "50"
      DSQL_RESERVOIR_BASE_LIFETIME: "11m"
      DSQL_RESERVOIR_LIFETIME_JITTER: "2m"
      DSQL_RESERVOIR_GUARD_WINDOW: "45s"
```

## Verification

### Check Connectivity

```bash
TOKEN=$(aws dsql generate-db-connect-admin-auth-token \
    --hostname "$CLUSTER_ENDPOINT" \
    --region "$REGION")

psql "host=$CLUSTER_ENDPOINT user=admin password=$TOKEN sslmode=require" \
    -c "SELECT version();"
```

### Verify Schema

```sql
SELECT * FROM schema_version;
SELECT tablename FROM pg_tables WHERE schemaname = 'public';
```

### Test Temporal

```bash
temporal operator namespace create default
temporal workflow start --type MyWorkflow --task-queue my-queue
```

## Monitoring

Key metrics to watch in production:

| Metric | Healthy | Alert If |
|--------|---------|----------|
| `dsql_reservoir_size / dsql_reservoir_target` | > 0.9 | < 0.5 for 5 min |
| `rate(dsql_reservoir_empty_total[5m])` | 0 | > 0 for 2 min |
| `dsql_tx_conflict_total` | Low, stable | Sustained spike |
| `dsql_pool_in_use / dsql_pool_max_open` | < 0.8 | > 0.9 for 5 min |

See [Metrics Reference](metrics.md) for the complete list and alerting recommendations.

See [Reservoir Design — Troubleshooting](reservoir-design.md#troubleshooting) for diagnostic procedures.

## Troubleshooting

### Connection Errors

| Error | Cause | Solution |
|-------|-------|----------|
| "REGION or AWS_REGION must be set" | Missing env var | Set `REGION` or `AWS_REGION` |
| "failed to resolve AWS credentials" | IAM misconfiguration | Check role/credentials, verify `dsql:DbConnectAdmin` |
| "connection rate limit exceeded" | Too many new connections | Check distributed rate limiter, stagger restarts |
| `ErrReservoirEmpty` | Reservoir temporarily drained | Transient — refiller will catch up. If persistent, see [reservoir troubleshooting](reservoir-design.md#common-issues-and-solutions) |

### Serialization Conflicts

OCC conflicts are expected under concurrent load. The retry logic handles them automatically.

- Monitor `dsql_tx_conflict_total` — a sustained spike may indicate hot-spot access patterns
- Monitor `dsql_tx_exhausted_total` — retries exhausted means the conflict rate exceeds retry budget

### Rollback to PostgreSQL

Update the persistence configuration and restart services:

```yaml
persistence:
  defaultStore: postgres-default
  datastores:
    postgres-default:
      sql:
        pluginName: "postgres12"
        connectAddr: "postgres-host:5432"
```
