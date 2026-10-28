# Deploying Temporal with Aurora DSQL

This guide covers deploying Temporal with Aurora DSQL as the persistence layer, using pgxpool for connection management.

## Prerequisites

- Aurora DSQL cluster (public endpoint enabled)
- IAM permissions: `dsql:DbConnectAdmin` for schema administration; `dsql:DbConnect` for hosts using a custom database role (see below)
- AWS credentials configured (IAM role, environment variables, or credentials file)
- `temporal-dsql-tool` binary for schema setup
- A CA bundle on each host that trusts Amazon's certificate authorities; standard bundles and usual base images include these roots

## 1. Schema Setup

**Current schema version: 1.2**

```bash
export CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
export REGION="us-east-1"

# Initial setup
./temporal-dsql-tool \
    --endpoint "$CLUSTER_ENDPOINT" \
    --region "$REGION" \
    --user admin \
    setup-schema \
    --schema-name "dsql/temporal" \
    --version 1.2

# Full reset (drops and recreates — use with caution)
./temporal-dsql-tool \
    --endpoint "$CLUSTER_ENDPOINT" \
    --region "$REGION" \
    --user admin \
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
    --user admin \
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
    --user admin \
    update-schema \
    --schema-name "dsql/temporal" \
    --target-version 1.2
```

v1.1 adds the `current_chasm_executions` table for CHASM support (standalone activities, schedulers).

### Restore retention on existing system namespaces

New clusters create `temporal-system` with seven days of retention. Clusters created
by earlier fork versions may still have zero retention; upgrading the server does
not update an existing namespace. Apply this one-time correction:

```bash
temporal operator namespace update --namespace temporal-system --retention 7d
```

The frontend does not special-case `temporal-system`, but this command has not been
tried against a live cluster. It cannot recover history that has already been deleted.

### Least-privilege role

After schema setup, connect as `admin` and run these statements individually to create a runtime role and map it to the Temporal hosts' IAM role:

```sql
CREATE ROLE temporal WITH LOGIN;
AWS IAM GRANT temporal TO 'arn:aws:iam::<account-id>:role/<temporal-role>';
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO temporal;
```

No schema grant is needed: every role already has `USAGE` on `public`, and DSQL rejects `GRANT ... ON SCHEMA public` because `public` is a system schema.

Configure `sql.user: temporal`. Each Temporal host's IAM identity needs `dsql:DbConnect` on the cluster ARN instead of `dsql:DbConnectAdmin`. Schema setup and upgrades still run as `admin` (`temporal-dsql-tool --user admin`) with the administrative IAM permission. Tables created later need the table grant again after setup or an upgrade.

An empty `sql.user` defaults to `admin`. The plugin ignores `sql.password`, because it uses IAM tokens, and warns once if a password is configured.

## 2. Static Configuration

```yaml
persistence:
  defaultStore: dsql-default
  datastores:
    dsql-default:
      sql:
        pluginName: "dsql"
        user: "admin" # Or temporal after completing the least-privilege role setup
        databaseName: "temporal"
        connectAddr: "${CLUSTER_ENDPOINT}:5432"
        maxConns: 50
        maxConnLifetime: "50m"
```

`maxIdleConns` is ignored. Service pools open and keep `maxConns` connections in the background (100 if unset). `maxConnLifetime` is capped at 50 minutes; shorter positive values apply. The pool uses four minutes of lifetime jitter and one-minute health checks. See [connection pool](connection-pool.md) for lifecycle details and limitations.

Pool sizing can also use `TEMPORAL_SQL_MAX_CONNS` where the deployment template maps it to `maxConns`. Count every pool in every Temporal process: their total must fit the cluster connection limit (10,000 by default), with headroom for startup pools and rolling replacements.

### TLS verification

The DSQL plugin defaults to `sslmode=verify-full`, verifying both the server certificate and the cluster hostname. The certificate is checked against the host in `connectAddr`, so use the cluster's endpoint host name there, not an IP address or another alias. This applies even when `tls.enabled` or `tls.enableHostVerification` is false or omitted. DSQL accepts only TLS. Each host needs a CA bundle trusting Amazon's certificate authorities; standard bundles and usual base images provide it. `tls.caFile` supplies a custom root bundle through `sslrootcert`; client certificate handling is unchanged.

An explicit `connectAttributes.sslmode` overrides this default. A value other than `verify-full` emits one warning because full certificate and hostname verification is no longer assured. Use this escape hatch only deliberately. The schema tool likewise defaults to `verify-full` and accepts `--sslmode` for an explicit override.

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

## 4. Connection Pool

pgxpool is always used. Startup performs a single bounded ping and returns while the service pool fills in the background. A process-wide semaphore permits eight concurrent limiter/token calls across all pools and releases before handshakes. The existing limiter paces startup and replacement connections.

IAM settings remain `REGION` or `AWS_REGION`, `CLUSTER_ENDPOINT` (or the configured address), and `DSQL_TOKEN_DURATION` (default 15 minutes). The cache derives expiry from the signed token, reserving 10% of its signed lifetime (at least five seconds) for opening a connection. Background checks refresh at 80% of the signed lifetime. A newly generated token inside the margin can be used while still unexpired, with a warning at most once a minute. Credentials refresh four to five minutes before expiry. Token duration does not determine established connection lifetime.

The old reservoir, pool warmup and pool keeper settings are ignored with one warning listing those present. See the [complete removed-variable list and rollout instructions](connection-pool.md#migration-from-the-old-layer).

## 5. Distributed Coordination (Multi-Service)

DynamoDB-backed rate limiting can coordinate multiple Temporal processes. It remains opt-in; token bucket mode uses GCRA reservations with monotonic shared state to address [#40](https://github.com/iw/temporal-dsql/issues/40).

### Token Bucket Rate Limiting

Coordinates the cluster-wide 100 connections/sec rate limit across all service instances. Takes advantage of DSQL's burst capacity (1,000 connections) for fast startup.

```bash
DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=true
DSQL_DISTRIBUTED_RATE_LIMITER_TABLE=temporal-dsql-rate-limiter

# Token bucket mode (recommended — enables burst capacity)
DSQL_TOKEN_BUCKET_ENABLED=true
DSQL_TOKEN_BUCKET_RATE=100       # Sustained rate (matches DSQL limit)
DSQL_TOKEN_BUCKET_CAPACITY=1000  # Burst capacity (matches DSQL burst)
DSQL_TOKEN_BUCKET_MAX_WAIT=30s   # Maximum reservation wait
```

Without token bucket mode, the distributed rate limiter falls back to a simple per-second counter (no burst support). The ECS terraform module currently uses the per-second counter with `DSQL_DISTRIBUTED_RATE_LIMITER_LIMIT=100`.

Both distributed modes use the local `ConnectionRateLimiter` for 30 seconds after DynamoDB API, throttling or network errors, then retry DynamoDB. The local controls are `DSQL_CONNECTION_RATE_LIMIT` (default 10/sec) and `DSQL_CONNECTION_BURST_LIMIT` (default 100). During an outage, every process paces itself locally, so the fleet may exceed DSQL's limits; DSQL rejects the excess with 53400 (not attempted). Lower the local burst if less excess is desired. Condition failures, the GCRA horizon timeout and cancellation do not cause fallback. Warnings are limited to once a minute; monitor `dsql_rate_limiter_fallbacks_total`. Startup fallback on client initialization failure is retained.

GCRA uses `dsql_connect_gcra#<endpoint>`, leaving old `dsql_connect_bucket#<endpoint>` items for TTL cleanup. During mixed-version rollout or rollback, the separate items permit up to twice the configured rate and burst. Monitor 53400 errors and keep the mixed-version interval short. No table migration is needed.

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

New hosts need only `UpdateItem`. Retain the old permissions until all old hosts have drained during rollout. The optional live test also needs `DeleteItem` to clean up its test item.

```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:UpdateItem"
  ],
  "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/temporal-dsql-rate-limiter"
}
```

### Removed connection lease

The slot-block connection lease was removed ([#33](https://github.com/iw/temporal-dsql/issues/33)). `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true` still refuses startup. The sum of `maxConns` across every Temporal process, including deploy overlap, must fit the cluster's connection limit. Leave headroom for ephemeral pools and administration, then unset the enable variable. The other retired settings are [ignored with a warning](connection-pool.md#migration-from-the-old-layer). The lease's DynamoDB table can be deleted once no old deployment uses it; retain the rate-limiter table.

## 6. Complete Production Configuration

Putting it all together for a multi-service deployment:

```bash
# DSQL connection
CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
REGION="us-east-1"
DSQL_TOKEN_DURATION="14m"

# Connection pool (per service instance — scale per service type)
TEMPORAL_SQL_MAX_CONNS=50

# Distributed rate limiting (token bucket)
DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=true
DSQL_DISTRIBUTED_RATE_LIMITER_TABLE=temporal-dsql-rate-limiter
DSQL_TOKEN_BUCKET_ENABLED=true
DSQL_TOKEN_BUCKET_RATE=100
DSQL_TOKEN_BUCKET_CAPACITY=1000

```

### Per-Service Pool Sizing

Each service pool opens its full `maxConns` allocation at startup and keeps it. A process can own several pools, so `maxConns × replicas` alone can undercount. Sum all pools across all processes, and reserve capacity for overlapping old/new replicas during rollout. Do not exceed the cluster connection limit (10,000 by default). Ephemeral pools add at most five connections each while startup work runs.

### ECS Task Definition

Example environment for a deployment template that maps these variables to SQL config:

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
  { name = "TEMPORAL_SQL_CONNECTION_TIMEOUT",          value = "30s" },

  # Distributed rate limiting
  { name = "DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED",   value = "true" },
  { name = "DSQL_DISTRIBUTED_RATE_LIMITER_TABLE",     value = var.dsql_rate_limiter_table },

]
```

### Docker Compose

```yaml
services:
  temporal-history:
    environment:
      TEMPORAL_SQL_MAX_CONNS: "50"
      AWS_REGION: "us-east-1"
      CLUSTER_ENDPOINT: "your-cluster.dsql.us-east-1.on.aws"
```

## Verification

### Check Connectivity

```bash
TOKEN=$(aws dsql generate-db-connect-admin-auth-token \
    --hostname "$CLUSTER_ENDPOINT" \
    --region "$REGION")

psql "host=$CLUSTER_ENDPOINT user=admin password=$TOKEN sslmode=verify-full" \
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
| `dsql_pool_open - dsql_pool_constructing` | Near expected service size | Sustained deficit |
| `rate(dsql_pool_canceled_acquires_total[5m])` | Low | Sustained increase |
| `dsql_tx_conflict_total` | Low, stable | Sustained spike |
| `dsql_pool_in_use / dsql_pool_max_open` | < 0.8 | > 0.9 for 5 min |

See [Metrics Reference](metrics.md) for the complete list and alerting recommendations.

Use pool waits, constructing connections and rate-limiter logs to distinguish saturation from failed connection creation.

## Troubleshooting

### Connection Errors

| Error | Cause | Solution |
|-------|-------|----------|
| "REGION or AWS_REGION must be set" | Missing env var | Set `REGION` or `AWS_REGION` |
| "failed to resolve AWS credentials" | IAM misconfiguration | Check role/credentials, verify `dsql:DbConnect` for a custom role or `dsql:DbConnectAdmin` for admin |
| "connection rate limit exceeded" | Too many new connections | Check distributed rate limiter, stagger restarts |
| `ResourceExhausted` / 53300 / 53400 | Connection not attempted or refused at admission | Inspect limiter and cluster connection budget |

### Serialization Conflicts

OCC conflicts are expected under concurrent load. The retry logic handles them automatically.

- Monitor `dsql_tx_conflict_total` — a sustained spike may indicate hot-spot access patterns
- Monitor `dsql_tx_exhausted_total` — retries exhausted means the conflict rate exceeds retry budget

### Rollout and rollback

**Role behavior changes:** a config whose `user` is not `admin` now connects as that role. Create and map the role before rollout, or change the config to `user: admin`. Previously the plugin ignored `user` and always connected as `admin`.

Ensure hosts have a trusted CA bundle before rolling out the new `verify-full` default. Remove password settings and unnecessary `sslmode: require` overrides to enable certificate verification.

Follow the [pool rollout checklist](connection-pool.md#rollout-and-rollback): budget every pool and overlapping replica, remove ignored settings, resolve reliance on leasing, then replace one process at a time while checking pool fill and waits. Startup does not wait for a full pool.

Rollback by deploying the previous image and restoring its previous connection configuration. This connection-layer change requires no schema migration or rollback.
