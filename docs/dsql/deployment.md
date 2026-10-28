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
schema_endpoint="your-cluster.dsql.us-east-1.on.aws"
schema_region="us-east-1"

# Initial setup
./temporal-dsql-tool \
    --endpoint "$schema_endpoint" \
    --region "$schema_region" \
    --user admin \
    setup-schema \
    --schema-name "dsql/temporal" \
    --version 1.2

# Full reset (drops and recreates — use with caution)
./temporal-dsql-tool \
    --endpoint "$schema_endpoint" \
    --region "$schema_region" \
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
    --endpoint "$schema_endpoint" \
    --region "$schema_region" \
    --user admin \
    update-schema \
    --schema-name "dsql/temporal" \
    --target-version 1.2
```

v1.2 drops the global unique index on `buffered_events(id)`, so the server can number each run's buffered events from 1. The previous server version also runs on a 1.2 schema, and the new version refuses to start until the schema is at 1.2.

### Upgrading from v1.0

```bash
./temporal-dsql-tool \
    --endpoint "$schema_endpoint" \
    --region "$schema_region" \
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
  visibilityStore: visibility
  datastores:
    dsql-default:
      sql:
        pluginName: "dsql"
        user: "admin" # Or temporal after completing the least-privilege role setup
        databaseName: "postgres" # a DSQL cluster has one database, named postgres
        connectAddr: "your-cluster.dsql.us-east-1.on.aws:5432"
        connectAttributes:
          token_duration: "15m"
          rate_limiter: "local"
          connection_rate_limit: "10"
          connection_burst_limit: "100"
        maxConns: 50
        maxConnLifetime: "50m"
    visibility: # a separate, durable store; DSQL doesn't support visibility
      sql:
        pluginName: "postgres12"
        databaseName: "temporal_visibility"
        connectAddr: "${VISIBILITY_ENDPOINT}:5432"
        user: "temporal"
        password: "${VISIBILITY_PASSWORD}"
```

Visibility can use any store Temporal supports except DSQL, such as Elasticsearch, OpenSearch, PostgreSQL or MySQL, configured as for upstream Temporal. The in-memory SQLite visibility in [`config/development-dsql.yaml`](../../config/development-dsql.yaml) loses its data on restart, so use it only for trying things out.

`maxIdleConns` is ignored. Service pools open and keep `maxConns` connections in the background (100 if unset). `maxConnLifetime` is capped at 50 minutes; shorter positive values apply. The pool uses four minutes of lifetime jitter and one-minute health checks. See [connection pool](connection-pool.md) for lifecycle details and limitations.

Pool sizing can also use `TEMPORAL_SQL_MAX_CONNS` where the deployment template maps it to `maxConns`. Count every pool in every Temporal process: their total must fit the cluster connection limit (10,000 by default), with headroom for startup pools and rolling replacements.

### Datastore settings and legacy fallbacks

Put plugin settings under `persistence.datastores.<store>.sql.connectAttributes`, with string values. They are resolved once when a datastore pool is created. Plugin keys are removed from a copy before building the PostgreSQL DSN; other attributes, including `sslmode`, pass through. The caller's config is unchanged. Visibility configuration is unaffected.

| Attribute | Default | Meaning |
|---|---|---|
| `region` | Endpoint region, then legacy/SDK fallbacks below | Region for IAM signing and the DynamoDB client |
| `cluster_endpoint` | Host in `connectAddr` | IAM token host and distributed limiter key; does not change the network address |
| `token_duration` | `15m` | Requested token lifetime, from `15s` through `15m` inclusive |
| `connection_rate_limit` | `10` | Local connections/sec, including distributed-mode outage fallback |
| `connection_burst_limit` | `100` | Local burst capacity, including outage fallback |
| `rate_limiter` | `local` | `local`, `dynamodb_token_bucket` or `dynamodb_counter` |
| `rate_limiter_table` | None | Required DynamoDB table for either distributed mode |
| `token_bucket_rate` | `100` | Token bucket connections/sec |
| `token_bucket_capacity` | `1000` | Token bucket burst capacity |
| `token_bucket_max_wait` | `30s` | Maximum reservation wait |
| `counter_limit` | `100` | Counter connections/sec |
| `counter_max_wait` | `30s` | Maximum counter permit wait |

Attributes override their legacy environment variables, which override defaults. Region has a special order: `region` attribute, region in the resolved endpoint (`<cluster-id>.dsql.<region>.on.aws`), `REGION`, then `AWS_REGION`. Otherwise startup fails with instructions for setting it. Inferring the region before environment fallbacks signs tokens for the cluster's own region. Endpoint order is `cluster_endpoint`, `CLUSTER_ENDPOINT`, then the host in `connectAddr`.

Integers and durations must parse and be positive, even when their mode is inactive. `token_duration` must also stay in its allowed range. An unknown mode or a DynamoDB mode without a table fails `CreateDB`, naming the setting and value. Valid settings unused by the chosen mode log a warning; local rate and burst are also used for distributed-mode fallback. A runtime failure to create the DynamoDB client still logs an error and falls back locally.

The connection limiter remains shared by all pools in a process: the first pool's settings win. A later pool requesting different effective limiter settings gets one warning naming the retained and requested settings. Use consistent limiter settings, regions and endpoints across pools that share a distributed limiter. Token cache entries remain distinct per endpoint, region, user and duration.

The following environment variables still work as fallbacks but are **deprecated** and will be removed in a future release. One warning per process lists all deprecated variables set, with their replacement attributes, including overridden or empty variables. Empty legacy values still mean unset. `AWS_REGION` remains the SDK's standard fallback and is not deprecated or warned about.

| Deprecated variable | Replacement in `connectAttributes` |
|---|---|
| `REGION` | `region` (usually inferred from the endpoint) |
| `CLUSTER_ENDPOINT` | `cluster_endpoint` (usually unnecessary) |
| `DSQL_TOKEN_DURATION` | `token_duration` |
| `DSQL_CONNECTION_RATE_LIMIT` | `connection_rate_limit` |
| `DSQL_CONNECTION_BURST_LIMIT` | `connection_burst_limit` |
| `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED` | `rate_limiter`: false/unset → `local`; true → a DynamoDB mode |
| `DSQL_TOKEN_BUCKET_ENABLED` | `rate_limiter`: with distributed enabled, true → `dynamodb_token_bucket`, false/unset → `dynamodb_counter` |
| `DSQL_DISTRIBUTED_RATE_LIMITER_TABLE` | `rate_limiter_table` |
| `DSQL_DISTRIBUTED_RATE_LIMITER_LIMIT` | `counter_limit` |
| `DSQL_DISTRIBUTED_RATE_LIMITER_MAX_WAIT` | `counter_max_wait` |
| `DSQL_TOKEN_BUCKET_RATE` | `token_bucket_rate` |
| `DSQL_TOKEN_BUCKET_CAPACITY` | `token_bucket_capacity` |
| `DSQL_TOKEN_BUCKET_MAX_WAIT` | `token_bucket_max_wait` |

Legacy enable variables must be valid booleans unless `rate_limiter` overrides them. Before rollout, move settings into each datastore's map and correct any values previously ignored by permissive parsing. For rollback to an older image, remove the new plugin attributes and restore the legacy variables: older images send these attributes to PostgreSQL as unknown runtime parameters. This configuration migration requires no schema change.

### TLS verification

The DSQL plugin defaults to `sslmode=verify-full`, verifying both the server certificate and the cluster hostname. The certificate is checked against the host in `connectAddr`, so use the cluster's endpoint host name there, not an IP address or another alias. This applies even when `tls.enabled` or `tls.enableHostVerification` is false or omitted. DSQL accepts only TLS. Each host needs a CA bundle trusting Amazon's certificate authorities; standard bundles and usual base images provide it. `tls.caFile` supplies a custom root bundle through `sslrootcert`; client certificate handling is unchanged.

An explicit `connectAttributes.sslmode` overrides this default. A value other than `verify-full` emits one warning because full certificate and hostname verification is no longer assured. Use this escape hatch only deliberately. The schema tool likewise defaults to `verify-full` and accepts `--sslmode` for an explicit override.

## 3. Dynamic Configuration

These are the dynamic config keys for DSQL deployments. Values shown are from the bench environment (200–800 WPS); scale down for lighter workloads. Keep the DSQL history batch and payload limits as shown.

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

The [development dynamic config](../../config/dynamicconfig/development-dsql.yaml) is a smaller starting point.

## 4. Connection Pool

pgxpool is always used. Startup performs a single bounded ping and returns while the service pool fills in the background. A process-wide semaphore permits eight concurrent limiter/token calls across all pools and releases before handshakes. The existing limiter paces startup and replacement connections.

IAM settings are `connectAttributes.region`, `cluster_endpoint` (defaulting to the host in `connectAddr`) and `token_duration` (default 15 minutes). The region is inferred from a DSQL endpoint when no attribute is set; see [settings and legacy fallbacks](#datastore-settings-and-legacy-fallbacks). The cache derives expiry from the signed token, reserving 10% of its signed lifetime (at least five seconds) for opening a connection. Background checks refresh at 80% of the signed lifetime. A newly generated token inside the margin can be used while still unexpired, with a warning at most once a minute. Credentials refresh four to five minutes before expiry. Token duration does not determine established connection lifetime.

The old reservoir, pool warmup and pool keeper settings are ignored with one warning listing those present. See the [complete removed-variable list and rollout instructions](connection-pool.md#migration-from-the-old-layer).

## 5. Distributed Coordination (Multi-Service)

DynamoDB-backed rate limiting can coordinate multiple Temporal processes. It remains opt-in; token bucket mode uses GCRA reservations with monotonic shared state under clock skew.

### Token Bucket Rate Limiting

Coordinates the cluster-wide 100 connections/sec rate limit across all service instances. Takes advantage of DSQL's burst capacity (1,000 connections) for fast startup.

```yaml
# Under persistence.datastores.<store>.sql:
connectAttributes:
  rate_limiter: "dynamodb_token_bucket"
  rate_limiter_table: "temporal-dsql-rate-limiter"
  token_bucket_rate: "100"       # Sustained connections/sec
  token_bucket_capacity: "1000"  # Burst capacity
  token_bucket_max_wait: "30s"   # Maximum reservation wait
```

Select `connectAttributes.rate_limiter: "dynamodb_counter"` for a simple per-second counter without burst support. Set `counter_limit` (default 100/sec) and `counter_max_wait` (default 30s) in the same map. Both DynamoDB modes require a nonempty `rate_limiter_table`; a missing table fails startup.

Both distributed modes use the local `ConnectionRateLimiter` for 30 seconds after DynamoDB API, throttling or network errors, then retry DynamoDB. The local controls are `connectAttributes.connection_rate_limit` (default 10/sec) and `connectAttributes.connection_burst_limit` (default 100). During an outage, every process paces itself locally, so the fleet may exceed DSQL's limits; DSQL rejects the excess with 53400 (not attempted). Lower the local burst if less excess is desired. Condition failures, the GCRA horizon timeout and cancellation do not cause fallback. Warnings are limited to once a minute; monitor `dsql_rate_limiter_fallbacks_total`. Startup fallback on client initialization failure is retained.

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

## 6. Multi-Service Configuration

Putting it all together for a multi-service deployment:

```yaml
persistence:
  datastores:
    dsql-default:
      sql:
        pluginName: "dsql"
        databaseName: "postgres"
        connectAddr: "your-cluster.dsql.us-east-1.on.aws:5432"
        user: "admin"
        maxConns: 50
        maxConnLifetime: "50m"
        connectAttributes:
          token_duration: "14m"
          rate_limiter: "dynamodb_token_bucket"
          rate_limiter_table: "temporal-dsql-rate-limiter"
          token_bucket_rate: "100"
          token_bucket_capacity: "1000"
```

Merge this datastore with the separate visibility configuration from section 2.

### Per-Service Pool Sizing

Each service pool opens its full `maxConns` allocation at startup and keeps it. A process can own several pools, so `maxConns × replicas` alone can undercount. Sum all pools across all processes, and reserve capacity for overlapping old/new replicas during rollout. Do not exceed the cluster connection limit (10,000 by default). Ephemeral pools add at most five connections each while startup work runs.

### ECS Task Definition

Package a complete Temporal configuration file based on the YAML above into the image at `/etc/temporal/config/dsql.yaml`, or mount it at that path. Set DSQL settings in its datastore's `sql.connectAttributes`. The task role supplies AWS credentials. For example, these fields in the container definition select that config:

```json
{
  "name": "temporal-history",
  "image": "<temporal-image>",
  "essential": true,
  "entryPoint": ["/usr/local/bin/temporal-server"],
  "command": [
    "--config-file", "/etc/temporal/config/dsql.yaml",
    "start", "--service", "history"
  ]
}
```

Use the server binary path in your image. DSQL plugin attributes are read from this file; the upstream environment-based template does not populate them.

### Docker Compose

```yaml
services:
  temporal-history:
    image: "<temporal-image>"
    entrypoint: ["/usr/local/bin/temporal-server"]
    command: ["--config-file", "/etc/temporal/config/dsql.yaml", "start", "--service", "history"]
    volumes:
      - "./dsql.yaml:/etc/temporal/config/dsql.yaml:ro"
```

## Verification

### Check Connectivity

```bash
TOKEN=$(aws dsql generate-db-connect-admin-auth-token \
    --hostname "$schema_endpoint" \
    --region "$schema_region")

psql "host=$schema_endpoint user=admin password=$TOKEN sslmode=verify-full" \
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
| "set connectAttributes.region" | No explicit region or recognizable DSQL endpoint | Set `connectAttributes.region` or use the cluster endpoint in `connectAddr` |
| Invalid setting or missing `rate_limiter_table` | Invalid datastore configuration | Correct the named attribute or legacy variable and restart |
| "failed to resolve AWS credentials" | IAM misconfiguration | Check role/credentials, verify `dsql:DbConnect` for a custom role or `dsql:DbConnectAdmin` for admin |
| "connection rate limit exceeded" | Too many new connections | Check distributed rate limiter, stagger restarts |
| `ResourceExhausted` / 53300 / 53400 | Connection not attempted or refused at admission | Inspect limiter and cluster connection budget |

### Serialization Conflicts

OCC conflicts are expected under concurrent load. Core transactions using `txExecute` retry SQLSTATE 40001 through the plugin policy.

- Monitor `dsql_tx_conflict_total` — a sustained spike may indicate hot-spot access patterns
- Monitor `dsql_tx_exhausted_total` — retries exhausted means the conflict rate exceeds retry budget

### Rollout and rollback

**Role behavior changes:** a config whose `user` is not `admin` now connects as that role. Create and map the role before rollout, or change the config to `user: admin`. Previously the plugin ignored `user` and always connected as `admin`.

Ensure hosts have a trusted CA bundle before rolling out the new `verify-full` default. Remove password settings and unnecessary `sslmode: require` overrides to enable certificate verification.

Follow the [pool rollout checklist](connection-pool.md#rollout-and-rollback): budget every pool and overlapping replica, remove ignored settings, resolve reliance on leasing, then replace one process at a time while checking pool fill and waits. Startup does not wait for a full pool.

Rollback by deploying the previous image and restoring its previous connection configuration. This connection-layer change requires no schema migration or rollback.

### No data migration

**There is no data migration** between DSQL and any other Temporal persistence store, in either direction. The schemas differ and no tooling transfers the data.

**Switching persistence stores abandons the data.** Pointing the persistence config at another, empty store starts Temporal on that empty store and leaves every workflow, history and task queue in DSQL behind. Never use a store switch as an incident rollback.

Rolling back a release means deploying the previous image with its previous config against the same persistence store, as described above.

Moving off DSQL means running a second Temporal cluster on the other store. Start new workflows there and let existing workflows finish on the DSQL cluster before retiring it. Other approaches, including Temporal multi-cluster replication, have not been tried with DSQL.
