# Deploying Temporal with Aurora DSQL

This guide covers deploying Temporal with Aurora DSQL as the persistence layer, and optionally as the visibility store, using pgxpool for connection management.

> **Experimental: not recommended for production.**

## Prerequisites

- Aurora DSQL cluster (public endpoint enabled)
- IAM permissions: `dsql:DbConnectAdmin` for schema administration; `dsql:DbConnect` for hosts using a custom database role (see below)
- AWS credentials configured (IAM role, environment variables, or credentials file)
- `temporal-dsql-tool` binary for schema setup
- A CA bundle on each host that trusts Amazon's certificate authorities; standard bundles and usual base images include these roots

## 1. Schema Setup

**Current schema version: 1.3**

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
    --version 1.3

# Full reset (drops and recreates — use with caution)
./temporal-dsql-tool \
    --endpoint "$schema_endpoint" \
    --region "$schema_region" \
    --user admin \
    setup-schema \
    --schema-name "dsql/temporal" \
    --version 1.3 \
    --overwrite
```

The tool uses IAM authentication automatically and has the DSQL schema embedded.

Secondary indexes are built asynchronously (`CREATE INDEX ASYNC`), and the tool
waits for each build to finish before it runs the next statement. When a build
finishes, DSQL marks the index active in its catalog, and transactions using the
schema at that moment fail with OC001. Because of the wait, the schema is settled
when the tool returns, and the server can start straight away.

The schema is one full baseline at version 1.3. The server requires that version;
a cluster that fails the version check needs a fresh schema. The region comes
from `--region` or the endpoint's host name.

Alternatively, initialize only the version tables at 0.0, then apply the baseline:

```bash
./temporal-dsql-tool --endpoint "$schema_endpoint" setup-schema --version 0.0
./temporal-dsql-tool --endpoint "$schema_endpoint" update-schema --schema-name dsql/temporal
```

For filesystem input, use `--schema-file schema/dsql/temporal/schema.sql` with the
1.3 setup command, or `--schema-dir schema/dsql/temporal/versioned` with the update
command. Both paths create the same tables and record version 1.3.


### Visibility schema

To keep visibility on DSQL too, set up its schema on the same cluster or on another one:

```bash
./temporal-dsql-tool \
    --endpoint "$schema_endpoint" \
    --region "$schema_region" \
    --user admin \
    setup-schema \
    --schema-name "dsql/visibility" \
    --version 1.0
```

The tool creates the PostgreSQL schema `temporal_visibility` if it's missing and sets the store up in it at version 1.0, waiting for each of its 19 index builds, which takes a few minutes. `--search-path` names another schema; the visibility datastore's `search_path` connect attribute must then name the same one. A schema name is one unquoted, lowercase PostgreSQL identifier. [Visibility](visibility.md) describes the table, its indexes, the query dialect and its limits.

### Least-privilege role

After schema setup, connect as `admin` and run these statements individually to create a runtime role and map it to the Temporal hosts' IAM role:

```sql
CREATE ROLE temporal WITH LOGIN;
AWS IAM GRANT temporal TO 'arn:aws:iam::<account-id>:role/<temporal-role>';
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO temporal;
```

No schema grant is needed: every role already has `USAGE` on `public`, and DSQL rejects `GRANT ... ON SCHEMA public` because `public` is a system schema.

For a visibility store on DSQL, also grant the role its schema and tables:

```sql
GRANT USAGE ON SCHEMA temporal_visibility TO temporal;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA temporal_visibility TO temporal;
```

DSQL applies grant changes to existing connections within the connection lifetime, so grant before starting Temporal.

Configure `sql.user: temporal`. Each Temporal host's IAM identity needs `dsql:DbConnect` on the cluster ARN instead of `dsql:DbConnectAdmin`. Schema operations run as `admin` (`temporal-dsql-tool --user admin`) with the administrative IAM permission. Tables created later need the table grant again after schema creation.

An empty `sql.user` defaults to `admin`. The plugin ignores `sql.password`, because it uses IAM tokens, and warns once if a password is configured.

## 2. Static Configuration

```yaml
persistence:
  defaultStore: dsql-default
  visibilityStore: dsql-visibility
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
    dsql-visibility:
      sql:
        pluginName: "dsql"
        user: "admin" # Or temporal, with the visibility grants above
        databaseName: "postgres"
        connectAddr: "your-cluster.dsql.us-east-1.on.aws:5432"
        connectAttributes:
          search_path: "temporal_visibility" # the default for a visibility datastore
        maxConns: 10
        maxConnLifetime: "50m"
```

The visibility datastore can point at the persistence cluster or at another one. Without `search_path`, a DSQL visibility datastore uses `temporal_visibility`. See [visibility](visibility.md#limits) for the namespace sizes it suits.

Visibility can also use any other store Temporal supports, such as Elasticsearch, OpenSearch, PostgreSQL or MySQL, configured as for upstream Temporal. [`config/development-dsql.yaml`](../../config/development-dsql.yaml) keeps both stores on one cluster.

`maxIdleConns` is ignored. Service pools open and keep `maxConns` connections in the background (100 if unset). `maxConnLifetime` is capped at 50 minutes; shorter positive values apply. The pool uses four minutes of lifetime jitter and one-minute health checks. See [connection pool](connection-pool.md) for lifecycle details and limitations.

Pool sizing can also use `TEMPORAL_SQL_MAX_CONNS` where the deployment template maps it to `maxConns`. Count every pool in every Temporal process: their total must fit the cluster connection limit (10,000 by default), with headroom for startup pools and rolling replacements.

### Datastore settings

Put plugin settings under `persistence.datastores.<store>.sql.connectAttributes`, with string values. They are resolved once when a datastore pool is created. The resolver filters plugin keys from a copy before building the PostgreSQL DSN; other attributes, including `sslmode` and `search_path`, pass through. The caller's config is unchanged. The same attributes apply to a DSQL visibility datastore.

| Attribute | Default | Meaning |
|---|---|---|
| `region` | Endpoint region | Region for IAM signing and the DynamoDB client |
| `cluster_endpoint` | Host in `connectAddr` | IAM token host and distributed limiter key; does not change the network address |
| `token_duration` | `15m` | Requested token lifetime, from `15s` through `15m` inclusive |
| `connection_rate_limit` | `10` | Local connections/sec, including distributed-mode outage fallback |
| `connection_burst_limit` | `100` | Local burst capacity, including outage fallback |
| `rate_limiter` | `local` | `local` or `dynamodb_token_bucket` |
| `rate_limiter_table` | None | Required DynamoDB table for token bucket mode |
| `token_bucket_rate` | `100` | Token bucket connections/sec |
| `token_bucket_capacity` | `1000` | Token bucket burst capacity |
| `token_bucket_max_wait` | `30s` | Maximum reservation wait |

Each setting comes from its attribute or its default. Region comes from `region` or the resolved endpoint's host name (`<cluster-id>.dsql.<region>.on.aws`); otherwise startup fails with instructions to set `region`. The endpoint comes from `cluster_endpoint` or the host in `connectAddr`. The AWS SDK credential chain supplies credentials, including profiles, environment credentials and IAM roles.

Integers and durations must parse and be positive, even when their mode is inactive. `token_duration` must also stay in its allowed range. An unknown mode or a DynamoDB mode without a table fails `CreateDB`, naming the setting and value. Valid settings unused by the chosen mode log a warning; local rate and burst are also used for distributed-mode fallback. A runtime failure to create the DynamoDB client still logs an error and falls back locally.

The connection limiter remains shared by all pools in a process: the first pool's settings win. A later pool requesting different effective limiter settings gets one warning naming the retained and requested settings. Use consistent limiter settings, regions and endpoints across pools that share a distributed limiter. Token cache entries remain distinct per endpoint, region, user and duration.

### TLS verification

The DSQL plugin defaults to `sslmode=verify-full`, verifying both the server certificate and the cluster hostname. The certificate is checked against the host in `connectAddr`, so use the cluster's endpoint host name there, not an IP address or another alias. This applies even when `tls.enabled` or `tls.enableHostVerification` is false or omitted. DSQL accepts only TLS. Each host needs a CA bundle trusting Amazon's certificate authorities; standard bundles and usual base images provide it. `tls.caFile` supplies a custom root bundle through `sslrootcert`; client certificate handling is unchanged.

An explicit `connectAttributes.sslmode` overrides this default. A value other than `verify-full` emits one warning because full certificate and hostname verification is not assured. Use this escape hatch only deliberately. The schema tool likewise defaults to `verify-full` and accepts `--sslmode` for an explicit override.

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

IAM settings are `connectAttributes.region`, `cluster_endpoint` (defaulting to the host in `connectAddr`) and `token_duration` (default 15 minutes). The region is inferred from a DSQL endpoint when no attribute is set; see [datastore settings](#datastore-settings). The cache derives expiry from the signed token, reserving 10% of its signed lifetime (at least five seconds) for opening a connection. Background checks refresh at 80% of the signed lifetime. A newly generated token inside the margin can be used while still unexpired, with a warning at most once a minute. Credentials refresh four to five minutes before expiry. Token duration does not determine established connection lifetime.

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

Token bucket mode requires a nonempty `rate_limiter_table`; a missing table fails startup.

The token bucket uses the local `ConnectionRateLimiter` for 30 seconds after DynamoDB API, throttling or network errors, then retries DynamoDB. The local controls are `connectAttributes.connection_rate_limit` (default 10/sec) and `connectAttributes.connection_burst_limit` (default 100). During an outage, every process paces itself locally, so the fleet may exceed DSQL's limits; DSQL rejects the excess with 53400 (not attempted). Lower the local burst if less excess is desired. Condition failures, the GCRA horizon timeout and cancellation do not cause fallback. Warnings are limited to once a minute; monitor `dsql_rate_limiter_fallbacks_total`. Startup fallback on client initialization failure is retained.

GCRA stores reservations in one `dsql_connect_gcra#<endpoint>` item with a string `pk` and TTL on `ttl_epoch`.

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

Hosts need only `UpdateItem`. The optional live test also needs `DeleteItem` to clean up its test item.

```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:UpdateItem"
  ],
  "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/temporal-dsql-rate-limiter"
}
```

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

Merge this datastore with the visibility configuration from section 2, and count the visibility pool in the connection budget too.

### Per-Service Pool Sizing

Each service pool opens its full `maxConns` allocation at startup and keeps it. A process can own several pools, so `maxConns × replicas` alone can undercount. Sum all pools across all processes, and reserve capacity for overlapping replicas during a deployment. Do not exceed the cluster connection limit (10,000 by default). Ephemeral pools add at most five connections each while startup work runs.

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
| Invalid setting or missing `rate_limiter_table` | Invalid datastore configuration | Correct the named attribute and restart |
| "failed to resolve AWS credentials" | IAM misconfiguration | Check role/credentials, verify `dsql:DbConnect` for a custom role or `dsql:DbConnectAdmin` for admin |
| "connection rate limit exceeded" | Too many new connections | Check distributed rate limiter, stagger restarts |
| `ResourceExhausted` / 53300 / 53400 | Connection not attempted or refused at admission | Inspect limiter and cluster connection budget |

### Serialization Conflicts

OCC conflicts are expected under concurrent load. Core transactions using `txExecute` retry SQLSTATE 40001 through the plugin policy.

- Monitor `dsql_tx_conflict_total` — a sustained spike may indicate hot-spot access patterns
- Monitor `dsql_tx_exhausted_total` — retries exhausted means the conflict rate exceeds retry budget

### No data migration

**There is no data migration** between DSQL and any other Temporal persistence store, in either direction. The schemas differ and no tooling transfers the data.

**Switching persistence stores abandons the data.** Pointing the persistence config at another, empty store starts Temporal on that empty store and leaves every workflow, history and task queue in DSQL behind.

Moving off DSQL means running a second Temporal cluster on the other store. Start new workflows there and let existing workflows finish on the DSQL cluster before retiring it. Other approaches, including Temporal multi-cluster replication, have not been tried with DSQL.
