# DSQL connection pool

The persistence plugin uses pgx v5.7.2's `pgxpool` behind `database/sql` and sqlx. A single pool owns the physical connections and the acquisition queue. Visibility continues to use its separately configured store.

## Size and startup

Service pools set both `MinConns` and `MaxConns` to `maxConns`, or 100 when unset. pgxpool fills them in the background and maintains that size. Startup pings once with a 30-second timeout; it does not wait for the entire pool to fill or add a startup delay. Short-lived schema and startup pools use `PoolSizeHint: ephemeral`: at most five connections, with no minimum.

Each service pool opens its full `maxConns` allocation even at low traffic. Count **every pool in every Temporal process**, including multiple stores in one process, across all replicas. Their sum must fit the cluster's connection limit (10,000 by default), with headroom for ephemeral pools, rolling deployments and administration. A tenfold increase in traffic queues at the same pool limit; it does not create an extra buffer of connections. A tenfold increase in replicas needs a new capacity budget.

`maxIdleConns` is ignored. The outer `sql.DB` has zero idle connections and unlimited open connections; pgxpool is the only queue. `MaxConnIdleTime` retains pgxpool's default (30 minutes). Idle eviction cannot reduce service pools below `MinConns == MaxConns`.

## Lifetime

DSQL closes physical connections at 60 minutes. The plugin uses a 50-minute lifetime, up to four minutes of positive jitter, and a one-minute health-check interval. A shorter positive `maxConnLifetime` is honored; a longer value is capped at 50 minutes with one warning per plugin instance. IAM token lifetime is independent of established connection lifetime.

pgxpool checks expiry on release and during health checks. No `BeforeAcquire` or `AfterRelease` hooks are installed. The intended margin is about five minutes for a transaction. This is not a hard maximum checkout age: in v5.7.2 health checks refill the minimum before retiring idle connections, and can defer expiry during slow or failed refills. They also retire expired idle connections incrementally at the minimum size. Acquired connections remain in use until released. Monitor connection failures and keep transactions shorter than five minutes.

## Authentication and connection pacing

Every physical connection runs `BeforeConnect`, including startup fill and health-check replacement:

1. Wait for one of eight process-wide slots, respecting cancellation.
2. Wait once on the plugin's existing connection rate limiter.
3. Obtain an IAM token for `cfg.User` (or `admin` when empty), using the resolved `connectAttributes.token_duration`, and set `ConnConfig.Password`. Admin uses `DbConnectAdmin`; other roles use `DbConnect`. Configured passwords are ignored with one warning.

The slot is released when the hook returns, before the handshake. It bounds concurrent limiter calls, including DynamoDB requests, across all pools in the process. IAM signing uses the resolved `connectAttributes.cluster_endpoint` (defaulting to the host in `connectAddr`) and `region` (inferred from a DSQL host name unless explicit). The DynamoDB client uses the same region. TLS defaults to `verify-full`, regardless of TLS boolean settings. Hosts need a CA bundle trusting Amazon's certificate authorities; standard bundles and usual base images provide it. An explicit `connectAttributes.sslmode` is honored, with one warning unless it is `verify-full`.

Local rate limiting remains the default (`connectAttributes.connection_rate_limit`, default 10/sec; `connection_burst_limit`, default 100). Set `connectAttributes.rate_limiter` to `dynamodb_counter` or `dynamodb_token_bucket`, with `rate_limiter_table`, to pace connections across processes. Token bucket mode uses GCRA reservations: 100/sec sustained, capacity 1000, and a maximum reservation wait of 30 seconds by default. Waiters sleep until their reserved time rather than polling DynamoDB. The shared theoretical arrival time only increases, so clock skew adds a bounded one-time allowance instead of a sustained excess rate.

Both distributed modes fall back to the process's local limiter for 30 seconds after a DynamoDB API, throttling or network error, then try DynamoDB again. Condition failures, reservation horizon errors and context cancellation do not trigger fallback. A warning is emitted at most once a minute, and `dsql_rate_limiter_fallbacks_total` counts calls routed to the local limiter. Startup still falls back locally if the DynamoDB client cannot be created. During a DynamoDB outage, each process paces itself at the local rate; DSQL rejects fleet-wide excess with SQLSTATE 53400, classified as not attempted. Operators who want less excess can lower the local burst.

The cache keeps one reusable token per endpoint, region, user and requested duration. Expiry comes from the signed `X-Amz-Date` plus `X-Amz-Expires`, including any shortening by credentials. Cache hits require 10% of the signed lifetime remaining, with a five-second minimum. Background checks refresh at 80% of the signed lifetime. Malformed or expired tokens are never served. A fresh token already inside the margin can still be used until its signed expiry, with a warning at most once a minute. Credential caching refreshes four to five minutes early using jitter.

Credential retrieval and signing happen outside the cache lock, with concurrent generation for the same key coalesced. Slow retrieval does not block valid cache hits. Failed refresh retains an old token only while it is outside its safety margin; otherwise a new connection receives an error. Existing connections are unaffected because DSQL checks the token only at connection establishment.

Settings are resolved and validated once per datastore before opening its pool. Invalid integers, durations, modes and missing DynamoDB tables fail `CreateDB`; unused mode settings warn. The first pool establishes the process-wide limiter. Later pools with different effective limiter settings emit one warning and keep that limiter. Local pacing during an outage uses the first pool's `connection_rate_limit` and `connection_burst_limit`.

Legacy environment variables still work but are deprecated and will be removed in a future release. See the [complete mapping and precedence rules](deployment.md#datastore-settings-and-legacy-fallbacks); `AWS_REGION` remains supported without a warning. The resolver strips plugin attributes from a copy before constructing the DSN.

## Errors and ownership

The plugin never asks `DatabaseHandle` to rebuild a healthy pool after a connection error. pgxpool discards broken connections individually.

| Error | Temporal result |
|---|---|
| Limiter/semaphore failure; SQLSTATE 53400 or 53300 | `ResourceExhausted`, cause `SYSTEM_OVERLOADED`: connection not attempted, operation not committed |
| `driver.ErrBadConn`, EOF, unexpected EOF, reset/aborted/refused socket, `pgconn.ConnectError`, SQLSTATE class 08, IAM access denied | `Unavailable` with `database connection lost`, without rebuilding the pool |
| 40001, 0A000 | Preserved for transaction retry classification |
| 54000 | `TransactionSizeLimitError` |
| Other errors | Unchanged |

The connector wraps `stdlib.GetPoolConnector` and implements `io.Closer`. Closing the plugin DB closes the handle and `sql.DB`, cancels background hook waits, joins the metrics collector, and closes the pool. Failed startup pings close the same resources before trying another default database name. pgxpool closes all physical connections and rejects subsequent acquires; its internal health-check goroutine and 500 ms health-check notifications may finish asynchronously after `Pool.Close()` returns. There is no plugin-owned refiller or driver registration to leak.

## Migration from the old layer

The reservoir and the default pool warmup / pool keeper have been removed. The simulator in `tools/poolsim` deliberately retains the historical reservoir model. A warm pgxpool completed the same simulated work within its configured size; the old reservoir needed up to twice that allocation. An acquire-time guard window converted expiry into request waits, so this design omits it.

These variables are ignored. If any are set, startup emits one warning listing their names (including variables set to an empty value):

- `DSQL_RESERVOIR_ENABLED`
- `DSQL_RESERVOIR_TARGET_READY`
- `DSQL_RESERVOIR_LOW_WATERMARK`
- `DSQL_RESERVOIR_BASE_LIFETIME`
- `DSQL_RESERVOIR_LIFETIME_JITTER`
- `DSQL_RESERVOIR_GUARD_WINDOW`
- `DSQL_RESERVOIR_INITIAL_FILL_TIMEOUT`
- `DSQL_RESERVOIR_INFLIGHT_LIMIT`
- `DSQL_POOL_WARMUP_STAGGER_DURATION`
- `DSQL_POOL_KEEPER_TICK_INTERVAL`
- `DSQL_POOL_KEEPER_MAX_CONNS_PER_TICK`
- `DSQL_STAGGERED_STARTUP`
- `DSQL_STAGGERED_STARTUP_MAX_DELAY`
- `DSQL_DISTRIBUTED_CONN_LEASE_TABLE`
- `DSQL_DISTRIBUTED_CONN_LIMIT`
- `DSQL_SLOT_BLOCK_SIZE`
- `DSQL_SLOT_BLOCK_COUNT`
- `DSQL_SLOT_BLOCK_TTL`
- `DSQL_SLOT_BLOCK_RENEW_INTERVAL`

The slot-block connection lease was removed ([#33](https://github.com/iw/temporal-dsql/issues/33)). `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true` still refuses startup: size the pools so the sum of `maxConns` across every Temporal process, including old and new replicas overlapping during deploys, fits the cluster's connection limit, then unset the variable. Allow headroom for ephemeral pools and administration. The lease's DynamoDB table can be deleted once no old deployment uses it; keep the separate rate-limiter table.

## Rollout and rollback

**Role behavior changes:** a config whose `user` is not `admin` now connects as that role. The role must exist and be mapped to the host's IAM identity first, or the config must say `user: admin`. See [least-privilege role setup](deployment.md#least-privilege-role). Previously the plugin always connected as admin.

Ensure every host has a CA bundle trusting Amazon's certificate authorities for the new `verify-full` default, and that `connectAddr` names the cluster's endpoint host rather than an IP address or alias. Remove configured passwords and unnecessary `sslmode: require` overrides. Schema setup and upgrades continue to use `temporal-dsql-tool --user admin`.

Budget all pool sizes, including old and new replicas during rollout. Remove ignored variables, set `maxConnLifetime` to 50 minutes or less, and remove `maxIdleConns`. Move rate-limiter configuration into each datastore's `connectAttributes`, using the [migration table](deployment.md#datastore-settings-and-legacy-fallbacks). Apply the fleet sizing rule above before unsetting the removed lease flag.

Token bucket reservations use the new `dsql_connect_gcra#<endpoint>` item in the existing table (`pk` string key, TTL on `ttl_epoch`). The plugin never reads the old `dsql_connect_bucket#<endpoint>` item; its TTL removes it. During a mixed-version rollout or rollback, old and new hosts use separate items and the fleet can admit up to twice the configured rate and burst. DSQL rejects excess connections with 53400, which the plugin treats as not attempted. Keep mixed-version overlap short and monitor connection throttling.

Roll one process at a time. Watch `dsql_pool_open`, `dsql_pool_constructing`, `dsql_pool_wait_total`, canceled acquires, and connection errors. A successful startup ping does not mean the background fill is complete. Check `open - constructing` against the expected size before advancing the rollout. See [metrics](metrics.md).

Rollback by deploying the previous image, removing the new plugin attributes from `connectAttributes`, and restoring its previous environment settings. Older images send the new keys to PostgreSQL as unknown parameters. There is no schema change for this replacement.
