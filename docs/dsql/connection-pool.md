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
3. Obtain an IAM token for `cfg.User` (or `admin` when empty), using `GetConfiguredTokenDuration()`, and set `ConnConfig.Password`. Admin uses `DbConnectAdmin`; other roles use `DbConnect`. Configured passwords are ignored with one warning.

The slot is released when the hook returns, before the handshake. It bounds concurrent limiter calls, including DynamoDB requests, across all pools in the process. The endpoint, `CLUSTER_ENDPOINT`, and `REGION`/`AWS_REGION` settings are unchanged. TLS defaults to `verify-full`, regardless of TLS boolean settings. Hosts need a CA bundle trusting Amazon's certificate authorities; standard bundles and usual base images provide it. An explicit `connectAttributes.sslmode` is honored, with one warning unless it is `verify-full`.

Local rate limiting remains the default (`DSQL_CONNECTION_RATE_LIMIT`, default 10/sec; `DSQL_CONNECTION_BURST_LIMIT`, default 100). With `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED`, the existing DynamoDB counter or token bucket paces connections across processes. Initialization and fallback behavior are unchanged. The token bucket's clock-skew and failure behavior remain tracked in [#40](https://github.com/iw/temporal-dsql/issues/40).

The cache keeps one reusable token per endpoint, region, user and requested duration. Expiry comes from the signed `X-Amz-Date` plus `X-Amz-Expires`, including any shortening by credentials. Cache hits require 10% of the signed lifetime remaining, with a five-second minimum. Background checks refresh at 80% of the signed lifetime. Malformed or expired tokens are never served. A fresh token already inside the margin can still be used until its signed expiry, with a warning at most once a minute. Credential caching refreshes four to five minutes early using jitter.

Credential retrieval and signing happen outside the cache lock, with concurrent generation for the same key coalesced. Slow retrieval does not block valid cache hits. Failed refresh retains an old token only while it is outside its safety margin; otherwise a new connection receives an error. Existing connections are unaffected because DSQL checks the token only at connection establishment.

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

The slot-block and distributed connection-lease implementations remain in the tree, unwired. `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true` refuses to connect at startup with an error naming [#33](https://github.com/iw/temporal-dsql/issues/33). A deployment relying on that global cap must resolve its capacity plan before upgrading.

## Rollout and rollback

**Role behavior changes:** a config whose `user` is not `admin` now connects as that role. The role must exist and be mapped to the host's IAM identity first, or the config must say `user: admin`. See [least-privilege role setup](deployment.md#least-privilege-role). Previously the plugin always connected as admin.

Ensure every host has a CA bundle trusting Amazon's certificate authorities for the new `verify-full` default, and that `connectAddr` names the cluster's endpoint host rather than an IP address or alias. Remove configured passwords and unnecessary `sslmode: require` overrides. Schema setup and upgrades continue to use `temporal-dsql-tool --user admin`.

Budget all pool sizes, including old and new replicas during rollout. Remove ignored variables, set `maxConnLifetime` to 50 minutes or less, and remove `maxIdleConns`. Keep rate-limiter configuration. Resolve any reliance on connection leasing before disabling it.

Roll one process at a time. Watch `dsql_pool_open`, `dsql_pool_constructing`, `dsql_pool_wait_total`, canceled acquires, and connection errors. A successful startup ping does not mean the background fill is complete. Check `open - constructing` against the expected size before advancing the rollout. See [metrics](metrics.md).

Rollback by deploying the previous image and restoring its previous connection settings. There is no schema change for this replacement.
