# DSQL connection pool

The persistence plugin uses pgx v5's `pgxpool` behind `database/sql` and sqlx. A single pool owns the physical connections, and a [gate](#the-gate) in front of it decides which caller gets the next one. A DSQL visibility datastore has a pool and gate of its own, built the same way.

## Size and startup

Service pools set both `MinConns` and `MaxConns` to `maxConns`, or 100 when unset. Where one config serves several services, `maxConnsByService` sets `maxConns` for each service it names, so that history's pools can be larger than worker's. pgxpool fills each pool in the background and maintains its size. Short-lived schema and startup pools use `PoolSizeHint: ephemeral`: at most five connections, with no minimum.

Startup pings once with a 30-second timeout. By default it doesn't wait for the rest of the pool. With `connectAttributes.pool_warmup_timeout` set, it then waits up to that long for a service pool to fill, so the service starts with its connections open, and logs a warning if the pool isn't full in time. pgxpool cancels the rest of its fill when a connection fails, and fills the pool at its next health check, a minute later. So a warm-up timeout over a minute, such as `90s`, lets a pool get over one failed connection.

Each service pool opens its full `maxConns` allocation even at low traffic. Count **every pool in every Temporal process**, including multiple stores in one process, across all replicas. Their sum must fit the cluster's connection limit (10,000 by default), with headroom for ephemeral pools, rolling deployments and administration. A tenfold increase in traffic queues at the same pool limit; it does not create an extra buffer of connections. A tenfold increase in replicas needs a new capacity budget.

`maxIdleConns` is ignored. The outer `sql.DB` has zero idle connections and unlimited open connections, so each statement, rows, transaction or `sql.Conn` takes a connection through the gate and gives it back when it's done. `MaxConnIdleTime` retains pgxpool's default (30 minutes). Idle eviction cannot reduce service pools below `MinConns == MaxConns`.

## The gate

The connector admits callers to the pool through a gate with one slot per connection. On its own, pgxpool serves callers first come, first served. On a pool too short for its load, history's reads and writes then interleave, every caller waits until its deadline, and no workflow finishes. The gate keeps the calls that can finish moving:

- **Writes first.** Transactions, and statements outside a transaction that change rows, wait in one queue, and reads in another. A slot that comes free goes to the first write, and only then to the first read. A call without a deadline waits with the writes, so it can't wait behind them without end.
- **Refused at once.** When every slot is taken, a caller with a deadline is refused at once if it can't be admitted in time to finish. The callers ahead of it, at the recent mean hold, and a reserve for its own hold must fit before its deadline. The reserve is 1.5 times the recent p90 hold of its kind, read or write, or of the other kind while its own has none.
- **Refused while waiting.** A waiting caller is refused once only its reserve is left before its deadline. A caller whose context ends while it waits gets the context's error, as it would from pgxpool.
- **Holds** run from admission until database/sql gives the connection back. The gate keeps the most recent 256 of each kind. A connect that fails gives its slot back without counting as a hold.

A refusal is a connection throttle. It returns `ResourceExhausted` with cause `SYSTEM_OVERLOADED`, the call never reaches DSQL, and the plugin reports a throttled [store fact](adaptive-store-facts.md#4-what-the-dsql-plugin-reports). Transactions and the plugin's workflow reads pass the refusal on as it is, and history backs off from it, where it resubmits `Unavailable` at once. Most other calls through the common store, history appends among them, report it as `Unavailable`. Callers that waited count as queued store facts.

While the pool has a free connection, the gate admits at once, so a pool sized for its load behaves as before. The gate doesn't hold back pgxpool's refills or health checks, and pgxpool still queues an admitted caller while it opens or replaces a connection.

The plugin's `GetWorkflowExecution` reads a workflow's mutable state on one connection, its nine queries sent as one pipelined batch, so a read takes one slot for about one round trip. See [implementation](implementation.md#workflow-reads).

## Lifetime

DSQL closes physical connections at 60 minutes. The plugin uses a 50-minute lifetime, up to four minutes of positive jitter, and a one-minute health-check interval. A shorter positive `maxConnLifetime` is honored; a longer value is capped at 50 minutes with one warning per plugin instance. IAM token lifetime is independent of established connection lifetime.

pgxpool checks expiry on release and during health checks. No `BeforeAcquire` or `AfterRelease` hooks are installed. The intended margin is about five minutes for a transaction. This is not a hard maximum checkout age: pgxpool's health checks refill the minimum before retiring idle connections, and can defer expiry during slow or failed refills. They also retire expired idle connections incrementally at the minimum size. Acquired connections remain in use until released. Monitor connection failures and keep transactions shorter than five minutes.

## Authentication and connection pacing

Every physical connection runs `BeforeConnect`, including startup fill and health-check replacement:

1. Wait for one of eight process-wide slots, respecting cancellation.
2. Wait once on the plugin's existing connection rate limiter.
3. Obtain an IAM token for `cfg.User` (or `admin` when empty), using the resolved `connectAttributes.token_duration`, and set `ConnConfig.Password`. Admin uses `DbConnectAdmin`; other roles use `DbConnect`. Configured passwords are ignored with one warning.

The slot is released when the hook returns, before the handshake. It bounds concurrent limiter calls, including DynamoDB requests, across all pools in the process. IAM signing uses the resolved `connectAttributes.cluster_endpoint` (defaulting to the host in `connectAddr`) and `region` (inferred from a DSQL host name unless explicit). The DynamoDB client uses the same region. TLS defaults to `verify-full`, regardless of TLS boolean settings. Hosts need a CA bundle trusting Amazon's certificate authorities; standard bundles and usual base images provide it. An explicit `connectAttributes.sslmode` is honored, with one warning unless it is `verify-full`.

Local rate limiting remains the default (`connectAttributes.connection_rate_limit`, default 10/sec; `connection_burst_limit`, default 100). Set `connectAttributes.rate_limiter` to `dynamodb_token_bucket`, with `rate_limiter_table`, to pace connections across processes. Token bucket mode uses GCRA reservations: 100/sec sustained, capacity 1000, and a maximum reservation wait of 30 seconds by default. Waiters sleep until their reserved time rather than polling DynamoDB. The shared theoretical arrival time only increases, so clock skew adds a bounded one-time allowance instead of a sustained excess rate.

The token bucket falls back to the process's local limiter for 30 seconds after a DynamoDB API, throttling or network error, then try DynamoDB again. Condition failures, reservation horizon errors and context cancellation do not trigger fallback. A warning is emitted at most once a minute, and `dsql_rate_limiter_fallbacks_total` counts calls routed to the local limiter. Startup still falls back locally if the DynamoDB client cannot be created. During a DynamoDB outage, each process paces itself at the local rate; DSQL rejects fleet-wide excess with SQLSTATE 53400, classified as not attempted. Operators who want less excess can lower the local burst.

The cache keeps one reusable token per endpoint, region, user and requested duration. Expiry comes from the signed `X-Amz-Date` plus `X-Amz-Expires`, including any shortening by credentials. Cache hits require 10% of the signed lifetime remaining, with a five-second minimum. Background checks refresh at 80% of the signed lifetime. Malformed or expired tokens are never served. A fresh token already inside the margin can still be used until its signed expiry, with a warning at most once a minute. Credential caching refreshes four to five minutes early using jitter.

Credential retrieval and signing happen outside the cache lock, with concurrent generation for the same key coalesced. Slow retrieval does not block valid cache hits. Failed refresh retains the cached token only while it is outside its safety margin; otherwise a new connection receives an error. Existing connections are unaffected because DSQL checks the token only at connection establishment.

Settings are resolved and validated once per datastore before opening its pool. Invalid integers, durations, modes and missing DynamoDB tables fail `CreateDB`; unused mode settings warn. The first pool establishes the process-wide limiter. Later pools with different effective limiter settings emit one warning and keep that limiter. Local pacing during an outage uses the first pool's `connection_rate_limit` and `connection_burst_limit`.

Plugin settings come only from datastore attributes or defaults. The resolver strips plugin attributes from a copy before constructing the DSN.

## Errors and ownership

The plugin never asks `DatabaseHandle` to rebuild a healthy pool after a connection error. pgxpool discards broken connections individually.

| Error | Temporal result |
|---|---|
| Gate refusal; limiter/semaphore failure; SQLSTATE 53400 or 53300 | `ResourceExhausted`, cause `SYSTEM_OVERLOADED`: connection not attempted, operation not committed |
| `driver.ErrBadConn`, EOF, unexpected EOF, reset/aborted/refused socket, `pgconn.ConnectError`, SQLSTATE class 08, IAM access denied | `Unavailable` with `database connection lost`, without rebuilding the pool |
| 40001, 0A000 | Preserved for transaction retry classification |
| 54000 | `TransactionSizeLimitError` |
| Other errors | Unchanged |

The connector wraps `stdlib.GetPoolConnector`, admits callers through the gate and implements `io.Closer`. Closing the plugin DB closes the handle and `sql.DB`, cancels background hook waits, joins the metrics collector, and closes the pool. Failed startup pings close the same resources before trying another default database name. pgxpool closes all physical connections and rejects subsequent acquires; its internal health-check goroutine and 500 ms health-check notifications may finish asynchronously after `Pool.Close()` returns. There is no plugin-owned refiller or driver registration to leak.

## Cancel requests

pgx sends a cancel request on a new connection whenever it cuts a statement off at its context's deadline, or closes a connection after an error. DSQL answers each one with `0A000 cancelling requests is not yet supported` and runs the statement to the end. So the plugin skips them: once a connection is established, its `DialFunc` refuses further dials, and `dsql_cancel_requests_skipped_total` counts each one refused. The caller still gets its context error at the deadline, and pgxpool replaces the closed connection through `BeforeConnect`, paced like any other.

## Deployment checks

Configure the database role and IAM mapping before starting the server. See
[least-privilege role setup](deployment.md#least-privilege-role). Each host needs a
trusted CA bundle, and `connectAddr` should name the cluster endpoint.

Budget every pool, including overlapping replicas during deployment. Set
`maxConnLifetime` to 50 minutes or less. Token bucket reservations use a
`dsql_connect_gcra#<endpoint>` item in the configured table with a string `pk` key
and TTL on `ttl_epoch`.

Watch `dsql_pool_open`, `dsql_pool_constructing`, the gate's waits and refusals,
the share of acquires that wait, canceled acquires and failed connections.
Without `pool_warmup_timeout`, a successful startup ping does not mean the
background fill is complete. Check `open - constructing` against the expected
size before starting more replicas. See [metrics](metrics.md).
