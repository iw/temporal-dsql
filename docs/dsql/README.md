# Aurora DSQL Support for Temporal

Aurora DSQL is Amazon's serverless, PostgreSQL-compatible distributed SQL database. This plugin enables Temporal to use DSQL as its persistence (default) store. Visibility isn't supported on DSQL, so configure a separate visibility store.

> **Status: experimental; not recommended for production.** The critical and high findings from the October 2026 review are fixed. Remaining findings are tracked in the [open review issues](https://github.com/iw/temporal-dsql/issues?q=is%3Aissue+is%3Aopen+label%3Adsql-review-2026-10).

## Key Capabilities

- **IAM authentication** with automatic token refresh — no stored passwords
- **Optimistic concurrency control** — core retries whole transactions on SQLSTATE 40001 through the plugin retry policy
- **pgxpool connection pooling** for rate-limit-aware connection management
- **Distributed rate limiting** via DynamoDB (opt-in), with monotonic token bucket reservations; size every pool to fit the cluster's connection limit
- **Per-run buffered event IDs**, allocated inside the transaction that writes them

## Documentation

| Document | Description |
|----------|-------------|
| [Deployment Guide](deployment.md) | Deployment: schema setup, configuration, and operational guidance |
| [Connection Pool](connection-pool.md) | Pool sizing, lifetime, deployment checks |
| [Implementation Details](implementation.md) | Code structure, schema layout, transaction retries, shard ownership checks and batched deletion |
| [Metrics Reference](metrics.md) | Emitted metrics and alerting recommendations |
| [Upstream Changes](upstream-changes.md) | Changes outside fork-owned packages and their path upstream |

## Quick Reference

**Schema version:** 1.2

**How the plugin handles DSQL differences:**

| PostgreSQL Feature | Aurora DSQL Today | Plugin Today |
|-------------------|-------------------|--------------|
| `BIGSERIAL` | Sequences and identity columns supported since February 2026 | `buffered_events` IDs allocated per run |
| `CHECK` constraints | Supported | Omitted from the schema; validated in the application |
| Pessimistic locking | Not supported; conflicts surface at commit | Core `txExecute` retries whole transactions on SQLSTATE 40001 |
| `FOR UPDATE` on JOINs | Supported since August 2026 | Split into single-table queries |
| `FOR SHARE` | Not supported; `FOR KEY SHARE` supported since August 2026 | Shard read lock uses `FOR KEY SHARE` |

**DSQL limits to keep in mind:** 3,000 modified rows and 10 MiB per transaction, 1 MiB per column value, 60-minute connections, and 100 new connections per second per cluster. The plugin deletes in batches to stay within the row limit, and reports a statement over a limit as `TransactionSizeLimitError`, which history doesn't retry.

## Architecture

```
Temporal Services (Frontend, History, Matching, Worker)
                    │
                    ▼
         DSQL Persistence Layer
    ┌────────────┬────────────┬──────────────┐
    │ IAM Auth   │ pgxpool    │ OCC Retry    │
    │ + Token    │ + Limiter  │ + Backoff    │
    │   Refresh  │ + Metrics  │ + Metrics    │
    └────────────┴────────────┴──────────────┘
                    │
                    ▼
           Aurora DSQL Cluster
```
