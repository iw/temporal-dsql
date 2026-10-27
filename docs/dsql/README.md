# Aurora DSQL Support for Temporal

Aurora DSQL is Amazon's serverless, PostgreSQL-compatible distributed SQL database. This plugin enables Temporal to use DSQL as its persistence (default) store. Visibility isn't supported on DSQL, so configure a separate visibility store.

> **Status: experimental.** Open defects from the October 2026 review are tracked as [issues labelled `dsql-review-2026-10`](https://github.com/iw/temporal-dsql/issues?q=is%3Aissue+is%3Aopen+label%3Adsql-review-2026-10). Fix the critical and high ones before any production use.

## Key Capabilities

- **IAM authentication** with automatic token refresh — no stored passwords
- **Optimistic concurrency control** — DSQL reports conflicts at commit; the transaction-level retry doesn't fire yet ([#24](https://github.com/iw/temporal-dsql/issues/24))
- **Connection Reservoir** for rate-limit-aware connection management
- **Distributed coordination** via DynamoDB for multi-service deployments (opt-in; see [#33](https://github.com/iw/temporal-dsql/issues/33) and [#40](https://github.com/iw/temporal-dsql/issues/40))
- **Snowflake ID generation** for `buffered_events` IDs, which can collide across transactions ([#34](https://github.com/iw/temporal-dsql/issues/34))

## Documentation

| Document | Description |
|----------|-------------|
| [Deployment Guide](deployment.md) | Production deployment: schema setup, configuration, and operational guidance |
| [Reservoir Design](reservoir-design.md) | Connection reservoir architecture, internals, and troubleshooting |
| [Implementation Details](implementation.md) | Code structure, schema changes, OCC handling, CAS updates |
| [Metrics Reference](metrics.md) | All emitted metrics with alerting recommendations |

## Quick Reference

**Schema version:** 1.1

**How the plugin handles DSQL differences:**

| PostgreSQL Feature | Aurora DSQL Today | Plugin Today |
|-------------------|-------------------|--------------|
| `BIGSERIAL` | Sequences and identity columns supported since February 2026 | Snowflake IDs ([#34](https://github.com/iw/temporal-dsql/issues/34)) |
| `CHECK` constraints | Supported | Omitted from the schema; validated in the application |
| Pessimistic locking | Not supported; conflicts surface at commit | Transactions that fail with SQLSTATE 40001 are retried |
| `FOR UPDATE` on JOINs | Supported since August 2026 | Split into single-table queries |
| `FOR SHARE` | Not supported; `FOR KEY SHARE` supported since August 2026 | Shard read lock uses `FOR KEY SHARE` |

**DSQL limits to keep in mind:** 3,000 modified rows and 10 MiB per transaction, 1 MiB per column value, 60-minute connections, and 100 new connections per second per cluster. Range deletes ([#26](https://github.com/iw/temporal-dsql/issues/26)) and large payloads ([#27](https://github.com/iw/temporal-dsql/issues/27)) can hit these limits today.

## Architecture

```
Temporal Services (Frontend, History, Matching, Worker)
                    │
                    ▼
         DSQL Persistence Layer
    ┌────────────┬────────────┬──────────────┐
    │ IAM Auth   │ Reservoir  │ OCC Retry    │
    │ + Token    │ + Refiller │ + Backoff    │
    │   Refresh  │ + Leasing  │ + Metrics    │
    └────────────┴────────────┴──────────────┘
                    │
                    ▼
           Aurora DSQL Cluster
```
