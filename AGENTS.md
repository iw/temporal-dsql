# AGENTS

## Mission
- Enable Temporal to run with Amazon Aurora DSQL as the **persistence** database while keeping the existing visibility store unchanged.
- Maintain Temporal feature parity, operational reliability, and performance envelopes established for the current persistence backends.
- Deliver incremental milestones that unblock experimentation without destabilizing the primary Temporal development branch.

## Scope & Non‑Goals
- **In scope:** schema compatibility, persistence layer wiring, config surfaces, tooling/tests for Aurora DSQL, migration/rollback paths, observability.
- **Not in scope:** changes to visibility DB, unrelated feature work, adopting new third‑party libraries unless usage already exists in `go.mod`.
- Avoid speculative refactors; tie every change to Aurora DSQL enablement or required hygiene uncovered along the way.

## Architecture Touchpoints
1. **`/common/persistence`**: storage interfaces, factory wiring, schema management utilities.
2. **`/service/*`**: ensure persistence clients are injectable/configurable per service without affecting visibility paths.
3. **`/schema`**: new/updated DSQL schema artifacts, migrations, and validation tooling.
4. **Config**: extend dynamic/static config knobs (under `/config` and `/common/dynamicconfig`) so operators can opt into DSQL.
5. **Tooling**: `make` targets, proto/codegen hooks, and deployment templates that reference persistence backends.

## Working Agreements
- Mirror existing code style, naming, and error handling patterns. Inspect neighboring files before editing.
- Verify any dependency is already used in the repo before importing it.
- Prefer incremental PRs grouped by subsystem (e.g., schema prep, persistence adapter, service wiring, tests).
- Keep comments minimal and focused on *why* design trade‑offs were made.
- Do not revert or overwrite local developer changes unless explicitly asked.
- Attribute agent-assisted work. Commits made with an AI coding agent end with a `Co-Authored-By:` trailer naming the agent; Claude Code adds `Co-Authored-By: <model> <noreply@anthropic.com>` by default. Pull request descriptions end with the agent's attribution line, such as `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. Keep these lines when squashing, rebasing or rewording commits. The shared `.claude/settings.json` sets Claude Code's `attribution.pr`, which also stops the deprecated `includeCoAuthoredBy: false` from turning attribution off.

## Development Flow
1. **Understand**: read surrounding code/tests/config before coding; capture findings in issue notes or design docs.
2. **Plan**: break tasks into verifiable steps with explicit test strategies; call out performance, scalability, complexity, and security trade‑offs and failure modes (crash recovery, 10× load, etc.).
3. **Implement**: follow repo conventions, ensure error paths are handled/logged appropriately (`logger.Fatal` vs `logger.DPanic`).
4. **Regenerate**: rerun codegen whenever touching `.proto`, `//go:generate`, or schema assets.
5. **Verify**: run `make lint-code` plus targeted unit tests (`make unit-test -tags test_dep` as needed). Document skipped tests with rationale.

## Testing Expectations
- Cover success and failure cases for persistence adapters, config guards, and migrations.
- Include regression tests for mixed persistence/visibility backends.
- Leverage deterministic fixtures; avoid flaky integration tests unless tagged with `integration`.
- Prefer `require` over `assert`, avoid testify suites in unit tests, use `require.Eventually` instead of `time.Sleep`.

## Operational Considerations
- Document rollout/rollback instructions for operators (config flags, schema migrations, fallback options).
- Ensure metrics/alerts exist for Aurora DSQL health and latency; extend `/common/metrics` definitions when necessary.
- Capture open questions (consistency model, transaction semantics, failure domains) in tracking issues or this document.

## Testing:
- Write tests for new functionality
- Run tests after altering code or tests
- Start with unit tests for fastest feedback
- Prefer `require` over `assert`, avoid testify suites in unit tests (functional tests require suites for test cluster setup), use `require.Eventually` instead of `time.Sleep` (forbidden by linter)
- For float comparisons in tests, use `InDelta` or `InEpsilon` instead of `Equal` (enforced by `testifylint`)
- For error assertions in testify suites, use `s.Require().NoError(err)` instead of `s.NoError(err)` (enforced by `testifylint`)

---

## Implementation Status

> **Known issues (review of 2026-10-16):** The critical and high findings are fixed. Remaining findings are tracked in the [open review issues](https://github.com/iw/temporal-dsql/issues?q=is%3Aissue+is%3Aopen+label%3Adsql-review-2026-10). The plugin remains experimental and is not recommended for production.

### ✅ Implemented

#### Schema Compatibility
- DSQL-compatible schema (`schema/dsql/temporal/schema.sql`)
- BYTEA → UUID conversion for composite primary keys
- BIGSERIAL → BIGINT, with `buffered_events` IDs allocated per run inside the transaction
- CHECK constraints omitted (DSQL now supports them)
- INDEX ASYNC for non-blocking index creation

#### Persistence Layer
- First-class DSQL plugin (`common/persistence/sql/sqlplugin/dsql/`)
- Optimistic concurrency control; transactions that fail with SQLSTATE 40001 are retried through the opt-in `sqlplugin.TxRetryPolicy` hook
- Shard updates are fenced by the range check in `lockShard`, as upstream
- FOR UPDATE on single tables only (DSQL lifted this limit in August 2026)
- Shard read lock uses FOR KEY SHARE, so a range renewal fails the commit of an in-flight write that read the old range

#### IAM Authentication
- pgxpool BeforeConnect hook obtains IAM tokens from the existing cache
- Token cache reads each token's expiry from the signed token and refreshes it before it expires
- Connections use the configured `user` role (default `admin`); passwords are ignored and TLS defaults to `verify-full`

#### Connection Management
- **pgxpool** behind `database/sql` — service pools keep `MinConns == MaxConns` (configured `maxConns`, default 100); ephemeral pools use max 5/min 0. A closing connector owns pool and metrics shutdown. Connection failures do not rebuild pools.
- Lifetime is capped at 50 minutes, with 4 minutes of jitter and 1-minute health checks. `maxIdleConns` is ignored. There are no acquire/release hooks. See the expiry and shutdown limitations in `docs/dsql/connection-pool.md`.
- A process-wide semaphore of 8 bounds limiter/token calls before each handshake. Background fill uses the same rate limiter as on-demand connections.
- **Token Bucket Rate Limiter** (opt-in) — DynamoDB-backed GCRA reservations, with burst capacity 1000 and 100/sec sustained. Reservations only move forward under clock skew. Both distributed modes fall back to local pacing for 30 seconds on DynamoDB errors; local and distributed counter modes remain available.

#### Observability
- DSQL-specific transaction conflict, retry, backoff and exhaustion metrics
- pgxpool gauges and counters (constructing, waits, new connections, lifetime destroys, canceled acquires)
- Connection pool metrics (open, idle, in-use)

#### Documentation
- `docs/dsql/README.md` - Documentation index
- `docs/dsql/deployment.md` - Production deployment and configuration
- `docs/dsql/implementation.md` - Technical implementation details
- `docs/dsql/connection-pool.md` - Pool architecture, sizing, rollout and rollback
- `docs/dsql/metrics.md` - Metrics reference

#### Tooling
- `tools/poolsim/` - Discrete event simulator retaining the historical reservoir model
- `tools/dsql/` - DSQL schema management tool

### Known Constraints

1. **Sequences and Identity Columns**: Supported by DSQL since February 2026; the plugin doesn't use them, because `buffered_events` IDs only need to be unique within a run
2. **CHECK Constraints**: Supported by DSQL; the schema omits them and the application validates instead
3. **FOR SHARE Not Supported**: The shard read lock uses FOR KEY SHARE instead, which DSQL has supported since August 2026
4. **FOR UPDATE on JOINs**: Supported by DSQL since August 2026; the plugin still splits these into single-table queries
5. **Connection Rate Limit**: 100 connections/second cluster-wide
6. **Connection Limit**: 10,000 connections per cluster (default, can be raised)
7. **Transaction Limits**: 3,000 modified rows and 10 MiB per transaction; range deletes run in batches that halve on SQLSTATE 54000
8. **Column Size**: 1 MiB per non-indexed column value; the plugin reports SQLSTATE 54000 as `TransactionSizeLimitError`, and the DSQL dynamic config caps history batches and payloads below 1 MiB
9. **Connection Duration**: 60 minutes maximum

### Recommended Configuration

**Service pool sizing:** set `maxConns` per pool and `maxConnLifetime: 50m` (or shorter). Every service pool opens its full size in the background and keeps it. The sum across all pools in all Temporal processes must fit the cluster limit (10,000 by default), allowing for rolling deployments and ephemeral pools. Remove `maxIdleConns`; it is ignored. Removed environment settings and rollout/rollback by previous image (no schema change) are documented in `docs/dsql/connection-pool.md`.

**Distributed Rate Limiting with Token Bucket** (opt-in; see `docs/dsql/connection-pool.md` for rollout and outage behavior):
```yaml
# Under persistence.datastores.<store>.sql:
connectAttributes:
  # region: "us-east-1" # Inferred from the DSQL endpoint when omitted
  rate_limiter: "dynamodb_token_bucket"
  rate_limiter_table: "temporal-dsql-rate-limiter"
  token_bucket_rate: "100"
  token_bucket_capacity: "1000"
```

Legacy environment settings still work but are deprecated and will be removed in a future release; see the [mapping and precedence rules](docs/dsql/deployment.md#datastore-settings-and-legacy-fallbacks). `AWS_REGION` remains a standard fallback without a warning. Invalid settings or a DynamoDB mode without a table fail startup. The process-wide limiter uses the first pool's settings and warns once if a later pool requests different effective settings.

### Validation Results

These come from benchmark runs against a live DSQL cluster, before the 2026-10-16 review. The reservoir measured in those runs has since been replaced by pgxpool.

- **400 WPS benchmark**: Validated with 100% workflow completion
- **Checkout latency**: Sub-millisecond (p99 < 1ms)
- **Historical zero empty events**: the former reservoir had connections available throughout the benchmark
- **Rate limit compliance**: Never exceeds 100 connections/second

---

## Documentation

| Document | Description |
|----------|-------------|
| `docs/dsql/README.md` | Documentation index |
| `docs/dsql/deployment.md` | Production deployment and configuration |
| `docs/dsql/implementation.md` | Technical implementation details |
| `docs/dsql/connection-pool.md` | Pool architecture, sizing, rollout and rollback |
| `docs/dsql/metrics.md` | Metrics reference |
| `tools/poolsim/README.md` | Pool simulator documentation |
