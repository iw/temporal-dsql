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

> **Known issues (review of 2026-10-16):** 32 open defects are tracked as GitHub issues #22–#53 ([label `dsql-review-2026-10`](https://github.com/iw/temporal-dsql/issues?q=is%3Aissue+is%3Aopen+label%3Adsql-review-2026-10)). Affected items below link to their issue; check it before relying on the item.

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
- Shard updates are fenced by the range check in `lockShard`, as upstream; the CAS helpers in `cas_updates.go` are unused ([#49](https://github.com/iw/temporal-dsql/issues/49))
- FOR UPDATE on single tables only (DSQL lifted this limit in August 2026)
- Shard read lock uses FOR KEY SHARE, so a range renewal fails the commit of an in-flight write that read the old range

#### IAM Authentication
- Token-refreshing driver with automatic credential refresh
- Token cache with expiry management (can serve expired tokens around credential rotation: [#32](https://github.com/iw/temporal-dsql/issues/32))

#### Connection Management
- **Connection Reservoir** (recommended) — Channel-based buffer with continuous refiller, proactive expiry scanner, guard window, in-flight semaphore (lifetime and leak issues: [#29](https://github.com/iw/temporal-dsql/issues/29), [#30](https://github.com/iw/temporal-dsql/issues/30))
- **Token Bucket Rate Limiter** (opt-in) — DynamoDB-backed, uses DSQL's burst capacity (1000) with 100/sec sustained (overshoots under clock skew: [#40](https://github.com/iw/temporal-dsql/issues/40))
- **Slot Block Manager** (opt-in) — Block-based distributed connection leasing to avoid DynamoDB hot partitions (100 blocks × 100 slots = 10k) (leaks blocks: [#33](https://github.com/iw/temporal-dsql/issues/33))
- Non-reservoir fallback (pool warmup + pool keeper), the default when `DSQL_RESERVOIR_ENABLED` is unset (hides pgx fast paths: [#38](https://github.com/iw/temporal-dsql/issues/38))

#### Observability
- DSQL-specific metrics (latency, and transaction conflicts, retries and exhaustion)
- Reservoir metrics (checkouts, empty events, discards, refills)
- Connection pool metrics (open, idle, in-use)

#### Documentation
- `docs/dsql/README.md` - Documentation index
- `docs/dsql/deployment.md` - Production deployment and configuration
- `docs/dsql/implementation.md` - Technical implementation details
- `docs/dsql/reservoir-design.md` - Reservoir architecture and configuration
- `docs/dsql/metrics.md` - Metrics reference

#### Tooling
- `tools/poolsim/` - Discrete event simulator for reservoir behavior validation
- `tools/dsql/` - DSQL schema management tool

### Known Constraints

1. **Sequences and Identity Columns**: Supported by DSQL since February 2026; the plugin doesn't use them, because `buffered_events` IDs only need to be unique within a run
2. **CHECK Constraints**: Supported by DSQL; the schema omits them and the application validates instead
3. **FOR SHARE Not Supported**: The shard read lock uses FOR KEY SHARE instead, which DSQL has supported since August 2026
4. **FOR UPDATE on JOINs**: Supported by DSQL since August 2026; the plugin still splits these into single-table queries
5. **Connection Rate Limit**: 100 connections/second cluster-wide
6. **Connection Limit**: 10,000 connections per cluster (default, can be raised)
7. **Transaction Limits**: 3,000 modified rows and 10 MiB per transaction; range deletes can exceed this ([#26](https://github.com/iw/temporal-dsql/issues/26))
8. **Column Size**: 1 MiB per non-indexed column value, below Temporal's 2 MiB blob limit ([#27](https://github.com/iw/temporal-dsql/issues/27))
9. **Connection Duration**: 60 minutes maximum

### Recommended Configuration

**Reservoir Mode** (recommended for production):
```bash
export DSQL_RESERVOIR_ENABLED=true
export DSQL_RESERVOIR_TARGET_READY=50
export DSQL_RESERVOIR_BASE_LIFETIME=11m
export DSQL_RESERVOIR_LIFETIME_JITTER=2m
export DSQL_RESERVOIR_GUARD_WINDOW=45s
export DSQL_RESERVOIR_INFLIGHT_LIMIT=8  # Max concurrent Open() calls
```

**Distributed Rate Limiting with Token Bucket** (opt-in; see [#40](https://github.com/iw/temporal-dsql/issues/40) before enabling):
```bash
export DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=true
export DSQL_DISTRIBUTED_RATE_LIMITER_TABLE=temporal-dsql-rate-limiter
export DSQL_TOKEN_BUCKET_ENABLED=true
export DSQL_TOKEN_BUCKET_RATE=100       # Tokens per second (DSQL sustained rate)
export DSQL_TOKEN_BUCKET_CAPACITY=1000  # Bucket capacity (DSQL burst capacity)
```

**Distributed Connection Limiting with Slot Blocks** (opt-in, for multi-service deployments; see [#33](https://github.com/iw/temporal-dsql/issues/33) before enabling):
```bash
export DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true
export DSQL_DISTRIBUTED_CONN_LEASE_TABLE=temporal-dsql-conn-lease
export DSQL_SLOT_BLOCK_SIZE=100   # Slots per block
export DSQL_SLOT_BLOCK_COUNT=100  # Total blocks (100 × 100 = 10k slots)
export DSQL_SLOT_BLOCK_TTL=3m     # TTL for crash recovery
export DSQL_SLOT_BLOCK_RENEW_INTERVAL=1m
```

### Validation Results

These come from benchmark runs against a live DSQL cluster, before the 2026-10-16 review.

- **400 WPS benchmark**: Validated with 100% workflow completion
- **Checkout latency**: Sub-millisecond (p99 < 1ms)
- **Zero empty events**: Reservoir always has connections available
- **Rate limit compliance**: Never exceeds 100 connections/second

---

## Documentation

| Document | Description |
|----------|-------------|
| `docs/dsql/README.md` | Documentation index |
| `docs/dsql/deployment.md` | Production deployment and configuration |
| `docs/dsql/implementation.md` | Technical implementation details |
| `docs/dsql/reservoir-design.md` | Reservoir architecture and configuration |
| `docs/dsql/metrics.md` | Metrics reference |
| `tools/poolsim/README.md` | Pool simulator documentation |
