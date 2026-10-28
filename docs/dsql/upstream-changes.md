# Changes to upstream code

The DSQL plugin, schemas, schema tool and documentation live in fork-owned
directories. A small set of changes remains in upstream packages to support
transaction retries, execution reads, pool sizing and bounded deletes. This
document records those changes and their path forward. Visibility continues to
use a separate, existing backend.

Upstream is unlikely to accept the DSQL plugin, so these changes stay in this
repository as a small patch set, rebased onto each upstream release tag. The
plugin stays in its own directories, which never conflict on a rebase, so a
separate plugin repository isn't needed while the plugin depends on the patch
set.

## Transaction retry hook

`common/persistence/sql/common.go` lets a database opt into
`sqlplugin.TxRetryPolicy`. It checks errors from beginning a transaction, running
its callback and committing before converting them to persistence errors. A
retry reruns the whole callback in a new transaction, with a context-aware delay
chosen by the plugin. When retries stop, the caller receives `ResourceExhausted`.
Typed transaction-size and resource-exhaustion errors are also preserved instead
of being wrapped as unavailable errors.

DSQL uses this to retry SQLSTATE `40001` optimistic concurrency conflicts with a
bounded backoff. Condition failures are not retryable. Databases without a retry
policy keep a single attempt; the reference-counted `DbConn` forwards the hook
only when its underlying database supports it. Keeping classification in the
plugin avoids teaching shared SQL code about DSQL error codes, while retrying the
whole transaction avoids replaying only part of a failed write.

Path forward: keep it, with its interface, forwarding methods and tests, in the
patch set.

## Plugin-provided execution store

`common/persistence/sql/factory.go` defines the optional `ExecutionStoreCreator`
interface. The factory creates the common execution store, then lets a supporting
connection wrap it. DSQL's wrapper reads the execution row, mutable-state maps,
buffered events, CHASM nodes and requested signal IDs in one transaction in
`GetWorkflowExecution`. Other methods delegate to the common store.

This reduces transaction boundaries for workflow-state reads on DSQL. Other
plugins retain the common store. Path forward: keep the factory hook in the
patch set; the DSQL wrapper stays in the plugin.

## Pool sizing for short-lived operations

`common/config/config.go` adds `SQL.PoolSizeHint`. The DSQL plugin uses
`"ephemeral"` for a pool with at most five connections and no minimum, instead of
keeping the full service pool open. Other plugins can ignore the hint.

`common/persistence/sql/version_checker.go` copies the SQL configuration and sets
the hint for version checks. `temporal/fx.go` copies the persistence configuration,
datastore map and SQL configurations for cluster-metadata initialization;
`temporal/server_impl.go` uses the same helper for system-namespace initialization.
The service configurations retain their original sizing.

DSQL needs this distinction because filling a full service pool for startup-only
work wastes connection capacity and connection-rate budget, especially during
rolling deployments. Path forward: keep it in the patch set, or replace it
with a plugin-side signal for short-lived pools.

## History branch deletes (#26)

`common/persistence/sql/history_store.go` deletes history-node ranges through the
database outside the encompassing transaction, then deletes the history-tree row.
DSQL can therefore commit bounded batches rather than put an entire branch into
one transaction. The tree row remains discoverable by retries and the scavenger
if deletion stops partway through. The file also preserves
`TransactionSizeLimitError` from `AppendHistoryNodes`, so oversized writes retain
their non-retryable classification.

Path forward: keep the delete ordering, the transaction boundary and the
typed-error preservation in the patch set, alongside the retry hook.
DSQL supplies the batching that keeps each delete within its transaction limits.

## Queue v2 range deletes (#69)

`common/persistence/sql/queue_v2.go` computes a fixed deletion range and deletes
messages through the database outside the metadata transaction. The range keeps
the last physical message so concurrent enqueues continue allocating increasing
IDs. After deletion, a short transaction rereads the metadata and advances the
minimum ID with `max`, so overlapping deletes cannot move it backwards.

DSQL can commit message deletions in bounded batches. If deletion or the metadata
update fails, readers skip missing rows and a retry can finish the operation.
`common/persistence/sql/sqlplugin/tests/queue_v2.go` moves range-delete fault
injection from the transaction wrapper to the database wrapper to match this
boundary.

Path forward: keep it, with its regression tests, in the patch set, like
the history deletes.

## Registration, dependencies and build support

| Files | Change and reason | Path forward |
| --- | --- | --- |
| `cmd/server/main.go` | Blank-imports the DSQL plugin so its `init` registers it with the SQL persistence factory. A server binary must import the plugin to select it by name. | Keep in the patch set. |
| `go.mod`, `go.sum` | Add direct AWS SDK v2 dependencies for configuration, DSQL IAM authentication and DynamoDB connection-rate limiting, with the associated dependency versions and checksums. | Keep while the plugin is part of this module. |
| `.dockerignore` | Allows the built `temporal-server` into the Docker build context for packaging the server with the plugin. | Keep. |
| `.gitignore` | Ignores the pool simulator binary, the local DSQL Dockerfile and Terraform state files, keeping generated artifacts and potentially sensitive state out of the repository. | Keep. |
| `AGENTS.md` | Records the fork's DSQL scope, development conventions and implementation status for coding agents. | Keep; the maintainer decides whether to restore upstream's instructions and move the fork's elsewhere. |
| `Makefile` | Includes the fork-owned `dsql.mk`, which builds `temporal-dsql-tool` and adds it to `bins` and `clean-bins` through prerequisites. | Keep the include as the only upstream Makefile edit; maintain DSQL recipes in `dsql.mk`. |

Token-cache counter definitions live with their only consumer in the plugin's
`token_cache.go`, using `metrics.NewCounterDef`. They record cache hits, misses, refreshes and failures. DSQL schema management uses `temporal-dsql-tool`, with IAM
authentication and TLS verification.

## Fork-added files inside upstream packages

These files extend existing packages without editing upstream files:

- `common/persistence/sql/sqlplugin/tx_retry_policy.go` defines the optional retry
  interface and the guarantee that retryable errors represent uncommitted
  transactions.
- `common/persistence/sql/tx_retry_policy.go` forwards the retry interface through
  `DbConn`, leaving databases without a policy non-retryable.
- `common/persistence/sql/tx_retry_test.go` checks typed errors under cancellation,
  cancellation during backoff and preservation of resource-exhaustion errors.
- `common/persistence/sql/queue_v2_test.go` checks that range deletion happens
  outside a transaction, recovery after a metadata-update failure and monotonic
  metadata under overlapping deletes.
- `common/persistence/metadata_manager_test.go` checks system-namespace creation,
  its serialized defaults, the already-existing case and propagation of store
  errors.

The retry and queue tests belong with their changes in the patch set.
The metadata-manager test guards system-namespace defaults and error handling.
Schema tests live in the fork-owned `tools/dsql` package. They verify schema-name
discovery and both baseline setup paths, with embedded and filesystem schemas,
without changing upstream's embedding code or tests.

## Listing the upstream-file diff

With an `upstream/main` ref available locally, this Bash command lists the fork's
committed changes since the common base, restricted to paths that exist in the
upstream tree:

```bash
base=$(git merge-base HEAD upstream/main)
paths=()
while IFS= read -r -d '' path; do
  if git cat-file -e "upstream/main:$path" 2>/dev/null; then
    paths+=("$path")
  fi
done < <(git diff --name-only -z "$base" HEAD)
if ((${#paths[@]})); then
  git diff --stat "$base" HEAD -- "${paths[@]}"
fi
```

Remove `--stat` for the patch. To include tracked, uncommitted work, omit `HEAD`
from both `git diff` commands. Review `git status --short` separately for new,
untracked files. To inventory fork-added files as well, run
`git diff --diff-filter=A --name-only "$base" HEAD` without the upstream-path
filter. None of these commands fetches or changes a ref.
