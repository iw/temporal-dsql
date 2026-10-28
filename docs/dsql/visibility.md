# Visibility on Aurora DSQL

DSQL can hold Temporal's visibility store as well as its persistence store, so a deployment can run on DSQL alone. The store is upstream v1.32.1's SQL visibility store with a DSQL schema and query dialect. When the fork moves to a release that includes upstream's refactored visibility stores ([temporalio/temporal#12297](https://github.com/temporalio/temporal/pull/12297)), the dialect moves with it.

It suits namespaces whose retained executions number in the tens of thousands; see [Limits](#limits). OpenSearch or Elasticsearch remain the better choice for larger namespaces and for heavy use of Text and KeywordList search attributes, which DSQL can't index.

[Deployment](deployment.md#visibility-schema) covers setting it up.

## Where it lives

The visibility store has its own PostgreSQL schema, `temporal_visibility` by default, on the persistence cluster or on another one. DSQL has a single database per cluster, so a schema is how the two stores share one. The plugin sets `search_path` on the visibility pool's connections, so the visibility tables and their `schema_version` row stay apart from the persistence store's. A datastore that sets `search_path` in `connectAttributes` uses that schema instead.

## Table

`executions_visibility` keeps upstream's 21 columns, with the same names and types, and the primary key `(namespace_id, run_id)`.

Each search attribute has a stored generated column, as upstream: 16 predefined, 28 custom (Bool01–03, Datetime01–03, Double01–03, Int01–03, Keyword01–10, Text01–03, KeywordList01–03) and 15 CHASM. The types match upstream's, except:

- **Text** columns are `TEXT`, since DSQL has no `tsvector`. Each holds the value's words separated by single spaces, with a space at each end.
- **Datetime** columns use `convert_ts`, written as an `IMMUTABLE` SQL function, since DSQL has no PL/pgSQL.

That makes 80 columns of DSQL's 255. DSQL can't add a generated column to an existing table, so every column is created up front. A test in `tools/dsql` checks that the columns match upstream's PostgreSQL table, so a search attribute that a later release adds shows up when the fork moves to it; adding one needs a table rebuild.

## Indexes

DSQL allows 24 indexes per table, and the primary key counts, so 23 are left. PostgreSQL's schema has 73. All of these are B-tree and ascending: DSQL scans them backward for the newest-first order.

- **`default_idx`:** `(namespace_id, COALESCE(close_time, '9999-12-31 23:59:59'), start_time, run_id) WHERE TemporalNamespaceDivision IS NULL`. Every workflow query carries that predicate, so a partial index serves the default list order.
- **`(namespace_id, <column>, <the same sort columns>)`** for 18 more:
  - **System:** `workflow_id`, `workflow_type_name`, `status`, `task_queue`, `execution_time`, `parent_workflow_id`, `root_workflow_id`.
  - **Predefined:** `TemporalNamespaceDivision`, `TemporalScheduledById`, `BatcherUser`, `TemporalWorkerDeployment`, `TemporalWorkerDeploymentVersion`, `TemporalWorkflowVersioningBehavior`.
  - **Custom:** `Keyword01`, `Keyword02`, `Keyword03`, `Int01`, `Datetime01`. Namespaces get the first free column of each type, so these serve the first custom attributes of each type.

That leaves 4 spare.

**Not indexed:** the history and duration counters, `parent_run_id`, `root_run_id`, the schedule start time and pause flag, the external-payload counters, the other custom columns, and every JSONB and Text column. Filters on these read the rows the query's other filters select. CHASM queries always filter on `TemporalNamespaceDivision`, so its index narrows them to the archetype's rows.

## Queries

The DSQL dialect differs from PostgreSQL's in three places:

- **Order:** `COALESCE(close_time, …) DESC, start_time DESC, run_id DESC`. Upstream sorts `run_id` ascending, which a backward index scan can't provide.
- **Paging:** the next page starts after the token, written as `COALESCE(close_time, …) <= ? AND (COALESCE(close_time, …), start_time, run_id) < (?, ?, ?)`. DSQL applies the row comparison as a filter, so the plain bound on the close time is what lets an index scan start at the token.
- **Text:** each word of the value matches as a whole word, case-sensitive, `Text01 LIKE '% word %'`, with the words combined by OR. PostgreSQL's `tsvector` match behaves the same way: whitespace-separated words, exact case, any word. LIKE's wildcards in a value match themselves.

KeywordList uses `@>` containment, as upstream, without an index. Datetimes, counts and group-by work as upstream.

Only the unified query converter has a DSQL dialect, so the visibility store uses it for DSQL whatever `system.visibilityEnableUnifiedQueryConverter` says.

## Writes

Writes work as upstream does. An insert ignores a duplicate. An upsert applies only when its `_version` is newer, and leaves the key columns alone. Each write is a single statement. An optimistic-concurrency conflict comes back as an error, and the visibility task retries it.

## Limits

These come from `EXPLAIN ANALYZE` on a cluster holding about 22,000 executions in one namespace.

- **First pages read the namespace.** For an ordered page over most of a namespace, such as the default list, a first page, or a filter that matches most executions, DSQL's planner reads the namespace's rows and sorts them rather than scanning an index. It took about 100 ms at 22,000 executions, and the time and read DPUs grow with the namespace. Forced through `default_idx`, a page with about 16,000 older rows read one 1,000-row batch in about 30 ms, but for a large range the planner's estimates don't favour the index.
- **Selective filters use their index.** A filter on an indexed column that matches a small part of the namespace, such as a workflow ID, a workflow type or a keyword value, reads only its matches. So do later pages once the rows left before the token are few.
- **Counts read every matching row.** Unindexed filters read the rows the other filters select, so on a large namespace they're slow and cost DPUs.
- **Memory:** a query can use at most 128 MiB.
- **Value sizes:** the memo and the search attributes are each limited to 1 MiB, and an index key to 1 KiB, which a long multi-byte workflow ID or keyword value can reach.
- **Connections:** the visibility pool counts towards the cluster's connection limit, and shares the process-wide connection rate limiter with the persistence pools.

## Tests

- Unit tests for the dialect, the store's row handling and the `search_path` default, in the plugin package.
- Schema tests in `tools/dsql`: both setup paths for the visibility schema, its baseline check, and the column match with upstream.
- Live tests from `.dagger/live`, on a throwaway cluster with both schemas set up: upstream's `VisibilityPersistenceSuite`, and a test of Text and KeywordList matching.
