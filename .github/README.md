# temporal-dsql

Temporal server with an [Amazon Aurora DSQL](https://aws.amazon.com/rds/aurora/dsql/) persistence plugin.

This is a community distribution of [Temporal](https://github.com/temporalio/temporal). It isn't affiliated with or endorsed by Temporal Technologies, Inc. "Temporal" is a trademark of Temporal Technologies, Inc.

> [!WARNING]
> **Experimental: don't use this in production yet.** The critical and high findings from the October 2026 review are fixed. See the [remaining open issues](https://github.com/iw/temporal-dsql/issues?q=is%3Aissue+is%3Aopen+label%3Adsql-review-2026-10).

## What this adds to Temporal

- A `dsql` SQL persistence plugin for Temporal's default store, in [`common/persistence/sql/sqlplugin/dsql`](../common/persistence/sql/sqlplugin/dsql)
- A DSQL schema, in [`schema/dsql`](../schema/dsql), and a schema tool, `temporal-dsql-tool`
- IAM authentication with generated tokens, so no database password is stored
- pgxpool connection management and optional DynamoDB rate limiting; pools must be sized to fit the cluster's connection limit

DSQL holds the persistence store only. Visibility needs a separate store, such as OpenSearch, Elasticsearch, PostgreSQL, MySQL or SQLite.

The fork also changes some upstream code to work within DSQL's limits, such as transaction retries and the history and queue range deletes.

## Compatibility

| | |
|---|---|
| Temporal base | Upstream `main` as of 2026-04-06, just before v1.31.0 |
| DSQL schema version | 1.2 |
| Go | 1.26 |

## Quick start

You need an Aurora DSQL cluster, AWS credentials that can call `dsql:DbConnectAdmin` on it, and Go 1.26.

1. Build the server and the schema tool:

   ```bash
   make temporal-server temporal-dsql-tool
   ```

2. Create the schema on a new cluster:

   ```bash
   ./temporal-dsql-tool --endpoint <cluster-id>.dsql.us-east-1.on.aws --region us-east-1 \
     setup-schema --schema-name dsql/temporal --version 1.2
   ```

   Start from a new cluster at version 1.2. To upgrade a cluster at 1.0 or 1.1, see the [deployment guide](../docs/dsql/deployment.md#upgrading-from-v11).

3. Copy [`config/development-dsql.yaml`](../config/development-dsql.yaml) and replace its `connectAddr` placeholder with your cluster endpoint. It uses the DSQL database `postgres` and a separate in-memory SQLite visibility store for trying things out only. Keep [`config/dynamicconfig/development-dsql.yaml`](../config/dynamicconfig/development-dsql.yaml) at the configured path, or adjust `dynamicConfigClient.filepath`.

   The plugin connects as `user`, which defaults to `admin`, using IAM tokens. It ignores `password` and defaults to `sslmode=verify-full` for server certificate and hostname verification. It takes the region from the cluster's host name, and credentials from the default AWS credential chain. Other plugin settings, such as rate limiting, go in the datastore's `connectAttributes`. The [deployment guide](../docs/dsql/deployment.md#datastore-settings-and-legacy-fallbacks) lists them, with the environment variables they replace. See [least-privilege role setup](../docs/dsql/deployment.md#least-privilege-role) before choosing another role.

4. Start the server:

   ```bash
   ./temporal-server --config-file <your-config>.yaml --allow-no-auth start
   ```

For production settings, see the [deployment guide](../docs/dsql/deployment.md), which also covers rollback and why there's no data migration.

## Documentation

- [DSQL documentation index](../docs/dsql/README.md)
- [Deployment guide](../docs/dsql/deployment.md)
- [Implementation details](../docs/dsql/implementation.md)
- [Connection pool design, rollout and rollback](../docs/dsql/connection-pool.md)
- [Metrics reference](../docs/dsql/metrics.md)

## Releases

There are no releases yet. Build from source as shown above.

## Contributing

Open issues and pull requests in this repository, not in `temporalio/temporal`. `main` is protected, so every change goes through a pull request, including merges from upstream.

## Built with Claude Code

This project is developed with [Claude Code](https://claude.com/claude-code), [Anthropic](https://www.anthropic.com)'s agentic coding tool, and its contributions are a big part of how the project moves forward.

Work done with Claude Code is credited in the history. Its commits carry a `Co-Authored-By: Claude … <noreply@anthropic.com>` trailer, and its pull requests end with a "Generated with Claude Code" line. To learn more, see the [Claude Code documentation](https://code.claude.com/docs/en/overview).

## License

MIT, the same as upstream Temporal. See [LICENSE](../LICENSE).
