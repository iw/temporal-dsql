# temporal-dsql

Temporal server with an [Amazon Aurora DSQL](https://aws.amazon.com/rds/aurora/dsql/) persistence plugin.

This is a community distribution of [Temporal](https://github.com/temporalio/temporal). It isn't affiliated with or endorsed by Temporal Technologies, Inc. "Temporal" is a trademark of Temporal Technologies, Inc.

> [!WARNING]
> **Experimental: don't use this in production yet.** Known issues are tracked as [GitHub issues](https://github.com/iw/temporal-dsql/issues).

## What this adds to Temporal

- A `dsql` SQL persistence plugin for Temporal's default store, in [`common/persistence/sql/sqlplugin/dsql`](../common/persistence/sql/sqlplugin/dsql)
- A DSQL schema, in [`schema/dsql`](../schema/dsql), and a schema tool, `temporal-dsql-tool`
- IAM authentication with generated tokens, so no database password is stored
- pgxpool connection management and optional DynamoDB token bucket rate limiting; pools must be sized to fit the cluster's connection limit
- A DSQL visibility store, in its own schema on the same cluster or another, with a query dialect for DSQL

DSQL visibility suits namespaces whose retained executions number in the tens of thousands. For larger namespaces, or heavy use of Text and KeywordList search attributes, use OpenSearch or Elasticsearch; see [visibility](../docs/dsql/visibility.md).

The fork also changes some upstream code to work within DSQL's limits, such as transaction retries and the history and queue range deletes. See [upstream changes](../docs/dsql/upstream-changes.md) for what remains and its path forward.

## Compatibility

| | |
|---|---|
| Temporal base | Upstream release `v1.32.1` |
| DSQL schema version | 1.3, and 1.0 for visibility |
| Go | 1.26 |

## Quick start

You need an Aurora DSQL cluster, AWS credentials that can call `dsql:DbConnectAdmin` on it, and Go 1.26.

1. Build the server and the schema tool:

   ```bash
   make temporal-server temporal-dsql-tool
   ```

2. Create the persistence and visibility schemas on a new cluster:

   ```bash
   ./temporal-dsql-tool --endpoint <cluster-id>.dsql.us-east-1.on.aws --region us-east-1 \
     setup-schema --schema-name dsql/temporal --version 1.3
   ./temporal-dsql-tool --endpoint <cluster-id>.dsql.us-east-1.on.aws --region us-east-1 \
     setup-schema --schema-name dsql/visibility --version 1.0
   ```

   The first creates a fresh persistence schema at version 1.3. The second creates the visibility store in its own PostgreSQL schema, `temporal_visibility`, at version 1.0. The tool waits for each index build, so these take a few minutes. The deployment guide covers both [schemas](../docs/dsql/deployment.md#1-schema-setup).

3. Copy [`config/development-dsql.yaml`](../config/development-dsql.yaml) and replace the `connectAddr` placeholder in both of its datastores with your cluster endpoint. Both stores use the DSQL database `postgres`: persistence in its default schema, and visibility in `temporal_visibility`. Keep [`config/dynamicconfig/development-dsql.yaml`](../config/dynamicconfig/development-dsql.yaml) at the configured path, or adjust `dynamicConfigClient.filepath`.

   The plugin connects as `user`, which defaults to `admin`, using IAM tokens. It ignores `password` and defaults to `sslmode=verify-full` for server certificate and hostname verification. It takes the region from the cluster's host name, and credentials from the default AWS credential chain. Other plugin settings, such as rate limiting, go in the datastore's `connectAttributes`. The [deployment guide](../docs/dsql/deployment.md#datastore-settings) lists the attributes and defaults. See [least-privilege role setup](../docs/dsql/deployment.md#least-privilege-role) before choosing another role.

4. Start the server:

   ```bash
   ./temporal-server --config-file <your-config>.yaml --allow-no-auth start
   ```

The [deployment guide](../docs/dsql/deployment.md) covers the rest of the configuration: dynamic config, connection pool sizing, rate limiting, monitoring and troubleshooting.

## Documentation

- [DSQL documentation index](../docs/dsql/README.md)
- [Deployment guide](../docs/dsql/deployment.md)
- [Implementation details](../docs/dsql/implementation.md)
- [Connection pool design and sizing](../docs/dsql/connection-pool.md)
- [Metrics reference](../docs/dsql/metrics.md)
- [CI](../docs/dsql/ci.md)

## Releases

There are no releases yet. Build from source as shown above.

## Contributing

Open issues and pull requests in this repository, not in `temporalio/temporal`. `main` is protected, so every change goes through a pull request, including moves to a new upstream release; see [upgrading to a new release tag](../docs/dsql/upstream-changes.md#upgrading-to-a-new-release-tag).

## Built with Claude Code

This project is developed with [Claude Code](https://claude.com/claude-code), [Anthropic](https://www.anthropic.com)'s agentic coding tool, and its contributions are a big part of how the project moves forward.

Work done with Claude Code is credited in the history. Its commits carry a `Co-Authored-By: Claude … <noreply@anthropic.com>` trailer, and its pull requests end with a "Generated with Claude Code" line. To learn more, see the [Claude Code documentation](https://code.claude.com/docs/en/overview).

## License

MIT, the same as upstream Temporal. See [LICENSE](../LICENSE).
