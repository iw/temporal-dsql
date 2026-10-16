# temporal-dsql

Temporal server with an [Amazon Aurora DSQL](https://aws.amazon.com/rds/aurora/dsql/) persistence plugin.

This is a community distribution of [Temporal](https://github.com/temporalio/temporal). It isn't affiliated with or endorsed by Temporal Technologies, Inc. "Temporal" is a trademark of Temporal Technologies, Inc.

> [!WARNING]
> **Experimental: don't use this in production yet.** A review in October 2026 found correctness defects in shard ownership, transaction retries and namespace updates. They're tracked as [open issues](https://github.com/iw/temporal-dsql/issues?q=is%3Aissue+is%3Aopen+label%3Adsql-review-2026-10), and the [critical and high ones](https://github.com/iw/temporal-dsql/issues?q=is%3Aissue+is%3Aopen+label%3A%22severity%3A+critical%22%2C%22severity%3A+high%22) should be fixed first.

## What this adds to Temporal

- A `dsql` SQL persistence plugin for Temporal's default store, in [`common/persistence/sql/sqlplugin/dsql`](../common/persistence/sql/sqlplugin/dsql)
- A DSQL schema, in [`schema/dsql`](../schema/dsql), and a schema tool, `temporal-dsql-tool`
- IAM authentication with generated tokens, so no database password is stored
- A connection reservoir, and optional DynamoDB coordination, for DSQL's connection-rate limits

DSQL holds the persistence store only. Visibility needs a separate store, such as OpenSearch, Elasticsearch, PostgreSQL, MySQL or SQLite.

Everything outside the plugin, its schema and its tooling is upstream Temporal code.

## Compatibility

| | |
|---|---|
| Temporal base | Upstream `main` as of 2026-04-06, just before v1.31.0 |
| DSQL schema version | 1.1 |
| Go | 1.26 |

## Quick start

You need an Aurora DSQL cluster, AWS credentials that can call `dsql:DbConnectAdmin` on it, and Go 1.26.

1. Build the server and the schema tool:

   ```bash
   make temporal-server temporal-dsql-tool
   ```

2. Create the schema on a new cluster:

   ```bash
   export AWS_REGION=us-east-1
   ./temporal-dsql-tool --endpoint <cluster-id>.dsql.us-east-1.on.aws --region us-east-1 \
     setup-schema --schema-name dsql/temporal --version 1.1
   ```

   Start from a new cluster at version 1.1. Upgrading an existing v1.0 schema currently fails ([#28](https://github.com/iw/temporal-dsql/issues/28)).

3. Copy [`config/development-dsql.yaml`](../config/development-dsql.yaml) and replace its `persistence` block with this one:

   ```yaml
   persistence:
     defaultStore: dsql
     visibilityStore: visibility
     numHistoryShards: 4              # can't be changed after the cluster is created
     datastores:
       dsql:
         sql:
           pluginName: "dsql"
           databaseName: "postgres"   # a DSQL cluster has one database, named postgres
           connectAddr: "<cluster-id>.dsql.us-east-1.on.aws:5432"
           connectProtocol: "tcp"
           tls:
             enabled: true
           maxConns: 20
           maxIdleConns: 20
       visibility:                    # in memory, for trying it out only
         sql:
           pluginName: "sqlite"
           databaseName: "default"
           connectAddr: "localhost"
           connectProtocol: "tcp"
           connectAttributes:
             mode: "memory"
             cache: "private"
           maxConns: 1
           maxIdleConns: 1
   ```

   The plugin connects as the DSQL `admin` role with IAM tokens and ignores `user` and `password`. With `tls.enabled` alone the connection is encrypted but the server certificate isn't verified; `enableHostVerification: true` verifies it ([#39](https://github.com/iw/temporal-dsql/issues/39)). The plugin reads the region from `REGION` or `AWS_REGION`, and AWS credentials from the default credential chain.

4. Start the server:

   ```bash
   ./temporal-server --config-file <your-config>.yaml --allow-no-auth start
   ```

For production settings, see the [deployment guide](../docs/dsql/deployment.md).

## Documentation

- [DSQL documentation index](../docs/dsql/README.md)
- [Deployment guide](../docs/dsql/deployment.md)
- [Implementation details](../docs/dsql/implementation.md)
- [Connection reservoir design](../docs/dsql/reservoir-design.md)
- [Metrics reference](../docs/dsql/metrics.md)

## Releases

There are no releases yet. Build from source as shown above.

## Contributing

Open issues and pull requests in this repository, not in `temporalio/temporal`. `main` is protected, so every change goes through a pull request, including merges from upstream.

## Built with Claude Code

This project is developed with [Claude Code](https://claude.com/claude-code), [Anthropic](https://www.anthropic.com)'s agentic coding tool, and its contributions are a big part of how the project moves forward. Claude Code reviewed the DSQL plugin in October 2026, filed the findings as [issues](https://github.com/iw/temporal-dsql/issues?q=is%3Aissue+label%3Adsql-review-2026-10), and contributes changes through pull requests, including this README.

Work done with Claude Code is credited in the history. Its commits carry a `Co-Authored-By: Claude … <noreply@anthropic.com>` trailer, and its pull requests end with a "Generated with Claude Code" line. To learn more, see the [Claude Code documentation](https://code.claude.com/docs/en/overview).

## License

MIT, the same as upstream Temporal. See [LICENSE](../LICENSE).
