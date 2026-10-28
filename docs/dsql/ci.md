# CI

temporal-dsql's CI is a [Dagger](https://dagger.io) module written in Rust with [tokeira/dagger-rust-sdk](https://github.com/tokeira/dagger-rust-sdk). [Buildkite](https://buildkite.com) runs it on [Namespace](https://namespace.so) agents, and the same functions run on a laptop. CI only starts the pinned engine and calls the module, so moving to another CI system would change only the trigger. GitHub Actions stays off, because upstream's workflows would misfire on a fork.

The module runs on the SDK's `v0.3.1` release and a stock Dagger `v1.0.0-beta.21` engine; see [The pinned SDK](#the-pinned-sdk).

## Layout

| Path | Contents |
|---|---|
| `dagger.toml` | Dagger workspace: installs the Rust SDK runtime at `v0.3.1` and registers the `ci` module |
| `dagger.lock` | Lock file that the engine records and reuses |
| `.dagger/modules/ci/` | The module, a Cargo project whose generated files are committed |
| `.dagger/modules/ci/src/live.sh` | The script that `live` runs against AWS |
| `.dagger/engine.sh` | Downloads and verifies the pinned CLI, starts the pinned engine, and prints the environment |
| `.buildkite/pipeline.yml` | Pipeline steps |
| `.buildkite/lint.sh` | Limits lint to the lines a build changes |

The module never sees Buildkite or Namespace. Its constructor takes the repository, which defaults to the workspace root without `.git`, `.dagger` and build output. The docs stay in, because a test checks the deployment guide's dynamic config. Pass `dagger call ci --source=<dir> …` to use another tree.

## Functions

| Call | What it does |
|---|---|
| `build [--platform]` | Compiles `temporal-server`, `temporal-dsql-tool` and `temporal-elasticsearch-tool`, and returns them as a directory |
| `lint [--patch]` | Runs golangci-lint v2.9.0 and the errortype vet check, as `make lint-code` does |
| `test [--packages] [--run]` | Runs `go test -race -shuffle=on` on `common/persistence/sql/...`, `schema/...`, `tools/dsql/...` and `tools/poolsim/...` |
| `live [--region]` and AWS credentials | Runs the DSQL plugin's live tests on a throwaway cluster and DynamoDB table, then deletes both |

`--platform` defaults to the engine's own platform. Go cross-compiles, so `build` can target any platform from any engine.

Pass `lint` a patch. Upstream lints only new code (`--new-from-rev`), and a full run reports thousands of existing issues.

## Live tests

`live` runs the plugin's integration tests (`-tags integration`) against real AWS resources:

1. It creates an Aurora DSQL cluster and a DynamoDB table in `--region` (default `eu-west-1`). Both are tagged `purpose=temporal-dsql-ci`, and the table is named `temporal-dsql-ci-<run>`.
2. It sets up the schema with `temporal-dsql-tool setup-schema --version 1.2`, which waits for the index builds.
3. It runs `go test -tags integration ./common/persistence/sql/sqlplugin/dsql/...` with `DSQL_TEST_ENDPOINT` and `DSQL_TEST_DDB_TABLE` set.
4. It deletes the cluster and the table, whether the tests passed or not.

Each run first deletes CI-tagged clusters and `temporal-dsql-ci-` tables that are more than three hours old. Those are left by runs that were cancelled before they could clean up. The output shows the account and cluster IDs as `<account>` and `<cluster>`, since CI logs can be public. A run takes a few minutes, most of it waiting for the schema's index builds.

`live` takes credentials in one of two forms, all of them as Dagger secrets:

- `--role-arn` and `--web-identity-token`, as CI passes them. The run exchanges the token for the role's temporary credentials once, at the start.
- `--aws-access-key-id` and `--aws-secret-access-key`, with `--aws-session-token` for temporary credentials. This is the form for local runs.

## Local use

You need Docker Desktop or OrbStack. Start the engine and put the matching CLI on `PATH`:

```bash
eval "$(.dagger/engine.sh)"
```

The first run downloads the CLI into `~/.cache/temporal-dsql/dagger/`, checks it against its pinned SHA-256, and starts the `temporal-dsql-engine-v1.0.0-beta.21` container from the image pinned by digest. Later runs reuse the container. The engine keeps its state in the `temporal-dsql-engine-v1.0.0-beta.21-state` volume and collects its cache above 30 GB. `.dagger/engine.sh stop` stops the engine and keeps the volume. Stop it this way rather than with `docker rm -f`: an engine that is killed rather than stopped discards its whole cache when it next starts.

Then call the module:

```bash
dagger call ci build export --path dist/bin
dagger call ci test
dagger call ci test --packages ./common/persistence/sql/sqlplugin/dsql/... --run TestPool
git diff origin/main... > /tmp/lint.patch && dagger call ci lint --patch /tmp/lint.patch
```

For `live`, export credentials that can create and delete the resources, then pass them as secrets:

```bash
eval "$(aws configure export-credentials --format env)"
dagger call ci live --aws-access-key-id env://AWS_ACCESS_KEY_ID \
  --aws-secret-access-key env://AWS_SECRET_ACCESS_KEY --aws-session-token env://AWS_SESSION_TOKEN
```

Leave out `--aws-session-token` for a long-term access key.

If your global Git configuration rewrites `https://github.com/` to SSH, run `dagger` with `GIT_CONFIG_GLOBAL=/dev/null`: the engine fetches the SDK runtime over HTTPS and can't use your SSH keys. In a Git worktree the CLI warns that it can't open the repository. The warning is harmless.

### Changing the module

Edit `.dagger/modules/ci/src/lib.rs`, `live.sh` or `Cargo.toml`, run `dagger generate`, and commit the regenerated files with the change. Until you do, loading the module fails with a diagnostic that names `dagger generate`. `Cargo.lock` is committed, because the engine builds the module with `--locked`.

## Buildkite on Namespace

Every build of a pull request, `main`, a release branch or a tag runs these steps, each on its own agent with its own engine:

| Step | Command | Commit status |
|---|---|---|
| build | `dagger call ci build` for linux/amd64 and linux/arm64 | `buildkite/build` |
| lint | `.buildkite/lint.sh`: a pull request against its merge base, other builds against the parent commit | `buildkite/lint` |
| test | `dagger call ci test` | `buildkite/test` |
| live | `dagger call ci live` with the job's OIDC token; at most two at a time | `buildkite/live` |

Each step keeps its engine's state on a Namespace cache volume of its own, by setting `TEMPORAL_DSQL_ENGINE_STATE`, so the engine's build cache outlives the job. The cache saves time but isn't guaranteed: a job placed on another machine starts from that machine's older snapshot, or from an empty volume. Namespace saves a volume only when its job passes, and the step stops the engine before the job ends, so the saved state comes from a clean shutdown. A volume must stay larger than the engine's 30 GB cache cap.

### Setting it up

1. **Namespace agents.** Connect the Buildkite organization to Namespace, as for tokeira/dagger-rust-sdk's pipeline. The agents reach the host's Docker daemon, which starts the engine's privileged container. If the cluster's default queue isn't Namespace's, add `queue:` to the pipeline's `agents`.
2. **Buildkite GitHub app.** Install it on `iw/temporal-dsql`, so that pushes and pull requests trigger builds and the steps can post commit statuses. Turn off the pipeline's own GitHub status, since each step posts its own. Leave building pull requests from forks off, which is Buildkite's default: a fork's build could otherwise request the live step's AWS role.
3. **Pipeline.** Create `temporal-dsql` with repository `https://github.com/iw/temporal-dsql.git`, and this one configured step, which reads `.buildkite/pipeline.yml` from the commit being built:

   ```yaml
   steps:
     - label: ":pipeline: upload"
       command: buildkite-agent pipeline upload
       agents:
         nsc-shape: "2x4"
   ```

4. **AWS role.** Create the role described in the next section. The live step reads its ARN from `TEMPORAL_DSQL_CI_ROLE_ARN` in the job's environment, and stops early without it. Keep the ARN in Namespace Vault, attached to the Buildkite organization, or set it as a pipeline environment variable. It isn't a credential, but keeping it out of the repository keeps the account ID out of Git.
5. **Required checks.** Once the steps pass, require their statuses on `main`.

### AWS role for the live step

The live step uses Buildkite's OIDC tokens rather than stored keys. Namespace's own workload federation (`nsc aws assume-role`) would also work, but its trust covers every job in the Namespace workspace, while a Buildkite token can be limited to this pipeline's live step. In the account that runs the tests:

1. Register `https://agent.buildkite.com` as an IAM OIDC identity provider, with audience `sts.amazonaws.com`.
2. Create a role that trusts it only for this pipeline's `live` step. Replace `<account-id>` and `<org-slug>`:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": { "Federated": "arn:aws:iam::<account-id>:oidc-provider/agent.buildkite.com" },
         "Action": "sts:AssumeRoleWithWebIdentity",
         "Condition": {
           "StringEquals": { "agent.buildkite.com:aud": "sts.amazonaws.com" },
           "StringLike": {
             "agent.buildkite.com:sub": "organization:<org-slug>:pipeline:temporal-dsql:ref:*:commit:*:step:live"
           }
         }
       }
     ]
   }
   ```

3. Give the role this permissions policy. It can create clusters only with the CI tag, and use or delete only clusters that carry it, and only tables named `temporal-dsql-ci-*`:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "CreateCiClusters",
         "Effect": "Allow",
         "Action": ["dsql:CreateCluster", "dsql:TagResource"],
         "Resource": "arn:aws:dsql:eu-west-1:<account-id>:cluster/*",
         "Condition": { "StringEquals": { "aws:RequestTag/purpose": "temporal-dsql-ci" } }
       },
       {
         "Sid": "UseAndDeleteCiClusters",
         "Effect": "Allow",
         "Action": ["dsql:GetCluster", "dsql:DbConnectAdmin", "dsql:DeleteCluster"],
         "Resource": "arn:aws:dsql:eu-west-1:<account-id>:cluster/*",
         "Condition": { "StringEquals": { "aws:ResourceTag/purpose": "temporal-dsql-ci" } }
       },
       {
         "Sid": "FindLeftoverClusters",
         "Effect": "Allow",
         "Action": ["dsql:ListClusters", "dsql:ListTagsForResource"],
         "Resource": "*"
       },
       {
         "Sid": "CiTables",
         "Effect": "Allow",
         "Action": [
           "dynamodb:CreateTable", "dynamodb:DescribeTable", "dynamodb:UpdateTimeToLive",
           "dynamodb:DeleteTable", "dynamodb:TagResource",
           "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"
         ],
         "Resource": "arn:aws:dynamodb:eu-west-1:<account-id>:table/temporal-dsql-ci-*"
       },
       {
         "Sid": "FindLeftoverTables",
         "Effect": "Allow",
         "Action": "dynamodb:ListTables",
         "Resource": "*"
       }
     ]
   }
   ```

The account needs DSQL's service-linked role, which AWS creates with the account's first cluster. Keep the role's maximum session duration at an hour or more, longer than a run.

## The pinned SDK

`dagger.toml` installs the runtime from `github.com/tokeira/dagger-rust-sdk/runtime@v0.3.1`, and the module's `dagger-module.toml` names the same release. Installed from a release tag, the runtime generates modules against that release from crates.io, so the module's `Cargo.toml` depends on `tokeira-dagger-sdk = "=0.3.1"`. To move to another release:

1. Run `dagger module update rust-sdk --version <tag>`, which moves `dagger.toml`.
2. Set the same tag in `.dagger/modules/ci/dagger-module.toml`'s `[runtime] source`, by hand: `dagger module update` leaves it unchanged.
3. Set the module's `tokeira-dagger-sdk` version to match, then run `cargo update -p tokeira-dagger-sdk` in the module.
4. Run `dagger generate`, and commit the result.

## Changing the Dagger release

The SDK talks only to an engine of exactly its target release. When a new SDK release targets a new Dagger release, these move with it:

- `version`, the CLI checksums and the image digest in `.dagger/engine.sh`
- `engineVersion` in `.dagger/modules/ci/dagger-module.toml`

## Still to come

The release steps from the earlier pipeline, which built the server and tool images, pushed them to GHCR and packaged the release binaries, will be ported in a later change.
