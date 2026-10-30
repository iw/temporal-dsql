# CI

temporal-dsql's CI is a [Dagger](https://dagger.io) module written in Rust with [tokeira/dagger-rust-sdk](https://github.com/tokeira/dagger-rust-sdk). [Buildkite](https://buildkite.com) runs it on [Namespace](https://namespace.so) agents, and the same functions run on a laptop. CI only starts the pinned engine and calls the module, so moving to another CI system would change only the trigger. GitHub Actions stays off, because upstream's workflows would misfire on a fork.

Pull request builds leave AWS out. The DSQL plugin's live tests need an AWS account, so they run on a developer's machine from `.dagger/live`, a Rust program that uses a scoped `temporal-dsql-ci` AWS profile.

The module and the live program run on the SDK's `v0.3.1` release and a stock Dagger `v1.0.0-beta.21` engine; see [The pinned SDK](#the-pinned-sdk).

## Layout

| Path | Contents |
|---|---|
| `dagger.toml` | Dagger workspace: installs the Rust SDK runtime at `v0.3.1` and registers the `ci` module |
| `dagger.lock` | Lock file that the engine records and reuses |
| `.dagger/modules/ci/` | The module, a Cargo project whose generated files are committed |
| `.dagger/live/` | The live tests: a Rust program that a developer runs, with its own `Cargo.lock` and toolchain pin |
| `.dagger/engine.sh` | Downloads and verifies the pinned CLI, starts the pinned engine, and prints the environment |
| `.buildkite/pipeline.yml` | Pipeline steps |

The module never sees Buildkite or Namespace. Its constructor takes the repository, which defaults to the workspace root without `.git`, `.dagger` and build output. The docs stay in, because a test checks the deployment guide's dynamic config. Pass `dagger call ci --source=<dir> …` to use another tree.

## Functions

| Call | What it does |
|---|---|
| `build [--platform]` | Compiles `temporal-server`, `temporal-dsql-tool` and `temporal-elasticsearch-tool`, and returns them as a directory |
| `lint [--base] [--patch]` | Runs golangci-lint v2.9.0 on the lines a change adds, and the errortype vet check, as `make lint-code` does |
| `test [--packages] [--run]` | Runs `go test -race -shuffle=on` on `common/adaptive/...`, `common/persistence`, `common/persistence/client/...`, `common/persistence/sql/...`, `common/persistence/visibility/...`, `common/signals/...`, `common/stats/...`, `schema/...`, `tools/dsql/...` and `tools/poolsim/...` |

`--platform` defaults to the engine's own platform. Go cross-compiles, so `build` can target any platform from any engine.

`lint` checks only the lines a change adds, as upstream's `--new-from-rev` does, since a full run reports thousands of existing issues. It works out the change from the repository's Git history:

- With `--base`, such as `--base main`, the change is everything since HEAD's merge base with that branch, which `lint` fetches from GitHub.
- Without it, the change is HEAD's own commit.
- In a checkout whose Git history the engine can't read, such as a Git worktree, pass the diff instead with `--patch`.

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
dagger call ci lint --base main
```

In a Git worktree, pass the diff instead: `git diff origin/main... > /tmp/lint.patch && dagger call ci lint --patch /tmp/lint.patch`.

If your global Git configuration rewrites `https://github.com/` to SSH, run `dagger` with `GIT_CONFIG_GLOBAL=/dev/null`: the engine fetches the SDK runtime over HTTPS and can't use your SSH keys. In a Git worktree the CLI warns that it can't open the repository. The warning is harmless.

### Changing the module

Edit `.dagger/modules/ci/src/lib.rs` or `Cargo.toml`, run `dagger generate`, and commit the regenerated files with the change. Until you do, loading the module fails with a diagnostic that names `dagger generate`. `Cargo.lock` is committed, because the engine builds the module with `--locked`.

## Live tests

`.dagger/live` runs the plugin's integration tests (`-tags integration`) against real AWS resources:

1. It creates an Aurora DSQL cluster and a DynamoDB table in the profile's region. Both are tagged `purpose=temporal-dsql-ci`, and the table is named `temporal-dsql-ci-<run>`.
2. In the Dagger engine, it sets up the persistence schema at version 1.3 and the visibility schema at version 1.0 with `temporal-dsql-tool setup-schema`, which waits for the index builds.
3. Also in the engine, it runs `go test -tags integration ./common/persistence/sql/sqlplugin/dsql/...` against them.
4. It deletes the cluster and the table, whether the tests passed or not, and also when you stop it with Ctrl-C.

Each run first deletes CI-tagged clusters and `temporal-dsql-ci-` tables that are more than three hours old, which runs that were killed outright left behind. A run takes about ten minutes, most of it waiting for the schemas' index builds. Run it before merging a change to the plugin, the schema or `temporal-dsql-tool`:

```bash
eval "$(.dagger/engine.sh)"
cd .dagger/live
AWS_PROFILE=temporal-dsql-ci dagger run cargo run --locked
```

Run it from `.dagger/live`, where `rust-toolchain.toml` pins Rust 1.99.1, which the SDK needs; rustup installs it on the first run. `dagger run` connects the program to the engine.

### Credentials

The program's own AWS calls use the local credential provider chain with the `temporal-dsql-ci` profile, so the profile's access key stays on your machine. The Go code in the engine needs credentials too, to sign DSQL tokens and DynamoDB requests. It gets only an hour-long session token from STS, and the cluster's endpoint, both as Dagger secrets, which Dagger shows as `***` in its output. The program prints the account and cluster IDs as `<account>` and `<cluster>`.

### The temporal-dsql-ci profile

The profile belongs to an IAM user, `temporal-dsql-ci`, that can do only what a run needs. In the account that runs the tests:

1. Create the user, and give it this inline policy. Replace `<account-id>`:

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

2. Create an access key for the user, and save it as the `temporal-dsql-ci` profile with region `eu-west-1`, for example with `aws configure --profile temporal-dsql-ci`.

The user can create clusters only with the CI tag, use and delete only clusters that carry it, and touch only tables named `temporal-dsql-ci-*`. Getting a session token from STS needs no permission. The account needs DSQL's service-linked role, which AWS creates with the account's first cluster.

## Buildkite on Namespace

Every build of a pull request, `main`, a release branch or a tag runs these steps, each on its own agent with its own engine:

| Step | Command | Commit status |
|---|---|---|
| build | `dagger call ci build` for linux/amd64 and linux/arm64 | `buildkite/build` |
| lint | `dagger call ci lint --base` with a pull request's base branch; other builds lint their own commit | `buildkite/lint` |
| test | `dagger call ci test` | `buildkite/test` |

None of the steps uses AWS, so the pipeline needs no AWS credentials. Run the live tests locally before merging a change they cover; see [Live tests](#live-tests).

Each step keeps its engine's state on a Namespace cache volume of its own, by setting `TEMPORAL_DSQL_ENGINE_STATE`, so the engine's build cache outlives the job. The cache saves time but isn't guaranteed: a job placed on another machine starts from that machine's older snapshot, or from an empty volume. Namespace saves a volume only when its job passes, and the step stops the engine before the job ends, so the saved state comes from a clean shutdown. A volume must stay larger than the engine's 30 GB cache cap.

### Setting it up

1. **Namespace agents.** Connect the Buildkite organization to Namespace, as for tokeira/dagger-rust-sdk's pipeline. The agents reach the host's Docker daemon, which starts the engine's privileged container. If the cluster's default queue isn't Namespace's, add `queue:` to the pipeline's `agents`.
2. **Buildkite GitHub app.** Install it on `iw/temporal-dsql`, so that pushes and pull requests trigger builds and the steps can post commit statuses. Turn off the pipeline's own GitHub status, since each step posts its own. Leave building pull requests from forks off, which is Buildkite's default.
3. **Pipeline.** Create `temporal-dsql` with repository `https://github.com/iw/temporal-dsql.git`, and this one configured step, which reads `.buildkite/pipeline.yml` from the commit being built:

   ```yaml
   steps:
     - label: ":pipeline: upload"
       command: buildkite-agent pipeline upload
       agents:
         nsc-shape: "2x4"
   ```

4. **Required checks.** Once the steps pass, require their statuses on `main`.

## The pinned SDK

`dagger.toml` installs the runtime from `github.com/tokeira/dagger-rust-sdk/runtime@v0.3.1`, and the module's `dagger-module.toml` names the same release. Installed from a release tag, the runtime generates modules against that release from crates.io, so the module's `Cargo.toml` depends on `tokeira-dagger-sdk = "=0.3.1"`, and so does `.dagger/live/Cargo.toml`. To move to another release:

1. Run `dagger module update rust-sdk --version <tag>`, which moves `dagger.toml`.
2. Set the same tag in `.dagger/modules/ci/dagger-module.toml`'s `[runtime] source`, by hand: `dagger module update` leaves it unchanged.
3. Set the `tokeira-dagger-sdk` version to match in the module's `Cargo.toml` and in `.dagger/live/Cargo.toml`, then run `cargo update -p tokeira-dagger-sdk` in each.
4. Run `dagger generate`, and commit the result.

## Changing the Dagger release

The SDK talks only to an engine of exactly its target release. When a new SDK release targets a new Dagger release, these move with it:

- `version`, the CLI checksums and the image digest in `.dagger/engine.sh`
- `engineVersion` in `.dagger/modules/ci/dagger-module.toml`

## Still to come

The release steps from the earlier pipeline, which built the server and tool images, pushed them to GHCR and packaged the release binaries, will be ported in a later change.
