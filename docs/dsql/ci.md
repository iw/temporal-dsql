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
| `test [--packages] [--run]` | Runs `go test -race -shuffle=on` on `common/persistence/sql/...`, `common/persistence/visibility/...`, `schema/...`, `tools/dsql/...` and `tools/poolsim/...` |

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
3. Also in the engine, it runs `go test -tags integration ./common/persistence/sql/sqlplugin/dsql/...` against them, and then the check of the functional tests' schema reset, which clears `public`.
4. It deletes the cluster and the table, whether the tests passed or not, and also when you stop it with Ctrl-C.

Each run first deletes CI-tagged clusters and `temporal-dsql-ci-` tables that are more than twelve hours old, which runs that were killed outright left behind. A run takes about ten minutes, most of it waiting for the schemas' index builds. Run it before merging a change to the plugin, the schema or `temporal-dsql-tool`:

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

### Runner instances

Running the functional tests [on a Graviton instance](#on-a-graviton-instance) needs a bucket, a role for the instances, a network and a second policy for the user. Set them up once, in eu-west-1:

1. **Bucket.** Create `temporal-dsql-ci-<account-id>`, tagged `purpose=temporal-dsql-ci`. Keep S3's defaults for a new bucket, which block public access and turn off ACLs. Add a lifecycle rule that expires objects under `runs/` after a day and aborts incomplete multipart uploads after a day, and a bucket policy that denies requests without TLS (`aws:SecureTransport` false).
2. **Runner role.** Create the role `temporal-dsql-ci-runner`, which EC2 can assume, with this inline policy, and an instance profile of the same name that holds the role:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "ConnectToCiClusters",
         "Effect": "Allow",
         "Action": "dsql:DbConnectAdmin",
         "Resource": "arn:aws:dsql:eu-west-1:<account-id>:cluster/*",
         "Condition": { "StringEquals": { "aws:ResourceTag/purpose": "temporal-dsql-ci" } }
       },
       {
         "Sid": "RunFiles",
         "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:PutObject"],
         "Resource": "arn:aws:s3:::temporal-dsql-ci-<account-id>/runs/*"
       }
     ]
   }
   ```

3. **Network.** Create a VPC, such as `10.42.0.0/16`, with an internet gateway, a route table whose default route goes to the gateway, and a subnet in each zone that gives instances a public IPv4 address. In the VPC, create the security group `temporal-dsql-ci-runner`, with no inbound rules and outbound TCP to ports 443 (S3) and 5432 (DSQL) only. Tag all of these `purpose=temporal-dsql-ci`. Nothing uses the VPC's default security group, so remove its rules.
4. **User policy.** Create the managed policy `temporal-dsql-ci-runners`, and attach it to the user. It's a managed policy because an IAM user's inline policies share a limit of 2,048 characters:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "LaunchCiRunners",
         "Effect": "Allow",
         "Action": "ec2:RunInstances",
         "Resource": "arn:aws:ec2:eu-west-1:<account-id>:instance/*",
         "Condition": {
           "StringEquals": {
             "aws:RequestTag/purpose": "temporal-dsql-ci",
             "ec2:MetadataHttpTokens": "required"
           },
           "StringLike": { "ec2:InstanceType": ["c8g.*", "c7g.*"] }
         }
       },
       {
         "Sid": "LaunchIntoTheCiNetwork",
         "Effect": "Allow",
         "Action": "ec2:RunInstances",
         "Resource": [
           "arn:aws:ec2:eu-west-1:<account-id>:subnet/*",
           "arn:aws:ec2:eu-west-1:<account-id>:security-group/*"
         ],
         "Condition": { "StringEquals": { "aws:ResourceTag/purpose": "temporal-dsql-ci" } }
       },
       {
         "Sid": "LaunchWithNewInterfacesAndVolumes",
         "Effect": "Allow",
         "Action": "ec2:RunInstances",
         "Resource": [
           "arn:aws:ec2:eu-west-1:<account-id>:network-interface/*",
           "arn:aws:ec2:eu-west-1:<account-id>:volume/*"
         ]
       },
       {
         "Sid": "LaunchFromAmazonImages",
         "Effect": "Allow",
         "Action": "ec2:RunInstances",
         "Resource": "arn:aws:ec2:eu-west-1::image/*",
         "Condition": { "StringEquals": { "ec2:Owner": "amazon" } }
       },
       {
         "Sid": "TagCiRunnersAtLaunch",
         "Effect": "Allow",
         "Action": "ec2:CreateTags",
         "Resource": [
           "arn:aws:ec2:eu-west-1:<account-id>:instance/*",
           "arn:aws:ec2:eu-west-1:<account-id>:volume/*"
         ],
         "Condition": { "StringEquals": { "ec2:CreateAction": "RunInstances" } }
       },
       {
         "Sid": "TerminateCiRunners",
         "Effect": "Allow",
         "Action": "ec2:TerminateInstances",
         "Resource": "arn:aws:ec2:eu-west-1:<account-id>:instance/*",
         "Condition": { "StringEquals": { "aws:ResourceTag/purpose": "temporal-dsql-ci" } }
       },
       {
         "Sid": "FindTheCiNetworkAndRunners",
         "Effect": "Allow",
         "Action": ["ec2:DescribeInstances", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups"],
         "Resource": "*"
       },
       {
         "Sid": "PassTheRunnerRole",
         "Effect": "Allow",
         "Action": "iam:PassRole",
         "Resource": "arn:aws:iam::<account-id>:role/temporal-dsql-ci-runner",
         "Condition": { "StringEquals": { "iam:PassedToService": "ec2.amazonaws.com" } }
       },
       {
         "Sid": "ResolveTheRunnerImage",
         "Effect": "Allow",
         "Action": ["ssm:GetParameter", "ssm:GetParameters"],
         "Resource": "arn:aws:ssm:eu-west-1:*:parameter/aws/service/ami-amazon-linux-latest/*"
       },
       {
         "Sid": "ListRunFiles",
         "Effect": "Allow",
         "Action": "s3:ListBucket",
         "Resource": "arn:aws:s3:::temporal-dsql-ci-<account-id>"
       },
       {
         "Sid": "RunFiles",
         "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
         "Resource": "arn:aws:s3:::temporal-dsql-ci-<account-id>/runs/*"
       }
     ]
   }
   ```

With this policy, the user can start only c8g and c7g instances. Each must require IMDSv2, carry the CI tag and use the runner role and an image that Amazon owns. It must also go into the tagged subnets and security group. The user can end only CI-tagged instances, and can use the bucket only under `runs/`. The runner role can connect only to CI-tagged clusters, and can read and write only run files. The network costs nothing while no instance runs. A running instance's public IPv4 address is billed by the hour.

## Functional tests on DSQL

Temporal's functional tests in `tests/` start whole servers in the test process. With `-persistenceDriver=dsql` they run against an Aurora DSQL cluster, with the plugin as the persistence store and the DSQL visibility store:

```bash
go test -tags disable_grpc_modules,test_dep -timeout 8h ./tests \
  -persistenceType=sql -persistenceDriver=dsql
```

The other drivers give each test cluster a database of its own. DSQL has one database, which holds up to ten schemas, and a persistence schema takes over a minute to set up. So each test cluster leases one of the persistence schemas that `DSQL_TEST_SCHEMAS` names, set up beforehand, and deletes the rows its previous cluster left before it starts. Every test cluster shares one visibility schema, since visibility rows are keyed by namespace ID, which is new for every cluster. A lock file for each schema, in the system's temporary directory, keeps the test processes on one machine apart.

| Variable | Meaning |
|---|---|
| `DSQL_TEST_ENDPOINT` | The cluster's endpoint. Credentials come from the default AWS chain |
| `DSQL_TEST_SCHEMAS` | The persistence schemas, comma-separated, each set up with `dsql/temporal`. The tests delete their rows |
| `DSQL_TEST_VISIBILITY_SCHEMA` | The visibility schema, set up with `dsql/visibility`; `temporal_visibility` by default |
| `DSQL_TEST_USER` | The database role; `admin` by default |
| `DSQL_TEST_REGION` | The cluster's region, when the endpoint doesn't name it |
| `DSQL_TEST_POOL_METRICS_FILE` | A file to which the functional tests' clusters append their pools' metrics, one JSON object a line |

Test clusters in the shared and dedicated pools keep their schemas until the test process ends. A test that needs a fresh cluster leases another schema while it holds a dedicated slot, and so does each suite that shares one cluster between its tests. A test waits for a free schema as long as it takes, and logs a warning each minute. So have more schemas than `TEMPORAL_TEST_SHARED_CLUSTERS` plus twice `TEMPORAL_TEST_DEDICATED_CLUSTERS`, the most that the pooled and fresh clusters can hold. With nine schemas, 3 and 2 leave two for the suites. Run one test package at a time, as `make functional-test` does.

The test clusters' servers get the limits `config/dynamicconfig/development-dsql.yaml` sets for DSQL: a `system.transactionSizeLimit` of 1 MiB and a `limit.blobSize.error` of 1,000,000 bytes. Their services size their pools with `maxConnsByService`. For persistence, history has 40 connections, matching 30, frontend 20 and worker 10. For visibility, history and frontend have 10 each, and matching and worker 5. That makes 130 connections a cluster. Other pools, such as the persistence tests', hold ten. The tests are the only client of their DSQL cluster, so the test process opens connections as fast as DSQL allows: a burst of 1,000, then 100 a second. Each pool fills before its service starts, waiting up to 90 seconds, since pgxpool fills the rest of a pool at its next health check, a minute later, after a failed connection.

To set up the schemas by hand, set them up one at a time, since DSQL can fail a statement with OC001 while another session builds an index:

```bash
for schema in corpus_1 corpus_2 corpus_3 corpus_4 corpus_5 corpus_6 corpus_7 corpus_8; do
  go run ./cmd/tools/dsql --endpoint "$DSQL_TEST_ENDPOINT" --search-path "$schema" \
    setup-schema --schema-name dsql/temporal --version 1.3
done
go run ./cmd/tools/dsql --endpoint "$DSQL_TEST_ENDPOINT" \
  setup-schema --schema-name dsql/visibility --version 1.0
```

### With the live tests

`--corpus` makes a live run set up eight more persistence schemas, `corpus_1` to `corpus_8`, after the live tests. Then it runs `./tests` on this machine rather than in the engine, since the engine's environment doesn't suit in-process servers, with the profile's credentials from the default chain:

```bash
AWS_PROFILE=temporal-dsql-ci dagger run cargo run --locked -- --corpus
```

The test clusters lease `public` and the eight schemas, with three shared and two dedicated pooled clusters, and their servers log at warning level and above. The run prints each top-level test's result as it ends, and writes these files, masked, to `.dagger/live/target/corpus/<run>`:

| File | Contents |
|---|---|
| `results.jsonl` | The tests' events from `go tool test2json` |
| `clusters.jsonl` | A line for each test cluster created |
| `pools.jsonl` | The test clusters' pool metrics: each pool's gauges and counters every 15 seconds, and each connection's handshake time or failure. A line names the cluster's schema, the test that created the cluster, the service and `db_kind` |
| `stderr.log` | The test process's standard error |
| `summary.txt` | The top-level tallies, the tests that failed, and the tests without a result |

The tests run as many tests at once as the machine has CPUs. To compare runs on machines of different sizes, `--parallel <count>` sets that number instead, as `-test.parallel`.

A run from a developer's machine takes about two hours: 20 to 30 minutes to set up the schemas, then an hour and a half of tests. `TestVersioning3FunctionalSuite` shares one cluster between its tests, and runs alone for the last half hour. A test that waits on a channel without a deadline can hold the run until the 8-hour timeout. To end the run sooner, send SIGQUIT to the `functional.test` process. It exits with a goroutine dump in `stderr.log`, and the run still writes its summary and deletes the cluster. A test that's stopped this way has no result. Neither does a test whose result line ran into a server's log output, which test2json reads as output. The summary lists both kinds.

The run covers `./tests`. The clusters in `tests/xdc` replicate namespaces between them, so their visibility rows would meet in the one visibility schema, and `tests/ndc` hasn't been run on DSQL yet.

### On a Graviton instance

With `--instance` as well, the tests run on an EC2 instance in the cluster's region rather than on your machine. It needs the one-time setup in [Runner instances](#runner-instances):

```bash
AWS_PROFILE=temporal-dsql-ci dagger run cargo run --locked -- --corpus --instance c8g.4xlarge
```

The type must be a Graviton2, 3 or 4 type, and the user's policy allows the c8g and c7g families. Before the run creates anything, it builds `functional.test` and `test2json` for linux/arm64 with `CGO_ENABLED=0`, `-trimpath` and `-ldflags=-s -w`. It sets `GOARM64` to the processor's architecture: `v9.0` for Graviton4's 8g families, `v8.4` for Graviton3's 7g families and `v8.2` for Graviton2's 6g families. After it sets up the schemas, the run:

1. Uploads both binaries to `runs/<run>/bin/` in the CI bucket.
2. Starts the instance in the first CI subnet whose zone has capacity for the type. The instance runs the latest Amazon Linux 2023 image for arm64 with the `temporal-dsql-ci-runner` instance profile, requires IMDSv2, and is tagged `purpose=temporal-dsql-ci` and `run=<run>`.
3. Prints each top-level test's result as the instance reports it. The instance's boot script fetches the binaries, and runs the tests with the same flags and variables as a run on your machine, writing a CPU profile as well. Each minute, it uploads the top-level results so far.
4. Fetches the tests' output once the instance has uploaded it, with their exit code. It masks the output into `.dagger/live/target/corpus/<run>`, with `cpu.pprof` beside the files above. The instance shuts down after its upload, which terminates it.
5. Ends the instance, if it's still running, and deletes `runs/<run>/` in the bucket, however the run ends.

On a c8g.4xlarge, a run takes about 52 minutes: 20 to build the binaries, create the cluster and set up its schemas, and 31 on the instance.

The instance has no SSH key and no inbound rules, and gets its credentials from its role. It shuts down after nine hours, whatever happens, and each run with `--instance` ends CI instances that are more than twelve hours old.

To stop a test that hangs, create the run's `stop` file. Within a minute, the instance sends the tests SIGQUIT, and they exit with a goroutine dump in `stderr.log`:

```bash
AWS_PROFILE=temporal-dsql-ci aws s3api put-object --bucket temporal-dsql-ci-<account-id> --key runs/<run>/stop
```

The CPU profile covers the whole test process, with its servers, clients and tests. On a c8g.4xlarge, the process used about 0.4 of a core on average: the tests mostly wait on DSQL, so the build's flags, or a build with `-pgo=cpu.pprof`, make little difference to how long a run takes.

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
