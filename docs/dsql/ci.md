# CI and release pipeline

temporal-dsql's CI is a [Dagger](https://dagger.io) module written with the Rust SDK from the [iw/dagger](https://github.com/iw/dagger) fork. [Buildkite](https://buildkite.com) runs it on [Namespace](https://namespace.so) agents, and the same functions run on a laptop. GitHub Actions stays off, because upstream's workflows would misfire on a fork.

> **Status: the module doesn't load yet.** It's pinned to the fork's `sdk/rust/v1.0.0-beta.11.rust.4` release. That release's Rust module runtime builds the module into a cache volume and then reads the binary back out of it, which Dagger doesn't allow. Every `dagger call ci …` therefore fails with `Rust runtime finalization failed`, and so does every pipeline step. A fixed fork release unblocks it. The engine operations behind each function have been run on the rust.4 engine through the Dagger API; see [Caching](#caching) for the timings.

## Layout

| Path | Contents |
|---|---|
| `dagger.toml` | Dagger workspace: installs the Rust SDK and registers the `ci` module |
| `dagger.lock` | Base image digests that the engine records and reuses |
| `.dagger/modules/ci/` | The module: a Cargo project whose functions are listed below |
| `.dagger/engine.sh` | Downloads, verifies and starts the pinned engine, and prints its environment |
| `.dagger/generate.sh` | Regenerates the module's generated code after an edit |
| `.buildkite/pipeline.yml` | Pipeline steps |
| `.buildkite/hooks/pre-command` | Starts the engine before each step |
| `.buildkite/lint.sh` | Limits lint to the lines a build changes |

The module never sees Buildkite or Namespace. It takes the repository through its constructor, which defaults to the workspace root without `.git`, `.dagger`, `docs` and build output. Pass `dagger call ci --source=<dir> …` to use another tree.

## Functions

| Call | What it does |
|---|---|
| `build [--platform]` | Compiles `temporal-server`, `temporal-dsql-tool` and `temporal-elasticsearch-tool`, and returns them as a directory |
| `lint [--patch]` | Runs golangci-lint v2.9.0 and the errortype vet check, as `make lint-code` does |
| `test [--packages] [--run]` | Runs `go test -race -shuffle=on` on `common/persistence/sql/...`, `common/persistence/sql/sqlplugin/dsql/...`, `schema/...` and `tools/dsql/...` |
| `images [--platform]` | Builds the server and tool images and returns them as OCI tarballs |
| `publish --tags --password [--platform] [--revision]` | Pushes both images for one platform as `<tag>-<arch>` |
| `publish-index --tags --password [--platforms]` | Joins the `<tag>-<arch>` images into a multi-platform `<tag>` |
| `release --version [--platforms]` | Packages the binaries as `temporal-dsql_<version>_<os>_<arch>.tar.gz` with a `SHA256SUMS` file |

`--platform` defaults to the engine's own platform. Go cross-compiles, so `build` and `release` can target any platform from any engine. The images run `apk` for their own platform, so each architecture is built on a native agent. `publish` and `publish-index` default to `ghcr.io/iw`; `--registry`, `--owner` and `--username` change that.

Pass `lint` a patch. Upstream lints only new code (`--new-from-rev`), and a full run reports about 6,600 existing issues, 189 of them in DSQL code.

## Images

| Image | Contents |
|---|---|
| `temporal-dsql-server` | All three binaries in `/usr/local/bin` on `alpine:3.23`, with `ca-certificates`, `tzdata`, `curl`, `python3`, `bash` and `aws-cli` |
| `temporal-dsql-tool` | `temporal-dsql-tool` on `alpine:3.23`, with `ca-certificates` and `tzdata` |

Both images run as `temporal` (UID 1000). The server image's entrypoint is `/etc/temporal/entrypoint.sh`, which runs `temporal-server` with its arguments. Its default arguments are `--config-file /etc/temporal/config/development-dsql.yaml --allow-no-auth start`. No configuration is baked in, so mount it or add it in a derived image. The images carry `org.opencontainers.image.source`, which links the GHCR packages to this repository, and `publish` adds the commit as `org.opencontainers.image.revision`.

## Local use on Apple Silicon

You need Docker Desktop or OrbStack. Start the engine and put the matching CLI on `PATH`:

```bash
eval "$(.dagger/engine.sh)"
```

The first run downloads the darwin/arm64 CLI and the linux/arm64 engine archive into `~/.cache/temporal-dsql/dagger/`. It checks both against the release's `SHA256SUMS`, whose own digest is pinned in the script. It then loads the image, checks the image digest, and starts the `temporal-dsql-dagger-<version>` container. Later runs reuse the container and take about a second.

Then call the module:

```bash
dagger call ci build export --path dist/bin
dagger call ci test
dagger call ci test --packages ./common/persistence/sql/sqlplugin/dsql/... --run TestReservoir
git diff origin/main... > /tmp/lint.patch && dagger call ci lint --patch /tmp/lint.patch
dagger call ci images export --path dist/images
dagger call ci release --version v0.0.0-dev export --path dist/release
```

`docker load -i dist/images/temporal-dsql-server.tar` prints an untagged image ID; `docker tag` it before you run it. `.dagger/engine.sh stop` removes the engine container and keeps its cache volume.

If your global git config rewrites `https://github.com/` to SSH, run `dagger` with `GIT_CONFIG_GLOBAL=/dev/null`. The engine fetches the SDK over HTTPS and can't use your SSH keys. In a git worktree the CLI warns that it can't open the repository. The warning is harmless, but its ignore rules then don't apply, so keep large build output out of the module directory.

### Changing the module

Edit `.dagger/modules/ci/src/lib.rs`, then run `.dagger/generate.sh` and commit the regenerated files with it. Until you do, every `dagger` command in the workspace fails with `GENERATED_STALE`. Run it once per edit: each run changes the digests in the generated files, even when the source hasn't changed. For a quick type check, run `cargo clippy --locked` in the module directory. Point `CARGO_TARGET_DIR` outside the repository, so that a `target/` directory doesn't get uploaded with the module.

`lib.rs` contains three workarounds for the rust.4 SDK, each marked:

- the generated module is declared behind a default `generated` feature, because the authoring compiler rejects a plain `pub mod dagger_generated;`
- the declaration also re-exports the core types that the generated code names
- functions return a local `Error` type, because returning `dagger_sdk::ModuleError` directly fails authoring

`Cargo.lock` is committed. Re-resolving it fails, because the SDK pins `zip = "=8.2.0"` and crates.io has yanked that version.

## CI flow

Every build of a pull request, `main`, a release branch or a tag runs these steps:

| Step | Agents | Command |
|---|---|---|
| build | amd64 and arm64 | `dagger call ci build` |
| lint | arm64 | `.buildkite/lint.sh`: a pull request against its merge base, other builds against the parent commit |
| test | amd64 and arm64 | `dagger call ci test` |

A release tag build continues to a block step once those pass. Release tags look like `v1.31.4-dsql.1`; see [Branches and tags](#branches-and-tags). Nothing is published until someone unblocks the block step. Then:

1. `images` builds both images on each architecture.
2. `publish` pushes `<tag>-amd64` and `<tag>-arm64` from their own agents.
3. `publish-index` creates the multi-platform `<tag>` for `ghcr.io/iw/temporal-dsql-server` and `ghcr.io/iw/temporal-dsql-tool`.
4. `release`, which runs in parallel with the images, packages linux/amd64, linux/arm64 and darwin/arm64 binaries as Buildkite artifacts.

To publish the binaries, download the `release` step's artifacts and attach them to a GitHub release:

```bash
gh release create <tag> --repo iw/temporal-dsql --draft dist/release/*
```

The `test` step will fail until two existing test problems are fixed:

- six `TestReservoirRefiller_*` tests in `sqlplugin/dsql/driver` race on the unsynchronised `mockReservoirMetrics`
- `schema`'s `TestSchemaDirs` expects `dsql/v12/temporal`, but the schema lives in `dsql/temporal`

## Secrets

**GHCR token.** The publish steps read a Buildkite secret named `GHCR_TOKEN` and pass it to the module as `--password env://GHCR_TOKEN`, so the value never appears on a command line or in a log. Use a GitHub classic personal access token for the `iw` account with `write:packages`. Buildkite secrets in pipeline YAML need agent 3.106.0 or later.

Each build uploads the `pipeline.yml` from its own commit, so any build's YAML can ask for the secret. Protect it in two ways:

- leave building pull requests from forks off, which is Buildkite's default
- give the secret an access policy that admits only this pipeline (`pipeline_id`) and release tags (`build_branch: "v*-dsql.*"`; on a tag build it holds the tag name)

**AWS, for the DSQL integration tests to come.** Use Buildkite's OIDC tokens rather than stored keys:

1. Register `https://agent.buildkite.com` as an IAM OIDC identity provider with audience `sts.amazonaws.com`.
2. Create a role that trusts it. Restrict `agent.buildkite.com:sub`, which has the form `organization:<org>:pipeline:<pipeline>:ref:<ref>:commit:<commit>:step:<step>`, to this pipeline and the refs that may run against DSQL. Grant `dsql:DbConnectAdmin` on a test cluster only.
3. Assume the role in the step with the `aws-assume-role-with-web-identity` plugin. Pass the temporary credentials to the module as secrets, for example `--aws-session-token env://AWS_SESSION_TOKEN`.

No module function takes AWS credentials yet.

## Caching

Each step runs on a fresh Namespace agent, so it starts a fresh engine. The engine holds the Go module, Go build and golangci-lint caches as Dagger cache volumes. They speed up later operations in the same step and on a reused engine, but not across agents. `dagger.lock` pins the base images, so a cold engine resolves the same digests.

Start cold and measure. These numbers come from an M1 Pro (10 cores, 10 GB OrbStack VM) running the same operations on the rust.4 engine through the Dagger API:

| Operation | Cache | Time |
|---|---|---|
| Engine bootstrap | Download, verify and load | about 20 s |
| Engine bootstrap | Container already running | about 1 s |
| `build`, linux/arm64 | Cold | 66 s |
| `test`, default packages | Go modules warm, race build cold | 39 s |
| `images`, linux/arm64 | Binaries cached, Alpine cold | 14 s |
| `lint`, one-file patch | Cold, including compiling golangci-lint | 174 s |
| `release`, three platforms | linux/arm64 binaries cached | 126 s |
| Module compile, before the runtime fails | Cold | 35 s |

The next step, once the numbers justify it, is one long-lived engine per architecture. Steps would point `_EXPERIMENTAL_DAGGER_RUNNER_HOST` at it instead of starting their own, so the cache volumes outlive the agents, and the engine's garbage collection bounds disk use. Avoid Namespace's persistent cache volumes (`nsc-cache-tag`) for this. They reached about 60 GB after two build generations.

## Setting up Buildkite and Namespace

1. **Namespace queue.** Ask support@namespace.so to enable Buildkite agents. In the Namespace dashboard, connect the Buildkite organization with a Buildkite API token that never expires. The token needs `read_pipelines`, `read_builds`, `write_builds`, `read_clusters`, `write_clusters` and `read_organizations`. Pick the cluster and a queue named `namespace`, or change `queue:` in `pipeline.yml`. Then confirm with Namespace that Linux jobs have a local Docker daemon that can run privileged containers, since the steps set `nsc-container-privileged`. Also confirm that the agent is 3.106.0 or later.
2. **Webhook.** In Buildkite, add the webhook URL and token that Namespace shows. Select the `ping`, `job.scheduled`, `job.started` and `job.finished` events for all pipelines.
3. **Buildkite GitHub app.** Install it on `iw/temporal-dsql`, so that pushes, pull requests and tags trigger builds and commit statuses are posted. The repository is public, so the checkout needs no credentials.
4. **Buildkite pipeline.** Create `temporal-dsql` in the Namespace cluster with repository `https://github.com/iw/temporal-dsql.git`. Build pull requests, the `main` and `release/*` branches, and tags. Use this initial step, which reads `.buildkite/pipeline.yml`:

   ```yaml
   steps:
     - label: ":pipeline: upload"
       command: buildkite-agent pipeline upload
       agents:
         queue: namespace
         nsc-shape: 2x4
   ```

   Set `BUILDKITE_GIT_SUBMODULES=false` in the pipeline's environment as well. The only submodule holds Grafana dashboards.
5. **GHCR token.** Create the classic token and store it as the Buildkite secret `GHCR_TOKEN` in the Namespace cluster, with the access policy described under [Secrets](#secrets). After the first publish, `temporal-dsql-server` and `temporal-dsql-tool` appear under the `iw` account, linked to this repository. Set their visibility there.

## Branches and tags

- `main` is upstream `main` plus the DSQL work.
- `release/v1.31-dsql` is upstream tag `v1.31.4` with `main` merged in.
- Release tags are `v<upstream version>-dsql.<n>`, cut from the matching release branch. For example, `v1.31.4-dsql.1` is the first release on upstream v1.31.4, and `v1.31.4-dsql.2` the next one on the same base. Only tags of this form reach the release steps; other tags just run CI.

## Changing the Dagger release

The SDK only talks to an engine of exactly its own release, so these move together:

- `VERSION`, `COMMIT`, `SUMS_SHA256` and the image digests in `.dagger/engine.sh`
- `rev` in `.dagger/modules/ci/Cargo.toml`, and `Cargo.lock`
- `engineVersion` in `.dagger/modules/ci/dagger-module.toml`

Then run `.dagger/generate.sh`.

When stock engines can load the fork's Rust SDK, or when moving to a managed engine:

- delete `.dagger/engine.sh`, `.dagger/generate.sh` and the pre-command hook
- let a stock `dagger` CLI provision its own engine
- drop the workarounds in `lib.rs` that the new SDK fixes

The functions and the pipeline commands stay the same.
