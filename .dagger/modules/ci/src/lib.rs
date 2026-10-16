//! Build, test and release pipeline for temporal-dsql.
//!
//! Every function runs inside the Dagger engine, so the same calls work from a
//! laptop, a CI agent or a managed engine. Orchestration, secrets and engine
//! provisioning stay outside the module.

// Dagger functions take their options as arguments.
#![allow(clippy::too_many_arguments)]

// Two workarounds for the rust.4 SDK. The authoring compiler rejects a declared
// dagger_generated module, but it evaluates cfg with no features enabled, so a
// default feature hides the module from it while cargo still compiles it. The
// generated code also names core types through this module without
// re-exporting them, so the wrapper re-exports the ones the functions use.
#[cfg(feature = "generated")]
pub mod dagger_generated {
    pub use dagger_sdk::{Directory, File, Secret};

    #[allow(clippy::get_first)]
    #[path = "mod.rs"]
    mod generated;
    pub use generated::*;
}

use dagger_generated::ModuleContext;
use dagger_sdk::{
    Container, ContainerPublishOpts, ContainerWithNewFileOpts, Directory, File, IdInput, Platform,
    QueryContainerOpts, Secret,
};

const BUILD_TAGS: &str = "disable_grpc_modules";
const TEST_TAGS: &str = "disable_grpc_modules,test_dep";
const ALPINE: &str = "alpine:3.23";
const GOLANGCI_LINT: &str = "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.9.0";
const ERRORTYPE: &str = "fillmore-labs.com/errortype@v0.0.7";
const SOURCE_URL: &str = "https://github.com/iw/temporal-dsql";
const SERVER_IMAGE: &str = "temporal-dsql-server";
const TOOL_IMAGE: &str = "temporal-dsql-tool";
const BINARIES: [(&str, &str); 3] = [
    ("temporal-server", "./cmd/server"),
    ("temporal-dsql-tool", "./cmd/tools/dsql"),
    ("temporal-elasticsearch-tool", "./cmd/tools/elasticsearch"),
];
const ENTRYPOINT: &str = "#!/bin/sh\nset -eu\nexec /usr/local/bin/temporal-server \"$@\"\n";

/// Failure returned to the Dagger caller; keeps the engine's exit code and output.
/// A local type because the rust.4 authoring compiler rejects functions that
/// return `dagger_sdk::ModuleError` directly.
#[derive(Debug)]
pub struct Error(dagger_sdk::ModuleError);

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self(dagger_sdk::ModuleError::new(message))
    }
}

impl From<dagger_sdk::QueryError> for Error {
    fn from(error: dagger_sdk::QueryError) -> Self {
        Self(error.into())
    }
}

impl From<Error> for dagger_sdk::ModuleError {
    fn from(error: Error) -> Self {
        error.0
    }
}

#[dagger_sdk::object(root, rename = "Ci")]
pub struct Ci {
    #[dagger(state)]
    source: Directory,
}

#[dagger_sdk::methods]
impl Ci {
    #[dagger(constructor, rename = "ci")]
    pub fn new(
        #[dagger(
            doc = "Repository checkout; defaults to the workspace root.",
            default_path = "/",
            ignore = [
                ".git", ".claude", ".cursor", ".idea", ".vscode", ".dagger", ".buildkite",
                "**/.DS_Store", ".bin", ".stamp", ".tmp", ".testoutput", ".coverage", "dist",
                "docs", "develop/docker-compose", "temporal-*", "tdbg", "tctl*", "fairsim", "poolsim"
            ]
        )]
        source: Directory,
    ) -> Ci {
        Ci { source }
    }

    /// Compiles temporal-server, temporal-dsql-tool and temporal-elasticsearch-tool.
    #[dagger(function)]
    pub async fn build(
        &self,
        #[dagger(context)] ctx: ModuleContext,
        #[dagger(doc = "Target platform, such as linux/arm64; defaults to the engine's platform.")]
        platform: Option<String>,
    ) -> Result<Directory, Error> {
        let platform = resolve_platform(&ctx, platform).await?;
        self.binaries(&ctx, &platform).await
    }

    /// Runs golangci-lint and the errortype vet check, as make lint-code does.
    #[dagger(function)]
    pub async fn lint(
        &self,
        #[dagger(context)] ctx: ModuleContext,
        #[dagger(
            doc = "Unified diff that limits golangci-lint to the lines it changes; lints everything when omitted."
        )]
        patch: Option<File>,
    ) -> Result<String, Error> {
        let query = ctx.query();
        let mut golangci = vec![
            "golangci-lint",
            "run",
            "--build-tags",
            TEST_TAGS,
            "--timeout",
            "30m",
            "--fix=false",
            "--config=.github/.golangci.yml",
        ];
        let mut linter = self
            .go_builder(&ctx)
            .await?
            .with_mounted_cache(
                query.cache_volume("temporal-dsql-golangci-lint"),
                "/root/.cache/golangci-lint",
            )
            .with_exec(vec!["go", "install", GOLANGCI_LINT])
            .with_exec(vec!["go", "install", ERRORTYPE]);
        if let Some(patch) = patch {
            linter = linter.with_file("/tmp/lint.patch", patch);
            golangci.push("--new-from-patch=/tmp/lint.patch");
        }
        Ok(linter
            .with_exec(golangci)
            .with_exec(vec![
                "go",
                "vet",
                "-tags",
                TEST_TAGS,
                "-vettool=/go/bin/errortype",
                "-style-check=false",
                "./...",
            ])
            .stdout()
            .await?)
    }

    /// Runs the SQL persistence, DSQL plugin, schema and DSQL tool tests with the race detector.
    #[dagger(function)]
    pub async fn test(
        &self,
        #[dagger(context)] ctx: ModuleContext,
        #[dagger(doc = "Package patterns to test instead of the default set.")] packages: Option<
            Vec<String>,
        >,
        #[dagger(doc = "Only run tests matching this regular expression, as go test -run.")]
        run: Option<String>,
    ) -> Result<String, Error> {
        let mut args = strings(&[
            "go",
            "test",
            "-race",
            "-shuffle=on",
            "-timeout=35m",
            "-tags",
            TEST_TAGS,
        ]);
        if let Some(run) = run {
            args.extend(["-run".to_owned(), run]);
        }
        args.extend(packages.unwrap_or_else(|| {
            strings(&[
                "./common/persistence/sql/...",
                "./common/persistence/sql/sqlplugin/dsql/...",
                "./schema/...",
                "./tools/dsql/...",
            ])
        }));
        Ok(self
            .go_builder(&ctx)
            .await?
            .with_env_variable("CGO_ENABLED", "1")
            .with_exec(args)
            .stdout()
            .await?)
    }

    /// Builds the server and tool images and returns them as OCI tarballs.
    #[dagger(function)]
    pub async fn images(
        &self,
        #[dagger(context)] ctx: ModuleContext,
        #[dagger(doc = "Target platform, such as linux/arm64; defaults to the engine's platform.")]
        platform: Option<String>,
    ) -> Result<Directory, Error> {
        let platform = resolve_platform(&ctx, platform).await?;
        let mut out = ctx.query().directory();
        for (name, image) in self.image_set(&ctx, &platform, None).await? {
            out = out.with_file(format!("{name}.tar"), image.as_tarball());
        }
        Ok(out)
    }

    /// Pushes the server and tool images for one platform as <tag>-<arch>.
    #[dagger(function, cache = "never")]
    pub async fn publish(
        &self,
        #[dagger(context)] ctx: ModuleContext,
        #[dagger(doc = "Tags to push; each gets the platform's architecture appended.")] tags: Vec<
            String,
        >,
        #[dagger(doc = "Registry token, such as a GitHub token with write:packages.")]
        password: Secret,
        #[dagger(doc = "Target platform, such as linux/arm64; defaults to the engine's platform.")]
        platform: Option<String>,
        #[dagger(doc = "Registry host.", default = "ghcr.io")] registry: String,
        #[dagger(doc = "Registry namespace that holds the images.", default = "iw")] owner: String,
        #[dagger(doc = "Registry user; defaults to the owner.")] username: Option<String>,
        #[dagger(doc = "Commit recorded in the org.opencontainers.image.revision label.")]
        revision: Option<String>,
    ) -> Result<Vec<String>, Error> {
        let platform = resolve_platform(&ctx, platform).await?;
        let (_, arch) = split_platform(&platform)?;
        let username = username.unwrap_or_else(|| owner.clone());
        let mut refs = Vec::new();
        for (name, image) in self.image_set(&ctx, &platform, revision.as_deref()).await? {
            let image =
                image.with_registry_auth(registry.as_str(), password.clone(), username.as_str());
            for tag in &tags {
                refs.push(
                    image
                        .publish(format!("{registry}/{owner}/{name}:{tag}-{arch}"))
                        .await?,
                );
            }
        }
        Ok(refs)
    }

    /// Combines the per-architecture images that publish pushed into multi-platform tags.
    #[dagger(function, cache = "never")]
    pub async fn publish_index(
        &self,
        #[dagger(context)] ctx: ModuleContext,
        #[dagger(doc = "Tags to create from the <tag>-<arch> images.")] tags: Vec<String>,
        #[dagger(doc = "Registry token, such as a GitHub token with write:packages.")]
        password: Secret,
        #[dagger(doc = "Platforms the index lists; each must already be published.")]
        platforms: Option<Vec<String>>,
        #[dagger(doc = "Registry host.", default = "ghcr.io")] registry: String,
        #[dagger(doc = "Registry namespace that holds the images.", default = "iw")] owner: String,
        #[dagger(doc = "Registry user; defaults to the owner.")] username: Option<String>,
    ) -> Result<Vec<String>, Error> {
        let platforms = platforms.unwrap_or_else(|| strings(&["linux/amd64", "linux/arm64"]));
        let username = username.unwrap_or_else(|| owner.clone());
        let mut refs = Vec::new();
        for name in [SERVER_IMAGE, TOOL_IMAGE] {
            for tag in &tags {
                let mut variants = Vec::new();
                for platform in &platforms {
                    let (_, arch) = split_platform(platform)?;
                    variants.push(IdInput::from(
                        platform_container(&ctx, platform)
                            .with_registry_auth(
                                registry.as_str(),
                                password.clone(),
                                username.as_str(),
                            )
                            .from(format!("{registry}/{owner}/{name}:{tag}-{arch}")),
                    ));
                }
                refs.push(
                    ctx.query()
                        .container()
                        .with_registry_auth(registry.as_str(), password.clone(), username.as_str())
                        .publish_opts(
                            format!("{registry}/{owner}/{name}:{tag}"),
                            &ContainerPublishOpts::default().with_platform_variants(variants),
                        )
                        .await?,
                );
            }
        }
        Ok(refs)
    }

    /// Packages the binaries for each platform as tar.gz archives with SHA256SUMS.
    #[dagger(function)]
    pub async fn release(
        &self,
        #[dagger(context)] ctx: ModuleContext,
        #[dagger(doc = "Release version, such as v1.31.4-dsql.1; names the archives.")]
        version: String,
        #[dagger(
            doc = "Platforms to package; defaults to linux/amd64, linux/arm64 and darwin/arm64."
        )]
        platforms: Option<Vec<String>>,
    ) -> Result<Directory, Error> {
        let version = version.trim_start_matches('v');
        let platforms =
            platforms.unwrap_or_else(|| strings(&["linux/amd64", "linux/arm64", "darwin/arm64"]));
        let mut packer = ctx
            .query()
            .container()
            .from(ALPINE)
            .with_exec(vec!["apk", "add", "--no-cache", "tar"])
            .with_workdir("/dist");
        for platform in &platforms {
            let (os, arch) = split_platform(platform)?;
            let name = format!("temporal-dsql_{version}_{os}_{arch}");
            let stage = format!("/stage/{name}");
            let files = BINARIES.map(|(binary, _)| binary).join(" ");
            packer = packer
                .with_directory(stage.as_str(), self.binaries(&ctx, platform).await?)
                .with_file(format!("{stage}/LICENSE"), self.source.file("LICENSE"))
                // Fixed owners, order and mtime make the archives reproducible.
                .with_exec(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    format!(
                        "tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
                         -C {stage} -cf - LICENSE {files} | gzip -n > /dist/{name}.tar.gz"
                    ),
                ]);
        }
        Ok(packer
            .with_exec(vec!["sh", "-c", "sha256sum *.tar.gz > SHA256SUMS"])
            .directory("/dist"))
    }
}

impl Ci {
    /// Go toolchain matching go.mod, with module and build caches and the source at /src.
    async fn go_builder(&self, ctx: &ModuleContext) -> Result<Container, Error> {
        let go = go_version(&self.source).await?;
        let query = ctx.query();
        Ok(query
            .container()
            .from(format!("golang:{go}-alpine"))
            .with_exec(vec![
                "apk",
                "add",
                "--no-cache",
                "make",
                "git",
                "gcc",
                "musl-dev",
            ])
            .with_mounted_cache(query.cache_volume("temporal-dsql-go-mod"), "/go/pkg/mod")
            .with_mounted_cache(
                query.cache_volume(format!("temporal-dsql-go-build-{go}")),
                "/root/.cache/go-build",
            )
            .with_directory("/src", self.source.clone())
            .with_workdir("/src"))
    }

    /// Cross-compiles the release binaries on the engine's own platform.
    async fn binaries(&self, ctx: &ModuleContext, platform: &str) -> Result<Directory, Error> {
        let (os, arch) = split_platform(platform)?;
        let mut builder = self
            .go_builder(ctx)
            .await?
            .with_env_variable("CGO_ENABLED", "0")
            .with_env_variable("GOOS", os)
            .with_env_variable("GOARCH", arch);
        for (binary, package) in BINARIES {
            builder = builder.with_exec(vec![
                "go".to_owned(),
                "build".to_owned(),
                "-tags".to_owned(),
                BUILD_TAGS.to_owned(),
                "-o".to_owned(),
                format!("/out/{binary}"),
                package.to_owned(),
            ]);
        }
        Ok(builder.directory("/out"))
    }

    /// The server and tool images for one platform. The server image keeps the
    /// layout DSQL deployments already expect: binaries in /usr/local/bin,
    /// config under /etc/temporal/config, and the temporal user.
    async fn image_set(
        &self,
        ctx: &ModuleContext,
        platform: &str,
        revision: Option<&str>,
    ) -> Result<[(&'static str, Container); 2], Error> {
        let binaries = self.binaries(ctx, platform).await?;
        let server = platform_container(ctx, platform)
            .from(ALPINE)
            .with_exec(vec![
                "sh",
                "-c",
                "apk add --no-cache ca-certificates tzdata curl python3 bash aws-cli \
                 && addgroup -g 1000 temporal && adduser -u 1000 -G temporal -D temporal \
                 && mkdir -p /etc/temporal/config/dynamicconfig \
                 && chown -R temporal:temporal /etc/temporal",
            ])
            .with_workdir("/etc/temporal")
            .with_env_variable("TEMPORAL_HOME", "/etc/temporal")
            .with_new_file_opts(
                ENTRYPOINT,
                "/etc/temporal/entrypoint.sh",
                &ContainerWithNewFileOpts::default().with_permissions(0o755),
            );
        let server = BINARIES.iter().fold(server, |image, (binary, _)| {
            image.with_file(format!("/usr/local/bin/{binary}"), binaries.file(*binary))
        });
        let server = server
            .with_user("temporal")
            .with_entrypoint(vec!["/etc/temporal/entrypoint.sh"])
            .with_default_args(vec![
                "--config-file",
                "/etc/temporal/config/development-dsql.yaml",
                "--allow-no-auth",
                "start",
            ]);
        let tool = platform_container(ctx, platform)
            .from(ALPINE)
            .with_exec(vec![
                "sh",
                "-c",
                "apk add --no-cache ca-certificates tzdata \
                 && addgroup -g 1000 temporal && adduser -u 1000 -G temporal -D temporal",
            ])
            .with_file(
                "/usr/local/bin/temporal-dsql-tool",
                binaries.file("temporal-dsql-tool"),
            )
            .with_user("temporal")
            .with_entrypoint(vec!["/usr/local/bin/temporal-dsql-tool"])
            .with_default_args(Vec::<String>::new());
        Ok([
            (
                SERVER_IMAGE,
                labeled(
                    server,
                    "Temporal server with the Aurora DSQL persistence plugin",
                    revision,
                ),
            ),
            (
                TOOL_IMAGE,
                labeled(tool, "Schema tool for Temporal on Aurora DSQL", revision),
            ),
        ])
    }
}

fn labeled(image: Container, description: &str, revision: Option<&str>) -> Container {
    let image = image
        .with_label("org.opencontainers.image.source", SOURCE_URL)
        .with_label("org.opencontainers.image.description", description)
        .with_label("org.opencontainers.image.licenses", "MIT");
    match revision {
        Some(revision) => image.with_label("org.opencontainers.image.revision", revision),
        None => image,
    }
}

fn platform_container(ctx: &ModuleContext, platform: &str) -> Container {
    ctx.query()
        .container_opts(&QueryContainerOpts::default().with_platform(Platform::new(platform)))
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

/// Reads the language version from go.mod and returns its major.minor, the tag
/// granularity of the official golang images.
async fn go_version(source: &Directory) -> Result<String, Error> {
    let go_mod = source.file("go.mod").contents().await?;
    let version = go_mod
        .lines()
        .find_map(|line| line.strip_prefix("go "))
        .ok_or_else(|| Error::new("go.mod has no go directive"))?
        .trim();
    let mut parts = version.split('.');
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => Ok(format!("{major}.{minor}")),
        _ => Err(Error::new(format!("unexpected go directive: {version}"))),
    }
}

async fn resolve_platform(ctx: &ModuleContext, platform: Option<String>) -> Result<String, Error> {
    match platform {
        Some(platform) => Ok(platform),
        None => Ok(ctx.query().default_platform().await?.into_inner()),
    }
}

/// Splits os/arch[/variant] into the GOOS and GOARCH values.
fn split_platform(platform: &str) -> Result<(String, String), Error> {
    let mut parts = platform.split('/');
    match (parts.next(), parts.next()) {
        (Some(os), Some(arch)) if !os.is_empty() && !arch.is_empty() => {
            Ok((os.to_owned(), arch.to_owned()))
        }
        _ => Err(Error::new(format!(
            "platform must look like linux/amd64, got {platform}"
        ))),
    }
}
