//! Runs the DSQL plugin's live tests on a throwaway Aurora DSQL cluster and DynamoDB
//! table, and deletes both however the run ends.
//!
//! It runs on a developer's machine, from this directory, with the `temporal-dsql-ci`
//! profile and the engine that `.dagger/engine.sh` starts:
//!
//! ```text
//! AWS_PROFILE=temporal-dsql-ci dagger run cargo run --locked
//! ```
//!
//! The AWS calls here use the local credential provider chain. The Go code under test,
//! the schema tool and the integration tests, runs in the Dagger engine, which gets only
//! short-lived session credentials and the cluster's endpoint, as secrets.

use std::error::Error;
use std::path::Path;
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_config::meta::region::RegionProviderChain;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_dsql::client::Waiters as _;
use aws_sdk_dsql::error::DisplayErrorContext;
use aws_sdk_dsql::types::ClusterStatus;
use aws_sdk_dynamodb::client::Waiters as _;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType, TableStatus,
    Tag, TimeToLiveSpecification,
};
use dagger_sdk::{Container, HostDirectoryOpts, Query, QueryError};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// Tags every resource a run creates, and starts every table's name.
const TAG: &str = "temporal-dsql-ci";
const DEFAULT_REGION: &str = "eu-west-1";
/// Resources older than this were left by an interrupted run, since no run takes so long.
const LEFTOVER_AGE: Duration = Duration::from_secs(3 * 60 * 60);
/// Outlasts a run, so the Go code's credentials don't expire part way.
const SESSION_SECONDS: i32 = 60 * 60;
const REPO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
/// What the Go container leaves out of the repository, as the ci module's source does.
const EXCLUDES: &[&str] = &[
    ".git",
    ".claude",
    ".cursor",
    ".idea",
    ".vscode",
    ".dagger",
    ".buildkite",
    "**/.DS_Store",
    ".bin",
    ".stamp",
    ".tmp",
    ".testoutput",
    ".coverage",
    "dist",
    "develop/docker-compose",
    "temporal-*",
    "tdbg",
    "tctl*",
    "fairsim",
    "poolsim",
];

struct Aws {
    dsql: aws_sdk_dsql::Client,
    dynamodb: aws_sdk_dynamodb::Client,
    sts: aws_sdk_sts::Client,
}

/// One run's names.
struct Run {
    id: String,
    region: String,
    cluster: String,
    table: String,
}

struct SessionCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
}

#[tokio::main]
async fn main() -> ExitCode {
    let started = Instant::now();
    match live().await {
        Ok(()) => {
            println!("--- passed in {}s", started.elapsed().as_secs());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("--- failed after {}s: {err}", started.elapsed().as_secs());
            ExitCode::FAILURE
        }
    }
}

async fn live() -> Result<()> {
    let region = RegionProviderChain::default_provider().or_else(Region::new(DEFAULT_REGION));
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .load()
        .await;
    let aws = Aws {
        dsql: aws_sdk_dsql::Client::new(&config),
        dynamodb: aws_sdk_dynamodb::Client::new(&config),
        sts: aws_sdk_sts::Client::new(&config),
    };
    let identity = aws
        .sts
        .get_caller_identity()
        .send()
        .await
        .map_err(failed("find the AWS identity"))?;
    let mut masker = Masker {
        account: identity.account().unwrap_or_default().to_owned(),
        cluster: String::new(),
    };

    if let Err(err) = sweep(&aws).await {
        println!(
            "couldn't check for resources earlier runs left behind: {}",
            masker.mask(&err.to_string())
        );
    }

    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();
    let region = config
        .region()
        .map_or_else(|| DEFAULT_REGION.to_owned(), ToString::to_string);
    println!("--- creating a cluster and a table in {region}");
    let created = aws
        .dsql
        .create_cluster()
        .deletion_protection_enabled(false)
        .tags("purpose", TAG)
        .tags("run", &id)
        .send()
        .await
        .map_err(|err| masker.mask(&describe("create the cluster", &err)))?;
    masker.cluster = created.identifier().to_owned();
    let run = Run {
        table: format!("{TAG}-{id}"),
        cluster: masker.cluster.clone(),
        region,
        id,
    };

    // Ctrl-C ends the run early, but still deletes what it created.
    let outcome = tokio::select! {
        outcome = exercise(&config, &aws, &run, &masker) => outcome,
        _ = tokio::signal::ctrl_c() => Err("interrupted".into()),
    };
    println!("--- deleting the cluster and the table");
    let cleaned = cleanup(&aws, &run).await;
    outcome
        .and(cleaned)
        .map_err(|err| masker.mask(&err.to_string()).into())
}

/// Creates the table, waits for both resources, then runs the Go code against them.
async fn exercise(config: &SdkConfig, aws: &Aws, run: &Run, masker: &Masker) -> Result<()> {
    aws.dynamodb
        .create_table()
        .table_name(&run.table)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()?,
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()?,
        )
        .tags(Tag::builder().key("purpose").value(TAG).build()?)
        .tags(Tag::builder().key("run").value(&run.id).build()?)
        .send()
        .await
        .map_err(failed("create the table"))?;
    aws.dynamodb
        .wait_until_table_exists()
        .table_name(&run.table)
        .wait(Duration::from_secs(300))
        .await
        .map_err(failed("wait for the table"))?;
    aws.dynamodb
        .update_time_to_live()
        .table_name(&run.table)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(true)
                .attribute_name("ttl_epoch")
                .build()?,
        )
        .send()
        .await
        .map_err(failed("set the table's TTL"))?;
    aws.dsql
        .wait_until_cluster_active()
        .identifier(&run.cluster)
        .wait(Duration::from_secs(900))
        .await
        .map_err(failed("wait for the cluster"))?;

    let credentials = session_credentials(config, aws).await?;
    let client = dagger_sdk::connect().await?;
    let outcome = go_steps(&client.query(), &credentials, run, masker).await;
    client.close().await?;
    outcome
}

/// Short-lived credentials for the Go code, so the profile's own keys stay on this machine.
async fn session_credentials(config: &SdkConfig, aws: &Aws) -> Result<SessionCredentials> {
    let provider = config
        .credentials_provider()
        .ok_or("no AWS credentials; set AWS_PROFILE=temporal-dsql-ci")?;
    let current = provider.provide_credentials().await?;
    if let Some(token) = current.session_token() {
        // Already temporary, such as an assumed role's.
        return Ok(SessionCredentials {
            access_key_id: current.access_key_id().to_owned(),
            secret_access_key: current.secret_access_key().to_owned(),
            session_token: token.to_owned(),
        });
    }
    let output = aws
        .sts
        .get_session_token()
        .duration_seconds(SESSION_SECONDS)
        .send()
        .await
        .map_err(failed("get a session token"))?;
    let session = output.credentials().ok_or("STS returned no credentials")?;
    Ok(SessionCredentials {
        access_key_id: session.access_key_id().to_owned(),
        secret_access_key: session.secret_access_key().to_owned(),
        session_token: session.session_token().to_owned(),
    })
}

/// Sets up the persistence and visibility schemas and runs the plugin's integration
/// tests, in the golang image the ci module's test function uses.
async fn go_steps(
    dag: &Query,
    credentials: &SessionCredentials,
    run: &Run,
    masker: &Masker,
) -> Result<()> {
    let repo = std::fs::canonicalize(REPO)?;
    let go = go_version(&repo)?;
    let source = dag.host().directory_opts(
        repo.to_string_lossy().into_owned(),
        &HostDirectoryOpts::default().with_exclude(EXCLUDES.to_vec()),
    );
    let endpoint = dag.set_secret(
        "temporal-dsql-ci-endpoint",
        format!("{}.dsql.{}.on.aws", run.cluster, run.region),
    );
    let runner = dag
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
        .with_mounted_cache(dag.cache_volume("temporal-dsql-go-mod"), "/go/pkg/mod")
        .with_mounted_cache(
            dag.cache_volume(format!("temporal-dsql-go-build-{go}")),
            "/root/.cache/go-build",
        )
        .with_directory("/src", source)
        .with_workdir("/src")
        // A run ID of its own keeps the engine from reusing an earlier run's results.
        .with_env_variable("CI_RUN", &run.id)
        .with_env_variable("AWS_REGION", &run.region)
        .with_secret_variable(
            "AWS_ACCESS_KEY_ID",
            dag.set_secret("temporal-dsql-ci-access-key-id", &credentials.access_key_id),
        )
        .with_secret_variable(
            "AWS_SECRET_ACCESS_KEY",
            dag.set_secret(
                "temporal-dsql-ci-secret-access-key",
                &credentials.secret_access_key,
            ),
        )
        .with_secret_variable(
            "AWS_SESSION_TOKEN",
            dag.set_secret("temporal-dsql-ci-session-token", &credentials.session_token),
        )
        // The schema tool reads its endpoint from SQL_HOST, and the tests from
        // DSQL_TEST_ENDPOINT.
        .with_secret_variable("SQL_HOST", endpoint.clone())
        .with_secret_variable("DSQL_TEST_ENDPOINT", endpoint)
        .with_env_variable("DSQL_TEST_DDB_TABLE", &run.table);

    println!("--- setting up the persistence schema");
    let persistence = runner.with_exec(vec![
        "go",
        "run",
        "./cmd/tools/dsql",
        "setup-schema",
        "--schema-name",
        "dsql/temporal",
        "--version",
        "1.3",
    ]);
    print_step(&persistence, masker).await?;

    println!("--- setting up the visibility schema");
    let schema = persistence.with_exec(vec![
        "go",
        "run",
        "./cmd/tools/dsql",
        "setup-schema",
        "--schema-name",
        "dsql/visibility",
        "--version",
        "1.0",
    ]);
    print_step(&schema, masker).await?;

    println!("--- running the live tests");
    let tests = schema.with_exec(vec![
        "go",
        "test",
        "-count=1",
        "-tags",
        "integration",
        "./common/persistence/sql/sqlplugin/dsql/...",
    ]);
    print_step(&tests, masker).await
}

/// Prints what a step wrote, and fails with its output when the step fails.
async fn print_step(step: &Container, masker: &Masker) -> Result<()> {
    match step.stdout().await {
        Ok(stdout) => {
            print_output(&step.stderr().await?, &stdout, masker);
            Ok(())
        }
        Err(QueryError::Exec { error, .. }) => {
            print_output(
                error.stderr().unwrap_or_default(),
                error.stdout().unwrap_or_default(),
                masker,
            );
            Err("the step failed".into())
        }
        Err(err) => Err(err.into()),
    }
}

fn print_output(stderr: &str, stdout: &str, masker: &Masker) {
    // The schema tool logs each statement at debug level, which the run leaves out.
    for line in stderr.lines().chain(stdout.lines()) {
        if !line.contains("DEBUG") {
            println!("{}", masker.mask(line));
        }
    }
}

/// Deletes CI-tagged clusters and CI tables that interrupted runs left behind.
async fn sweep(aws: &Aws) -> Result<()> {
    let cutoff = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .saturating_sub(LEFTOVER_AGE)
            .as_secs(),
    )?;
    let mut clusters = aws.dsql.list_clusters().into_paginator().items().send();
    while let Some(summary) = clusters.next().await {
        let summary = summary.map_err(failed("list clusters"))?;
        let tags = aws
            .dsql
            .list_tags_for_resource()
            .resource_arn(summary.arn())
            .send()
            .await
            .map_err(failed("read a cluster's tags"))?;
        if tags
            .tags()
            .and_then(|tags| tags.get("purpose"))
            .map(String::as_str)
            != Some(TAG)
        {
            continue;
        }
        let cluster = aws
            .dsql
            .get_cluster()
            .identifier(summary.identifier())
            .send()
            .await
            .map_err(failed("read a cluster"))?;
        let deleting = matches!(
            cluster.status(),
            ClusterStatus::Deleting | ClusterStatus::Deleted
        );
        if !deleting && cluster.creation_time().secs() < cutoff {
            aws.dsql
                .delete_cluster()
                .identifier(summary.identifier())
                .send()
                .await
                .map_err(failed("delete a cluster"))?;
            println!("deleted a cluster an earlier run left behind");
        }
    }

    let prefix = format!("{TAG}-");
    let mut tables = aws.dynamodb.list_tables().into_paginator().items().send();
    while let Some(name) = tables.next().await {
        let name = name.map_err(failed("list tables"))?;
        if !name.starts_with(&prefix) {
            continue;
        }
        let described = aws
            .dynamodb
            .describe_table()
            .table_name(&name)
            .send()
            .await
            .map_err(failed("read a table"))?;
        let Some(table) = described.table() else {
            continue;
        };
        let old = table
            .creation_date_time()
            .is_some_and(|created| created.secs() < cutoff);
        if old && table.table_status() != Some(&TableStatus::Deleting) {
            aws.dynamodb
                .delete_table()
                .table_name(&name)
                .send()
                .await
                .map_err(failed("delete a table"))?;
            println!("deleted a table an earlier run left behind");
        }
    }
    Ok(())
}

/// Deletes the run's cluster and table, and reports every failure.
async fn cleanup(aws: &Aws, run: &Run) -> Result<()> {
    let mut failures = Vec::new();
    if let Err(err) = aws
        .dsql
        .delete_cluster()
        .identifier(&run.cluster)
        .send()
        .await
    {
        failures.push(describe("delete the cluster", &err));
    }
    match aws
        .dynamodb
        .delete_table()
        .table_name(&run.table)
        .send()
        .await
    {
        Ok(_) => {}
        // The run stopped before it created the table.
        Err(err)
            if err
                .as_service_error()
                .is_some_and(|err| err.is_resource_not_found_exception()) => {}
        Err(err) => failures.push(describe("delete the table", &err)),
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("{}; the next run deletes what's left", failures.join("; ")).into())
    }
}

/// The language version from go.mod as major.minor, the tag granularity of the golang
/// images.
fn go_version(repo: &Path) -> Result<String> {
    let go_mod = std::fs::read_to_string(repo.join("go.mod"))?;
    let version = go_mod
        .lines()
        .find_map(|line| line.strip_prefix("go "))
        .ok_or("go.mod has no go directive")?
        .trim();
    let mut parts = version.split('.');
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => Ok(format!("{major}.{minor}")),
        _ => Err(format!("unexpected go directive: {version}").into()),
    }
}

/// Describes a failed AWS call with its whole error chain, which the SDK's own message
/// leaves out.
fn describe(what: &str, err: &dyn Error) -> String {
    format!("couldn't {what}: {}", DisplayErrorContext(err))
}

fn failed<E: Error>(what: &'static str) -> impl FnOnce(E) -> Box<dyn Error + Send + Sync> {
    move |err| describe(what, &err).into()
}

/// Hides the account and cluster IDs in what a run prints, since the output can end up
/// in shared logs and traces.
struct Masker {
    account: String,
    cluster: String,
}

impl Masker {
    fn mask(&self, text: &str) -> String {
        let mut text = text.to_owned();
        for (id, placeholder) in [(&self.account, "<account>"), (&self.cluster, "<cluster>")] {
            if !id.is_empty() {
                text = text.replace(id.as_str(), placeholder);
            }
        }
        text
    }
}
