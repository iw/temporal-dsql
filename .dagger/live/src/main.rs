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
//!
//! With `--corpus`, the run then sets up more persistence schemas and runs Temporal's
//! functional tests against the cluster. Their in-process servers don't run in the
//! engine, so they run on this machine, with the profile's credentials from the default
//! chain, and write their output, masked, to `target/corpus/<run>`:
//!
//! ```text
//! AWS_PROFILE=temporal-dsql-ci dagger run cargo run --locked -- --corpus
//! ```

use std::collections::BTreeSet;
use std::error::Error;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{ExitCode, ExitStatus, Stdio};
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
use tokio::io::{AsyncBufReadExt as _, AsyncRead, BufReader};
use tokio::process::Command;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// Tags every resource a run creates, and starts every table's name.
const TAG: &str = "temporal-dsql-ci";
const DEFAULT_REGION: &str = "eu-west-1";
/// Resources older than this were left by an interrupted run, since no run takes so long.
const LEFTOVER_AGE: Duration = Duration::from_secs(12 * 60 * 60);
/// The persistence schemas the functional tests' clusters lease besides public. With
/// public and the visibility schema, they make the ten schemas DSQL allows a database.
const CORPUS_SCHEMAS: usize = 8;
/// Bounds the functional tests, which take hours against DSQL.
const CORPUS_TIMEOUT: &str = "8h";
const GO_TEST_TAGS: &str = "disable_grpc_modules,test_dep";
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
    let mut corpus = false;
    for arg in std::env::args().skip(1) {
        if arg == "--corpus" {
            corpus = true;
        } else {
            eprintln!("unknown argument {arg}; the run takes only --corpus");
            return ExitCode::FAILURE;
        }
    }
    match live(corpus).await {
        Ok(()) => {
            // The functional tests' failures are in their summary, and don't fail the run.
            let ended = if corpus { "finished" } else { "passed" };
            println!("--- {ended} in {}s", started.elapsed().as_secs());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("--- failed after {}s: {err}", started.elapsed().as_secs());
            ExitCode::FAILURE
        }
    }
}

async fn live(corpus: bool) -> Result<()> {
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
        outcome = exercise(&config, &aws, &run, &masker, corpus) => outcome,
        _ = tokio::signal::ctrl_c() => Err("interrupted".into()),
    };
    println!("--- deleting the cluster and the table");
    let cleaned = cleanup(&aws, &run).await;
    outcome
        .and(cleaned)
        .map_err(|err| masker.mask(&err.to_string()).into())
}

/// Creates the table, waits for both resources, then runs the Go code against them.
async fn exercise(
    config: &SdkConfig,
    aws: &Aws,
    run: &Run,
    masker: &Masker,
    corpus: bool,
) -> Result<()> {
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
    let outcome = go_steps(&client.query(), &credentials, run, masker, corpus).await;
    client.close().await?;
    outcome?;
    if corpus {
        functional_tests(run, masker).await?;
    }
    Ok(())
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
/// tests, in the golang image the ci module's test function uses. For the functional
/// tests, it sets up their further schemas too.
async fn go_steps(
    dag: &Query,
    credentials: &SessionCredentials,
    run: &Run,
    masker: &Masker,
    corpus: bool,
) -> Result<()> {
    let repo = std::fs::canonicalize(REPO)?;
    let go = go_version(&repo)?;
    let source = dag.host().directory_opts(
        repo.to_string_lossy().into_owned(),
        &HostDirectoryOpts::default().with_exclude(EXCLUDES.to_vec()),
    );
    let endpoint = dag.set_secret("temporal-dsql-ci-endpoint", endpoint(run));
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
    print_step(&tests, masker).await?;

    // The test clusters' reset deletes rows, so it runs after the tests that use public.
    println!("--- checking the test clusters' schema reset");
    let reset = tests
        .with_env_variable("DSQL_TEST_SCHEMAS", "public")
        .with_exec(vec![
            "go",
            "test",
            "-count=1",
            "-tags",
            "integration,disable_grpc_modules,test_dep",
            "-run",
            "^TestDSQLTestClusterResetIntegration$",
            "./common/persistence/persistence-tests/",
        ]);
    print_step(&reset, masker).await?;
    if !corpus {
        return Ok(());
    }

    // DSQL can fail a statement with OC001 while another session builds an index, so the
    // schemas are set up one at a time.
    let mut schemas = reset;
    for name in corpus_schemas() {
        println!("--- setting up the persistence schema {name}");
        schemas = schemas.with_exec(vec![
            "go",
            "run",
            "./cmd/tools/dsql",
            "--search-path",
            &name,
            "setup-schema",
            "--schema-name",
            "dsql/temporal",
            "--version",
            "1.3",
        ]);
        print_step(&schemas, masker).await?;
    }
    Ok(())
}

/// Runs Temporal's functional tests on this machine against the run's cluster. Their
/// output, masked, goes to target/corpus/<run>: results.jsonl from test2json,
/// clusters.jsonl with each test cluster created, and summary.txt.
async fn functional_tests(run: &Run, masker: &Masker) -> Result<()> {
    let repo = std::fs::canonicalize(REPO)?;
    let out = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/corpus")
        .join(&run.id);
    std::fs::create_dir_all(&out)?;
    let binary = out.join("functional.test");

    println!("--- building the functional tests");
    let built = Command::new("go")
        .args(["test", "-c", "-tags", GO_TEST_TAGS, "-o"])
        .arg(&binary)
        .arg("./tests")
        .current_dir(&repo)
        .status()
        .await?;
    if !built.success() {
        return Err("couldn't build the functional tests".into());
    }

    println!(
        "--- running the functional tests, with their output in {}",
        out.display()
    );
    let schemas: Vec<String> = std::iter::once("public".to_owned())
        .chain(corpus_schemas())
        .collect();
    let mut command = Command::new(&binary);
    // `dagger run` sets OpenTelemetry variables for its own traces, and the servers'
    // span exporters would read them.
    for (name, _) in std::env::vars_os() {
        if name.to_str().is_some_and(is_telemetry_variable) {
            command.env_remove(name);
        }
    }
    let mut tests = command
        .args([
            "-test.v=test2json",
            "-test.count=1",
            &format!("-test.timeout={CORPUS_TIMEOUT}"),
            "-persistenceType=sql",
            "-persistenceDriver=dsql",
        ])
        .current_dir(repo.join("tests"))
        .env("DSQL_TEST_ENDPOINT", endpoint(run))
        .env("DSQL_TEST_SCHEMAS", schemas.join(","))
        .env("AWS_REGION", &run.region)
        // A pooled test cluster keeps its schema until the tests end, so the pools stay
        // small enough to leave schemas for the clusters that tests create and remove.
        .env("TEMPORAL_TEST_SHARED_CLUSTERS", "1")
        .env("TEMPORAL_TEST_DEDICATED_CLUSTERS", "3")
        .env("TEMPORAL_TEST_LOG_LEVEL", "warn")
        .env(
            "TEMPORAL_TEST_CLUSTER_EVENTS_FILE",
            out.join("clusters.jsonl"),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let verbose: Stdio = tests
        .stdout
        .take()
        .ok_or("the tests have no output")?
        .try_into()?;
    let mut test2json = Command::new("go")
        .args([
            "tool",
            "test2json",
            "-t",
            "-p",
            "go.temporal.io/server/tests",
        ])
        .current_dir(&repo)
        .stdin(verbose)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stderr = tests
        .stderr
        .take()
        .ok_or("the tests have no error output")?;
    let errors = tokio::spawn(write_masked(stderr, out.join("stderr.log"), masker.clone()));

    let mut results = std::io::BufWriter::new(std::fs::File::create(out.join("results.jsonl"))?);
    let mut tally = Tally::default();
    let events = test2json.stdout.take().ok_or("test2json has no output")?;
    let mut lines = BufReader::new(events).lines();
    while let Some(line) = lines.next_line().await? {
        let line = masker.mask(&line);
        writeln!(results, "{line}")?;
        if let Some(result) = tally.add(&line) {
            println!("{result}");
        }
    }
    results.flush()?;
    let status = tests.wait().await?;
    test2json.wait().await?;
    errors.await??;
    std::fs::remove_file(&binary)?;

    let summary = tally.summary(status);
    println!("{summary}");
    std::fs::write(out.join("summary.txt"), &summary)?;
    if tally.passed + tally.failed + tally.skipped == 0 {
        return Err("no functional test ran".into());
    }
    Ok(())
}

/// Writes what a process prints, masked, to a file.
async fn write_masked(output: impl AsyncRead + Unpin, path: PathBuf, masker: Masker) -> Result<()> {
    let mut file = std::io::BufWriter::new(std::fs::File::create(path)?);
    let mut lines = BufReader::new(output).lines();
    while let Some(line) = lines.next_line().await? {
        writeln!(file, "{}", masker.mask(&line))?;
    }
    file.flush()?;
    Ok(())
}

/// Counts the top-level tests' results in test2json's events.
#[derive(Default)]
struct Tally {
    passed: usize,
    failed: usize,
    skipped: usize,
    failures: Vec<String>,
    /// Top-level tests that started and have no result: the test binary stopped before
    /// they ended, or their result line ran into a server's log output, which test2json
    /// then reads as output.
    unfinished: BTreeSet<String>,
    /// Set when the test binary itself failed, such as on its timeout.
    binary_failed: bool,
}

impl Tally {
    /// Counts an event that starts or ends a top-level test, and returns a line about
    /// each one that ends.
    fn add(&mut self, event: &str) -> Option<String> {
        let action = json_field(event, "Action")?;
        let Some(test) = json_field(event, "Test") else {
            self.binary_failed |= action == "fail";
            return None;
        };
        if test.contains('/') {
            return None;
        }
        match action {
            "run" => {
                self.unfinished.insert(test.to_owned());
                return None;
            }
            "pass" => self.passed += 1,
            "fail" => {
                self.failed += 1;
                self.failures.push(test.to_owned());
            }
            "skip" => self.skipped += 1,
            _ => return None,
        }
        self.unfinished.remove(test);
        let elapsed = json_field(event, "Elapsed").unwrap_or("?");
        Some(format!("{action} {test} ({elapsed}s)"))
    }

    fn summary(&self, status: ExitStatus) -> String {
        let unfinished = match self.unfinished.len() {
            0 => String::new(),
            n => format!(", {n} without a result"),
        };
        let mut summary = format!(
            "--- functional tests: {} passed, {} failed, {} skipped{unfinished}; the test binary exited with {status}",
            self.passed, self.failed, self.skipped
        );
        if self.binary_failed {
            summary.push_str(", reporting a failure of its own");
        }
        for test in &self.failures {
            summary.push_str("\nfailed: ");
            summary.push_str(test);
        }
        for test in &self.unfinished {
            summary.push_str("\nno result: ");
            summary.push_str(test);
        }
        summary.push('\n');
        summary
    }
}

/// Reads one field of a test2json event, whose fields are strings without escapes or
/// plain numbers.
fn json_field<'a>(event: &'a str, name: &str) -> Option<&'a str> {
    let key = format!("\"{name}\":");
    let rest = &event[event.find(&key)? + key.len()..];
    match rest.strip_prefix('"') {
        Some(quoted) => quoted.find('"').map(|end| &quoted[..end]),
        None => rest.find([',', '}']).map(|end| &rest[..end]),
    }
}

fn is_telemetry_variable(name: &str) -> bool {
    name.starts_with("OTEL_") || name == "TRACEPARENT" || name == "TRACESTATE"
}

fn corpus_schemas() -> impl Iterator<Item = String> {
    (1..=CORPUS_SCHEMAS).map(|n| format!("corpus_{n}"))
}

fn endpoint(run: &Run) -> String {
    format!("{}.dsql.{}.on.aws", run.cluster, run.region)
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
#[derive(Clone)]
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

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt as _;

    use super::*;

    #[test]
    fn reads_test2json_fields() {
        let event = r#"{"Time":"2026-11-12T18:08:21Z","Action":"pass","Package":"go.temporal.io/server/tests","Test":"TestSignal/WithStart","Elapsed":1.25}"#;
        assert_eq!(json_field(event, "Action"), Some("pass"));
        assert_eq!(json_field(event, "Test"), Some("TestSignal/WithStart"));
        assert_eq!(json_field(event, "Elapsed"), Some("1.25"));
        assert_eq!(json_field(event, "Output"), None);
    }

    #[test]
    fn tallies_top_level_tests() {
        let mut tally = Tally::default();
        let event = |action: &str, test: &str| {
            format!(r#"{{"Action":"{action}","Package":"p","Test":"{test}","Elapsed":2}}"#)
        };
        assert_eq!(tally.add(&event("run", "TestA")), None);
        assert_eq!(
            tally.add(&event("pass", "TestA")),
            Some("pass TestA (2s)".to_owned())
        );
        assert_eq!(tally.add(&event("fail", "TestB/sub")), None);
        assert_eq!(
            tally.add(&event("fail", "TestB")),
            Some("fail TestB (2s)".to_owned())
        );
        assert_eq!(
            tally.add(&event("skip", "TestC")),
            Some("skip TestC (2s)".to_owned())
        );
        assert_eq!(tally.add(&event("run", "TestD")), None);
        assert_eq!(tally.add(r#"{"Action":"output","Output":"ok\n"}"#), None);
        assert!(!tally.binary_failed);
        assert_eq!(
            tally.add(r#"{"Action":"fail","Package":"p","Elapsed":9}"#),
            None
        );
        assert!(tally.binary_failed);

        let summary = tally.summary(ExitStatus::from_raw(256));
        assert!(summary.starts_with(
            "--- functional tests: 1 passed, 1 failed, 1 skipped, 1 without a result;"
        ));
        assert!(summary.contains("reporting a failure of its own"));
        assert!(summary.ends_with("\nfailed: TestB\nno result: TestD\n"));
    }

    #[test]
    fn names_the_corpus_schemas() {
        let schemas: Vec<String> = corpus_schemas().collect();
        assert_eq!(schemas.len(), CORPUS_SCHEMAS);
        assert_eq!(schemas.first().map(String::as_str), Some("corpus_1"));
        assert_eq!(schemas.last().map(String::as_str), Some("corpus_8"));
    }

    #[test]
    fn finds_telemetry_variables() {
        assert!(is_telemetry_variable("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"));
        assert!(is_telemetry_variable("TRACEPARENT"));
        assert!(!is_telemetry_variable("AWS_PROFILE"));
        assert!(!is_telemetry_variable("DSQL_TEST_ENDPOINT"));
    }
}
