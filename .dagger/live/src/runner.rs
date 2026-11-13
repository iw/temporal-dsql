//! Runs the functional tests on a Graviton instance in the cluster's region, for
//! `--corpus --instance <type>`.
//!
//! The tests run in shards side by side, as upstream's CI runs them on separate machines:
//! each shard is a test process that runs the tests whose names hash to it, against a
//! DSQL cluster of its own. A test process whose tests stop it, such as by dumping every
//! goroutine's stack, then stops only its own shard's tests.

use std::fmt::Write as _;
use std::path::Path;
use std::time::{Duration, Instant};

use aws_sdk_ec2::error::ProvideErrorMetadata as _;
use aws_sdk_ec2::types::{
    Filter, HttpTokensState, IamInstanceProfileSpecification, InstanceMetadataEndpointState,
    InstanceMetadataOptionsRequest, InstanceStateName, InstanceType, ResourceType,
    ShutdownBehavior, TagSpecification,
};
use aws_sdk_s3::primitives::ByteStream;
use tokio::process::Command;

use crate::results::{Attempt, Pauses, Rerun, Results, plural};
use crate::{
    AWAIT_ATTEMPT_TIMEOUT, Aws, GO_TEST_TAGS, Masker, REPO, Result, Run, TAG, cluster_env,
    corpus_dir, describe, failed, summarize, write_masked,
};

/// The runner instances' image: the latest Amazon Linux 2023 for arm64, which comes with
/// the AWS CLI.
const IMAGE: &str =
    "resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64";
/// The instance profile and the security group of a runner instance. The profile's role
/// connects to CI clusters and reads and writes run files, and the group allows no inbound
/// traffic.
const NAME: &str = "temporal-dsql-ci-runner";
/// How many shards a run has unless `--shards` says otherwise. Upstream's CI runs the
/// functional tests in five shards on machines with four CPUs.
pub(crate) const DEFAULT_SHARDS: u16 = 4;
/// The most shards a run may have, each with a cluster of its own.
pub(crate) const MAX_SHARDS: u16 = 8;
/// How long each of a runner instance's test processes may take.
pub(crate) const TIMEOUT: &str = "90m";
/// How long a runner instance lives at most: three test processes' timeouts one after the
/// other, as the first shard's lane runs them, the wait for this machine to say which
/// tests run again, and an hour to fetch the tests and upload their output. It then shuts
/// down, which terminates it.
const LIFETIME_MINUTES: u64 = 6 * 60;
/// How often a runner instance reports, and this machine checks on it.
const POLL: Duration = Duration::from_secs(60);
/// How many polls a shard's lane waits for this machine to say which of its tests run
/// again, before it ends without running them.
const PLAN_POLLS: u32 = 15;
/// Tests that run in a test process of their own, after the first shard's. Their
/// workflows block under a short deadlock detection timeout, on purpose, and the Go SDK
/// dumps every goroutine's stack at each detection, which stops the whole process while
/// it runs. Go splits the pattern at its slash, so it names the suite and its two tests.
const ALONE: &str =
    "^TestVersioningFunctionalSuite$/^TestWorkflowTaskRedirectInRetry(First|NonFirst)Task$";
/// A shard's shared and dedicated test clusters. With nine persistence schemas, they leave
/// four places for suites that share one cluster, and one schema that a dedicated slot's
/// fresh cluster can always lease.
const SHARED_CLUSTERS: u16 = 2;
const DEDICATED_CLUSTERS: u16 = 2;
/// How long a runner instance has to make its first report.
const FIRST_REPORT: Duration = Duration::from_secs(15 * 60);
/// How many checks on a runner instance can fail in a row, such as while this machine is
/// offline, before the run gives up on it.
const CHECK_FAILURES: u32 = 15;
/// The exit code a runner instance reports when it can't fetch the tests.
const FETCH_FAILED: i32 = 125;
/// What this machine builds for a runner instance.
const BINARIES: &[&str] = &["functional.test", "test2json"];
/// What a runner instance writes for each test process, and whether each file is text for
/// the masker.
const OUTPUT: &[(&str, bool)] = &[
    ("results.jsonl", true),
    ("clusters.jsonl", true),
    ("pools.jsonl", true),
    ("stderr.log", true),
    ("cpu.pprof", false),
];
/// A runner instance's boot script, which runs as root. It fetches the tests, then starts
/// a lane for each shard. A lane runs the shard's test process with its output in
/// out/shard-<n>; the first lane then runs the tests in ALONE in a process of their own,
/// in out/alone. Each lane then waits for this machine to name the shard's failed tests in
/// plan/<n>, and runs them again in a process of their own, in out/shard-<n>-rerun. A test
/// process uploads its output once it ends, then its exit code as out/<dir>.code. Each
/// poll, the instance uploads each shard's top-level results so far, and if the run's
/// `stop` file has appeared, it sends the test processes SIGQUIT, which ends them with
/// their goroutines' stacks. Once every lane has ended, it uploads its own exit code, and
/// shuts down.
const SCRIPT: &str = r#"#!/bin/bash
shutdown -h +{minutes}
export HOME=/root AWS_REGION={region} AWS_DEFAULT_REGION={region}
run=s3://{bucket}/runs/{id}
mkdir -p /var/tmp/ci/out && cd /var/tmp/ci || exit 1
finish() {
  echo "$1" > exit-code && aws s3 cp --quiet exit-code "$run/exit-code"
  shutdown -h now
  exit
}
aws s3 cp --quiet "$run/bin/functional.test" . && aws s3 cp --quiet "$run/bin/test2json" . &&
  chmod +x functional.test test2json || finish {fetch_failed}
{exports}export TEMPORAL_ROOT=/var/tmp/ci
attempt() {
  local dir=out/$1
  shift
  mkdir -p "$dir"
  TEMPORAL_TEST_CLUSTER_EVENTS_FILE="$PWD/$dir/clusters.jsonl" DSQL_TEST_POOL_METRICS_FILE="$PWD/$dir/pools.jsonl" \
    ./functional.test {flags} "$@" -test.cpuprofile="$dir/cpu.pprof" 2> "$dir/stderr.log" |
    ./test2json -t -p go.temporal.io/server/tests > "$dir/results.jsonl"
  local code=${PIPESTATUS[0]}
  grep -E '"Action":"fail","Package":"[^"]*","Test":"' "$dir/results.jsonl" > "$dir/failed.jsonl"
  aws s3 cp --quiet --recursive "$dir" "$run/$dir"
  echo "$code" > "$dir.code" && aws s3 cp --quiet "$dir.code" "$run/$dir.code"
}
rerun() {
  for _ in $(seq {plan_polls}); do
    if aws s3 cp --quiet "$run/plan/$1" "plan-$1" 2> /dev/null; then
      if [ -s "plan-$1" ]; then attempt "shard-$1-rerun" "-test.run=$(cat "plan-$1")"; fi
      return
    fi
    sleep {poll}
  done
}
lanes=
{lanes}while sleep {poll}; do
  for shard in {shards}; do
    grep -E '"Action":"(pass|fail|skip)","Package":"[^"]*","Test":"[^"/]*",' "out/shard-$shard/results.jsonl" > "progress-$shard.jsonl" 2> /dev/null
    aws s3 cp --quiet "progress-$shard.jsonl" "$run/progress/$shard.jsonl"
  done
  aws s3api head-object --bucket {bucket} --key runs/{id}/stop > /dev/null 2>&1 && pkill -QUIT -x functional.test
done &
wait $lanes
finish 0
"#;

/// How the functional tests run on an instance.
#[derive(Debug, PartialEq)]
pub(crate) struct Runner {
    /// The instance type.
    pub(crate) kind: String,
    /// How many test processes run side by side.
    pub(crate) shards: u16,
    /// Whether the tests run on SQLite, for a baseline, rather than on DSQL.
    pub(crate) sqlite: bool,
}

impl Runner {
    pub(crate) fn new(
        kind: String,
        shards: u16,
        sqlite: bool,
    ) -> std::result::Result<Self, String> {
        goarm64(&kind)?;
        if !(1..=MAX_SHARDS).contains(&shards) {
            return Err(format!("a run has from 1 to {MAX_SHARDS} shards"));
        }
        let cpus = vcpus(&kind)?;
        if shards > cpus {
            return Err(format!(
                "a {kind} has {}, too few for {}",
                plural(usize::from(cpus), "vCPU"),
                plural(usize::from(shards), "shard")
            ));
        }
        Ok(Self {
            kind,
            shards,
            sqlite,
        })
    }

    /// The tests' persistence driver.
    pub(crate) fn driver(&self) -> &'static str {
        if self.sqlite { "sqlite" } else { "dsql" }
    }

    /// How many clusters the run needs: one for each shard, or none on SQLite.
    pub(crate) fn clusters(&self) -> usize {
        if self.sqlite {
            0
        } else {
            usize::from(self.shards)
        }
    }

    /// Each test process's share of the instance's CPUs, for GOMAXPROCS, which also sets
    /// how many tests it runs at once unless the run sets -test.parallel.
    fn procs(&self) -> u16 {
        vcpus(&self.kind).map_or(1, |cpus| (cpus / self.shards).max(1))
    }
}

/// Builds the functional tests and test2json for a runner instance: static linux/arm64
/// binaries for its processor, without the symbol tables and debug information that the
/// run doesn't use.
pub(crate) async fn build(out: &Path, kind: &str) -> Result<()> {
    let repo = std::fs::canonicalize(REPO)?;
    let level = goarm64(kind)?;
    std::fs::create_dir_all(out)?;
    println!("--- building the functional tests for a {kind}, with GOARM64={level}");
    let go = || {
        let mut command = Command::new("go");
        command
            .current_dir(&repo)
            .env("GOOS", "linux")
            .env("GOARCH", "arm64")
            .env("GOARM64", level)
            .env("CGO_ENABLED", "0");
        command
    };
    let built = go()
        .args([
            "test",
            "-c",
            "-trimpath",
            "-ldflags=-s -w",
            "-tags",
            GO_TEST_TAGS,
            "-o",
        ])
        .arg(out.join("functional.test"))
        .arg("./tests")
        .status()
        .await?;
    if !built.success() {
        return Err("couldn't build the functional tests".into());
    }
    let built = go()
        .args(["build", "-trimpath", "-ldflags=-s -w", "-o"])
        .arg(out.join("test2json"))
        .arg("cmd/test2json")
        .status()
        .await?;
    if !built.success() {
        return Err("couldn't build test2json".into());
    }
    Ok(())
}

/// How many vCPUs a Graviton instance type has, from its size. Each is a whole core.
pub(crate) fn vcpus(kind: &str) -> std::result::Result<u16, String> {
    let size = kind.split_once('.').map_or("", |(_, size)| size);
    let cpus = match size {
        "medium" => Some(1),
        "large" => Some(2),
        "xlarge" => Some(4),
        _ => size
            .strip_suffix("xlarge")
            .and_then(|count| count.parse::<u16>().ok())
            .and_then(|count| count.checked_mul(4)),
    };
    cpus.filter(|&cpus| cpus > 0).ok_or_else(|| {
        format!("can't tell how many vCPUs a {kind} has; use a size such as 4xlarge")
    })
}

/// The Arm architecture version of a Graviton instance type's processor, for GOARM64. The
/// 8g families' Graviton4 implements Armv9.0, the 7g families' Graviton3 Armv8.4, and the
/// 6g families' Graviton2 Armv8.2.
pub(crate) fn goarm64(kind: &str) -> std::result::Result<&'static str, String> {
    let family = kind.split_once('.').map_or("", |(family, _)| family);
    let generation = family
        .find(|c: char| c.is_ascii_digit())
        .map_or("", |at| &family[at..]);
    if generation.starts_with("8g") {
        Ok("v9.0")
    } else if generation.starts_with("7g") {
        Ok("v8.4")
    } else if generation.starts_with("6g") {
        Ok("v8.2")
    } else {
        Err(format!(
            "{kind} isn't a Graviton2, 3 or 4 instance type, such as c8g.4xlarge"
        ))
    }
}

/// Runs Temporal's functional tests on a runner instance in the cluster's region. It then
/// writes each test process's output, masked, to its own directory in
/// target/corpus/<run>, as `crate::functional_tests` does, with its CPU profile as
/// cpu.pprof, and summarizes them all.
pub(crate) async fn functional_tests(
    aws: &Aws,
    run: &Run,
    masker: &Masker,
    runner: &Runner,
) -> Result<()> {
    let out = corpus_dir(&run.id);
    let prefix = format!("runs/{}", run.id);
    println!("--- uploading the functional tests to the CI bucket");
    for name in BINARIES {
        let path = out.join(name);
        aws.s3
            .put_object()
            .bucket(&run.bucket)
            .key(format!("{prefix}/bin/{name}"))
            .body(ByteStream::from_path(&path).await?)
            .send()
            .await
            .map_err(failed("upload the tests"))?;
        std::fs::remove_file(path)?;
    }
    let (instance, zone) = launch(aws, run, runner).await?;
    println!(
        "--- running the functional tests in {}{} on a {} in {zone}",
        plural(usize::from(runner.shards), "shard"),
        if runner.sqlite { ", on SQLite," } else { "" },
        runner.kind,
    );
    let (code, plans) = wait(aws, run, masker, &instance, runner.shards).await?;
    if code == FETCH_FAILED {
        return Err("the runner instance couldn't fetch the tests".into());
    }

    println!(
        "--- fetching the functional tests' output, to {}",
        out.display()
    );
    let mut attempts = Vec::new();
    for (shard, plan) in (1..=runner.shards).zip(plans) {
        let mut attempt = fetch(
            aws,
            run,
            masker,
            &format!("shard-{shard}"),
            format!("shard {shard}"),
        )
        .await?;
        attempt.reruns = true;
        if matches!(plan, Some(Rerun::Tests(_))) {
            let again = fetch(
                aws,
                run,
                masker,
                &format!("shard-{shard}-rerun"),
                format!("shard {shard}, run again"),
            )
            .await?;
            attempt.again = Some(Box::new(again));
        }
        attempts.push(attempt);
    }
    attempts.push(fetch(aws, run, masker, "alone", "alone".to_owned()).await?);
    summarize(&out, &attempts)
}

/// Fetches what one of the runner instance's test processes wrote to out/<dir>, masked,
/// into the same directory in target/corpus/<run>, and reads its results.
async fn fetch(aws: &Aws, run: &Run, masker: &Masker, dir: &str, name: String) -> Result<Attempt> {
    let local = corpus_dir(&run.id).join(dir);
    std::fs::create_dir_all(&local)?;
    for &(file, text) in OUTPUT {
        let fetched = aws
            .s3
            .get_object()
            .bucket(&run.bucket)
            .key(format!("runs/{}/out/{dir}/{file}", run.id))
            .send()
            .await;
        let object = match fetched {
            Ok(object) => object,
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|err| err.is_no_such_key()) =>
            {
                println!("the runner instance wrote no {dir}/{file}");
                continue;
            }
            Err(err) => return Err(describe("fetch the tests' output", &err).into()),
        };
        if text {
            let body = Box::pin(object.body.into_async_read());
            write_masked(body, local.join(file), masker.clone()).await?;
        } else {
            std::fs::write(local.join(file), object.body.collect().await?.into_bytes())?;
        }
    }
    let code = read_object(
        &aws.s3,
        &run.bucket,
        &format!("runs/{}/out/{dir}.code", run.id),
    )
    .await?;
    let read = |file: &str| std::fs::read_to_string(local.join(file)).unwrap_or_default();
    Ok(Attempt {
        name,
        results: Results::read(&read("results.jsonl")),
        pauses: Pauses::read(&read("pools.jsonl")),
        exit: code.map_or_else(
            || "a status it didn't report".to_owned(),
            |code| format!("exit status: {}", code.trim()),
        ),
        reruns: false,
        again: None,
    })
}

/// Starts a runner instance in the first CI subnet whose zone has capacity for it, and
/// returns its ID and zone.
async fn launch(aws: &Aws, run: &Run, runner: &Runner) -> Result<(String, String)> {
    let kind = runner.kind.as_str();
    let tagged = || Filter::builder().name("tag:purpose").values(TAG).build();
    let found = aws
        .ec2
        .describe_subnets()
        .filters(tagged())
        .send()
        .await
        .map_err(failed("find the CI subnets"))?;
    let mut subnets: Vec<(&str, &str)> = found
        .subnets()
        .iter()
        .filter_map(|subnet| Some((subnet.availability_zone()?, subnet.subnet_id()?)))
        .collect();
    subnets.sort_unstable();
    let groups = aws
        .ec2
        .describe_security_groups()
        .filters(tagged())
        .filters(Filter::builder().name("group-name").values(NAME).build())
        .send()
        .await
        .map_err(failed("find the runner security group"))?;
    let group = groups
        .security_groups()
        .first()
        .and_then(|group| group.group_id())
        .ok_or("no runner security group; docs/dsql/ci.md describes the CI network")?;
    if subnets.is_empty() {
        return Err("no CI subnets; docs/dsql/ci.md describes the CI network".into());
    }

    let script = aws_smithy_types::base64::encode(script(run, runner));
    let mut full = Vec::new();
    for (zone, subnet) in subnets {
        let launched = aws
            .ec2
            .run_instances()
            .image_id(IMAGE)
            .instance_type(InstanceType::from(kind))
            .min_count(1)
            .max_count(1)
            .subnet_id(subnet)
            .security_group_ids(group)
            .iam_instance_profile(
                IamInstanceProfileSpecification::builder()
                    .name(NAME)
                    .build(),
            )
            .metadata_options(
                InstanceMetadataOptionsRequest::builder()
                    .http_endpoint(InstanceMetadataEndpointState::Enabled)
                    .http_tokens(HttpTokensState::Required)
                    .build(),
            )
            .instance_initiated_shutdown_behavior(ShutdownBehavior::Terminate)
            .user_data(&script)
            .tag_specifications(launch_tags(ResourceType::Instance, run))
            .tag_specifications(launch_tags(ResourceType::Volume, run))
            .send()
            .await;
        match launched {
            Ok(output) => {
                let instance = output
                    .instances()
                    .first()
                    .and_then(|instance| instance.instance_id())
                    .ok_or("EC2 started no instance")?;
                return Ok((instance.to_owned(), zone.to_owned()));
            }
            // The zone has no capacity for the type, or doesn't offer it.
            Err(err)
                if matches!(
                    err.code(),
                    Some("InsufficientInstanceCapacity" | "Unsupported")
                ) =>
            {
                println!("no {kind} capacity in {zone}");
                full.push(zone);
            }
            Err(err) => {
                return Err(describe("start the runner instance", &err)
                    .replace(subnet, "<subnet>")
                    .replace(group, "<security-group>")
                    .into());
            }
        }
    }
    Err(format!("no {kind} capacity in {}", full.join(", ")).into())
}

/// Tags a resource that a runner instance's launch creates.
fn launch_tags(kind: ResourceType, run: &Run) -> TagSpecification {
    let tag = |key: &str, value: &str| {
        aws_sdk_ec2::types::Tag::builder()
            .key(key)
            .value(value)
            .build()
    };
    TagSpecification::builder()
        .resource_type(kind)
        .tags(tag("purpose", TAG))
        .tags(tag("run", &run.id))
        .tags(tag("Name", &format!("{TAG}-{}", run.id)))
        .build()
}

/// A runner instance's boot script for a run.
fn script(run: &Run, runner: &Runner) -> String {
    // Every test process gets these, and each lane adds its shard's cluster and index.
    let exports: String = [
        ("GOMAXPROCS", runner.procs().to_string()),
        ("TEMPORAL_TEST_SHARED_CLUSTERS", SHARED_CLUSTERS.to_string()),
        (
            "TEMPORAL_TEST_DEDICATED_CLUSTERS",
            DEDICATED_CLUSTERS.to_string(),
        ),
        ("TEMPORAL_TEST_LOG_LEVEL", "warn".to_owned()),
        (
            "TEMPORAL_AWAIT_ATTEMPT_TIMEOUT",
            AWAIT_ATTEMPT_TIMEOUT.to_owned(),
        ),
        ("TEST_TOTAL_SHARDS", runner.shards.to_string()),
    ]
    .into_iter()
    .map(|(name, value)| format!("export {name}='{value}'\n"))
    .collect();
    let mut lanes = String::new();
    for shard in 1..=runner.shards {
        let index = usize::from(shard - 1);
        let mut env = run
            .clusters
            .get(index)
            .map(|cluster| cluster_env(&run.region, cluster))
            .unwrap_or_default();
        env.push(("TEST_SHARD_INDEX", index.to_string()));
        lanes.push_str("(\n");
        for (name, value) in env {
            let _ = writeln!(lanes, "  export {name}='{value}'");
        }
        let _ = writeln!(lanes, "  attempt shard-{shard} '-test.skip={ALONE}'");
        if shard == 1 {
            // Without the shard variables, the process runs every test that -test.run names.
            let _ = writeln!(
                lanes,
                "  (unset TEST_TOTAL_SHARDS TEST_SHARD_INDEX && attempt alone '-test.run={ALONE}')"
            );
        }
        let _ = writeln!(lanes, "  rerun {shard}\n) &\nlanes=\"$lanes $!\"");
    }
    let shards: Vec<String> = (1..=runner.shards).map(|shard| shard.to_string()).collect();
    SCRIPT
        .replace("{exports}", &exports)
        .replace("{lanes}", &lanes)
        .replace("{shards}", &shards.join(" "))
        .replace("{flags}", &run.test_flags.join(" "))
        .replace("{minutes}", &LIFETIME_MINUTES.to_string())
        .replace("{poll}", &POLL.as_secs().to_string())
        .replace("{plan_polls}", &PLAN_POLLS.to_string())
        .replace("{fetch_failed}", &FETCH_FAILED.to_string())
        .replace("{bucket}", &run.bucket)
        .replace("{region}", &run.region)
        .replace("{id}", &run.id)
}

/// What a check on a runner instance found.
enum Check {
    Running,
    Exited(i32),
    Stopped,
}

/// Waits for the runner instance to report its exit code, and returns it, with what each
/// shard ran again, once known. It prints each top-level test's result as the instance
/// reports it, and tells the instance what each shard runs again once its first test
/// process has ended.
async fn wait(
    aws: &Aws,
    run: &Run,
    masker: &Masker,
    instance: &str,
    shards: u16,
) -> Result<(i32, Vec<Option<Rerun>>)> {
    let started = Instant::now();
    let limit = Duration::from_secs(LIFETIME_MINUTES * 60) + FIRST_REPORT;
    let exit = format!("runs/{}/exit-code", run.id);
    let shards = usize::from(shards);
    let mut progress: Vec<Results> = (0..shards).map(|_| Results::default()).collect();
    // Each shard's report's lines printed so far, once it has a report.
    let mut reported: Vec<Option<usize>> = vec![None; shards];
    let mut plans: Vec<Option<Rerun>> = (0..shards).map(|_| None).collect();
    let mut failures = 0;
    loop {
        tokio::time::sleep(POLL).await;
        let checked: Result<Check> = async {
            for shard in 1..=shards {
                let key = format!("runs/{}/progress/{shard}.jsonl", run.id);
                if let Some(report) = read_object(&aws.s3, &run.bucket, &key).await? {
                    let at = reported[shard - 1].unwrap_or(0);
                    for line in report.lines().skip(at) {
                        if let Some(result) = progress[shard - 1].add(&masker.mask(line)) {
                            println!("shard {shard}: {result}");
                        }
                    }
                    reported[shard - 1] = Some(report.lines().count());
                }
                if plans[shard - 1].is_none() {
                    plans[shard - 1] = plan(aws, run, shard).await?;
                }
            }
            if let Some(code) = read_object(&aws.s3, &run.bucket, &exit).await? {
                return Ok(Check::Exited(exit_code(&code)?));
            }
            if is_up(aws, instance).await? {
                return Ok(Check::Running);
            }
            // It may have reported the exit code just before it stopped.
            match read_object(&aws.s3, &run.bucket, &exit).await? {
                Some(code) => Ok(Check::Exited(exit_code(&code)?)),
                None => Ok(Check::Stopped),
            }
        }
        .await;
        match checked {
            Ok(Check::Exited(code)) => return Ok((code, plans)),
            Ok(Check::Stopped) => {
                return Err(
                    "the runner instance stopped without reporting the tests' exit code".into(),
                );
            }
            Ok(Check::Running) => failures = 0,
            Err(err) => {
                failures += 1;
                println!(
                    "couldn't check on the runner instance: {}",
                    masker
                        .mask(&err.to_string())
                        .replace(instance, "<instance>")
                );
                if failures >= CHECK_FAILURES {
                    return Err("lost touch with the runner instance".into());
                }
            }
        }
        if reported.iter().all(Option::is_none) && started.elapsed() > FIRST_REPORT {
            return Err("the runner instance didn't report in its first 15 minutes".into());
        }
        if started.elapsed() > limit {
            return Err("the runner instance outlived the tests' timeout".into());
        }
    }
}

/// Once a shard's first test process has ended, decides which of its failed tests run
/// again, and tells the runner instance in plan/<shard>, which is empty when none do. It
/// returns None while the process runs.
async fn plan(aws: &Aws, run: &Run, shard: usize) -> Result<Option<Rerun>> {
    let ended = format!("runs/{}/out/shard-{shard}.code", run.id);
    let Some(code) = read_object(&aws.s3, &run.bucket, &ended).await? else {
        return Ok(None);
    };
    let fails = format!("runs/{}/out/shard-{shard}/failed.jsonl", run.id);
    let fails = read_object(&aws.s3, &run.bucket, &fails)
        .await?
        .unwrap_or_default();
    let rerun = Rerun::plan(&fails);
    aws.s3
        .put_object()
        .bucket(&run.bucket)
        .key(format!("runs/{}/plan/{shard}", run.id))
        .body(ByteStream::from(rerun.pattern().into_bytes()))
        .send()
        .await
        .map_err(failed("tell the runner instance what to run again"))?;
    println!(
        "--- shard {shard}'s test process exited with exit status {}; {rerun}",
        code.trim()
    );
    Ok(Some(rerun))
}

fn exit_code(text: &str) -> Result<i32> {
    text.trim()
        .parse()
        .map_err(|_| format!("the runner instance reported an exit code of {text:?}").into())
}

/// Reads a small run file as text, or None while it doesn't exist.
async fn read_object(s3: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Result<Option<String>> {
    match s3.get_object().bucket(bucket).key(key).send().await {
        Ok(object) => {
            let bytes = object.body.collect().await?.into_bytes();
            Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
        }
        Err(err)
            if err
                .as_service_error()
                .is_some_and(|err| err.is_no_such_key()) =>
        {
            Ok(None)
        }
        Err(err) => Err(describe("read a run file", &err).into()),
    }
}

/// Whether a runner instance is still up: not shutting down, stopped or gone.
async fn is_up(aws: &Aws, instance: &str) -> Result<bool> {
    let output = aws
        .ec2
        .describe_instances()
        .instance_ids(instance)
        .send()
        .await
        .map_err(failed("check on the runner instance"))?;
    let state = output
        .reservations()
        .iter()
        .flat_map(|reservation| reservation.instances())
        .find_map(|instance| instance.state()?.name().cloned());
    Ok(matches!(
        state,
        Some(InstanceStateName::Pending | InstanceStateName::Running)
    ))
}

/// The runner instances that are up, or one run's, with their launch times.
async fn runners(aws: &Aws, run: Option<&str>) -> Result<Vec<(String, i64)>> {
    let mut request = aws
        .ec2
        .describe_instances()
        .filters(Filter::builder().name("tag:purpose").values(TAG).build())
        .filters(
            Filter::builder()
                .name("instance-state-name")
                .values("pending")
                .values("running")
                .values("stopping")
                .values("stopped")
                .build(),
        );
    if let Some(id) = run {
        request = request.filters(Filter::builder().name("tag:run").values(id).build());
    }
    let mut pages = request.into_paginator().send();
    let mut instances = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.map_err(failed("list the runner instances"))?;
        for instance in page
            .reservations()
            .iter()
            .flat_map(|reservation| reservation.instances())
        {
            if let Some(id) = instance.instance_id() {
                let launched = instance.launch_time().map_or(0, |time| time.secs());
                instances.push((id.to_owned(), launched));
            }
        }
    }
    Ok(instances)
}

/// Terminates the run's runner instance, if it's still up, and deletes the run's files.
pub(crate) async fn end(aws: &Aws, run: &Run) -> Result<()> {
    let instances: Vec<String> = runners(aws, Some(&run.id))
        .await?
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    if !instances.is_empty() {
        aws.ec2
            .terminate_instances()
            .set_instance_ids(Some(instances))
            .send()
            .await
            .map_err(failed("end the runner instance"))?;
    }
    let mut pages = aws
        .s3
        .list_objects_v2()
        .bucket(&run.bucket)
        .prefix(format!("runs/{}/", run.id))
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        for object in page.map_err(failed("list the run's files"))?.contents() {
            if let Some(key) = object.key() {
                aws.s3
                    .delete_object()
                    .bucket(&run.bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(failed("delete the run's files"))?;
            }
        }
    }
    Ok(())
}

/// Ends the runner instances that interrupted runs left up, launched before `cutoff`.
pub(crate) async fn sweep(aws: &Aws, cutoff: i64) -> Result<()> {
    let left: Vec<String> = runners(aws, None)
        .await?
        .into_iter()
        .filter(|&(_, launched)| launched < cutoff)
        .map(|(id, _)| id)
        .collect();
    if !left.is_empty() {
        aws.ec2
            .terminate_instances()
            .set_instance_ids(Some(left))
            .send()
            .await
            .map_err(failed("end runner instances"))?;
        println!("ended runner instances that earlier runs left up");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_graviton_generations() {
        assert_eq!(goarm64("c8g.4xlarge"), Ok("v9.0"));
        assert_eq!(goarm64("m8gd.large"), Ok("v9.0"));
        assert_eq!(goarm64("c7gn.xlarge"), Ok("v8.4"));
        assert_eq!(goarm64("hpc7g.16xlarge"), Ok("v8.4"));
        assert_eq!(goarm64("c6g.large"), Ok("v8.2"));
        assert!(goarm64("c7i.4xlarge").is_err());
        assert!(goarm64("a1.large").is_err());
        assert!(goarm64("c8g").is_err());
    }

    #[test]
    fn counts_vcpus() {
        assert_eq!(vcpus("c8g.medium"), Ok(1));
        assert_eq!(vcpus("c8g.large"), Ok(2));
        assert_eq!(vcpus("m8g.xlarge"), Ok(4));
        assert_eq!(vcpus("m8g.4xlarge"), Ok(16));
        assert_eq!(vcpus("c7g.16xlarge"), Ok(64));
        assert!(vcpus("c8g.metal-24xl").is_err());
        assert!(vcpus("c8g.0xlarge").is_err());
        assert!(vcpus("c8g").is_err());
    }

    #[test]
    fn sizes_the_shards() {
        let runner = Runner::new("m8g.4xlarge".to_owned(), 4, false).unwrap();
        assert_eq!(runner.procs(), 4);
        assert_eq!(runner.clusters(), 4);
        assert_eq!(runner.driver(), "dsql");
        let runner = Runner::new("c8g.4xlarge".to_owned(), 3, true).unwrap();
        assert_eq!(runner.procs(), 5);
        assert_eq!(runner.clusters(), 0);
        assert_eq!(runner.driver(), "sqlite");
        assert!(Runner::new("c8g.xlarge".to_owned(), 5, false).is_err());
        assert!(Runner::new("c8g.4xlarge".to_owned(), 0, false).is_err());
        assert!(Runner::new("c7i.4xlarge".to_owned(), 1, false).is_err());
    }

    #[test]
    fn lives_long_enough() {
        let timeout: u64 = TIMEOUT
            .strip_suffix('m')
            .and_then(|m| m.parse().ok())
            .unwrap_or(0);
        let plan_wait = u64::from(PLAN_POLLS) * POLL.as_secs() / 60;
        assert!(LIFETIME_MINUTES >= 3 * timeout + plan_wait + 60);
    }

    fn run(clusters: &[&str]) -> Run {
        Run {
            id: "1700000000000".to_owned(),
            region: "eu-west-1".to_owned(),
            clusters: clusters.iter().map(|&cluster| cluster.to_owned()).collect(),
            table: "temporal-dsql-ci-1700000000000".to_owned(),
            bucket: "temporal-dsql-ci-bucket".to_owned(),
            test_flags: crate::test_flags(TIMEOUT, "dsql", std::num::NonZeroU16::new(10)),
        }
    }

    /// Fails if a placeholder is left in the script. The script's own expansions start
    /// with $.
    fn assert_filled_in(script: &str) {
        for (at, _) in script.match_indices('{') {
            let word = script[at + 1..].split('}').next().unwrap_or_default();
            let placeholder =
                !word.is_empty() && word.chars().all(|c| c.is_ascii_lowercase() || c == '_');
            assert!(
                !placeholder || script[..at].ends_with('$'),
                "{{{word}}} is left in the script"
            );
        }
    }

    #[test]
    fn writes_the_runner_script() {
        let runner = Runner::new("m8g.4xlarge".to_owned(), 2, false).unwrap();
        let script = script(
            &run(&["abcdefghijklmnopqrstuvwxyz", "zyxwvutsrqponmlkjihgfedcba"]),
            &runner,
        );
        assert!(script.starts_with("#!/bin/bash\nshutdown -h +360\n"));
        assert!(script.contains("\nrun=s3://temporal-dsql-ci-bucket/runs/1700000000000\n"));
        assert!(script.contains("\nexport GOMAXPROCS='8'\n"));
        assert!(script.contains("\nexport TEMPORAL_TEST_SHARED_CLUSTERS='2'\n"));
        assert!(script.contains("\nexport TEMPORAL_TEST_DEDICATED_CLUSTERS='2'\n"));
        assert!(script.contains("\nexport TEMPORAL_AWAIT_ATTEMPT_TIMEOUT='15s'\n"));
        assert!(script.contains("\nexport TEST_TOTAL_SHARDS='2'\n"));
        assert!(script.contains(
            "TEMPORAL_TEST_CLUSTER_EVENTS_FILE=\"$PWD/$dir/clusters.jsonl\" DSQL_TEST_POOL_METRICS_FILE=\"$PWD/$dir/pools.jsonl\" \\\n"
        ));
        assert!(script.contains(
            "\n    ./functional.test -test.v=test2json -test.count=1 -test.timeout=90m -persistenceType=sql -persistenceDriver=dsql -test.parallel=10 \"$@\" -test.cpuprofile=\"$dir/cpu.pprof\" "
        ));
        let lane = |shard: u16, cluster: &str| {
            format!(
                "\n(\n  export DSQL_TEST_ENDPOINT='{cluster}.dsql.eu-west-1.on.aws'\n  export DSQL_TEST_SCHEMAS='public,corpus_1,corpus_2,corpus_3,corpus_4,corpus_5,corpus_6,corpus_7,corpus_8'\n  export AWS_REGION='eu-west-1'\n  export TEST_SHARD_INDEX='{}'\n  attempt shard-{shard} '-test.skip={ALONE}'\n",
                shard - 1
            )
        };
        assert!(script.contains(&lane(1, "abcdefghijklmnopqrstuvwxyz")));
        assert!(script.contains(&lane(2, "zyxwvutsrqponmlkjihgfedcba")));
        assert_eq!(script.matches("attempt alone").count(), 1);
        assert!(script.contains(&format!(
            "\n  (unset TEST_TOTAL_SHARDS TEST_SHARD_INDEX && attempt alone '-test.run={ALONE}')\n  rerun 1\n) &\nlanes=\"$lanes $!\"\n"
        )));
        assert!(script.contains("\n  rerun 2\n) &\nlanes=\"$lanes $!\"\nwhile sleep 60; do\n"));
        assert!(script.contains("\n  for shard in 1 2; do\n"));
        assert!(script.contains("  for _ in $(seq 15); do\n"));
        assert!(script.contains("|| finish 125\n"));
        assert!(script.contains("--key runs/1700000000000/stop "));
        assert!(script.ends_with("\nwait $lanes\nfinish 0\n"));
        assert_filled_in(&script);

        let runner = Runner::new("m8g.4xlarge".to_owned(), 4, true).unwrap();
        let script = super::script(&run(&[]), &runner);
        assert!(!script.contains("DSQL_TEST_ENDPOINT"));
        assert!(script.contains("\n(\n  export TEST_SHARD_INDEX='3'\n  attempt shard-4 "));
        assert!(script.contains("\n  for shard in 1 2 3 4; do\n"));
        assert_filled_in(&script);
    }
}
