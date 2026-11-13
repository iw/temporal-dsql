//! Runs the functional tests on a Graviton instance in the cluster's region, for
//! `--corpus --instance <type>`.

use std::io::BufRead as _;
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

use crate::{
    Aws, GO_TEST_TAGS, Masker, REPO, Result, Run, TAG, Tally, corpus_dir, describe, failed,
    summarize, test_env, write_masked,
};

/// The runner instances' image: the latest Amazon Linux 2023 for arm64, which comes with
/// the AWS CLI.
const IMAGE: &str =
    "resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64";
/// The instance profile and the security group of a runner instance. The profile's role
/// connects to CI clusters and reads and writes run files, and the group allows no inbound
/// traffic.
const NAME: &str = "temporal-dsql-ci-runner";
/// How long a runner instance lives at most: the tests' timeout, and an hour to fetch them
/// and upload their output. It then shuts down, which terminates it.
const LIFETIME_MINUTES: u64 = 9 * 60;
/// How often a runner instance reports, and this machine checks on it.
const POLL: Duration = Duration::from_secs(60);
/// How long a runner instance has to make its first report.
const FIRST_REPORT: Duration = Duration::from_secs(15 * 60);
/// How many checks on a runner instance can fail in a row, such as while this machine is
/// offline, before the run gives up on it.
const CHECK_FAILURES: u32 = 15;
/// The exit code a runner instance reports when it can't fetch the tests.
const FETCH_FAILED: i32 = 125;
/// What this machine builds for a runner instance.
const BINARIES: &[&str] = &["functional.test", "test2json"];
/// What a runner instance writes, and whether each file is text for the masker.
const OUTPUT: &[(&str, bool)] = &[
    ("results.jsonl", true),
    ("clusters.jsonl", true),
    ("stderr.log", true),
    ("cpu.pprof", false),
];
/// A runner instance's boot script, which runs as root. It fetches the tests, runs them
/// with their output in out/, and reports the top-level tests' results each poll. If the
/// run's `stop` file appears, it sends the tests SIGQUIT, which ends them with their
/// goroutines' stacks. When the tests end, it uploads out/ and their exit code, and shuts
/// down.
const SCRIPT: &str = r#"#!/bin/bash
shutdown -h +{minutes}
export HOME=/root AWS_REGION={region} AWS_DEFAULT_REGION={region}
run=s3://{bucket}/runs/{id}
mkdir -p /var/tmp/ci/out && cd /var/tmp/ci || exit 1
finish() {
  for file in out/*; do aws s3 cp --quiet "$file" "$run/$file"; done
  echo "$1" > exit-code && aws s3 cp --quiet exit-code "$run/exit-code"
  shutdown -h now
  exit
}
aws s3 cp --quiet "$run/bin/functional.test" . && aws s3 cp --quiet "$run/bin/test2json" . &&
  chmod +x functional.test test2json || finish {fetch_failed}
{exports}export TEMPORAL_ROOT=/var/tmp/ci
while sleep {poll}; do
  grep -E '"Action":"(pass|fail|skip)","Package":"[^"]*","Test":"[^"/]*",' out/results.jsonl > progress.jsonl
  aws s3 cp --quiet progress.jsonl "$run/progress.jsonl"
  aws s3api head-object --bucket {bucket} --key runs/{id}/stop > /dev/null 2>&1 && pkill -QUIT -x functional.test
done &
./functional.test {flags} -test.cpuprofile=out/cpu.pprof 2> out/stderr.log |
  ./test2json -t -p go.temporal.io/server/tests > out/results.jsonl
finish "${PIPESTATUS[0]}"
"#;

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
/// writes their output, masked, to target/corpus/<run>, as `crate::functional_tests`
/// does, with their CPU profile as cpu.pprof.
pub(crate) async fn functional_tests(
    aws: &Aws,
    run: &Run,
    masker: &Masker,
    kind: &str,
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
    let (instance, zone) = launch(aws, run, kind).await?;
    println!("--- running the functional tests on a {kind} in {zone}");
    let code = wait(aws, run, masker, &instance).await?;
    if code == FETCH_FAILED {
        return Err("the runner instance couldn't fetch the tests".into());
    }

    println!(
        "--- fetching the functional tests' output, to {}",
        out.display()
    );
    for &(name, text) in OUTPUT {
        let fetched = aws
            .s3
            .get_object()
            .bucket(&run.bucket)
            .key(format!("{prefix}/out/{name}"))
            .send()
            .await;
        let object = match fetched {
            Ok(object) => object,
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|err| err.is_no_such_key()) =>
            {
                println!("the runner instance wrote no {name}");
                continue;
            }
            Err(err) => return Err(describe("fetch the tests' output", &err).into()),
        };
        if text {
            let body = Box::pin(object.body.into_async_read());
            write_masked(body, out.join(name), masker.clone()).await?;
        } else {
            std::fs::write(out.join(name), object.body.collect().await?.into_bytes())?;
        }
    }
    let mut tally = Tally::default();
    if let Ok(results) = std::fs::File::open(out.join("results.jsonl")) {
        for line in std::io::BufReader::new(results).lines() {
            tally.add(&line?);
        }
    }
    summarize(&out, &tally, format!("exit status: {code}"))
}

/// Starts a runner instance in the first CI subnet whose zone has capacity for it, and
/// returns its ID and zone.
async fn launch(aws: &Aws, run: &Run, kind: &str) -> Result<(String, String)> {
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

    let script = aws_smithy_types::base64::encode(script(run));
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
fn script(run: &Run) -> String {
    let exports: String = test_env(run, "/var/tmp/ci/out/clusters.jsonl")
        .into_iter()
        .map(|(name, value)| format!("export {name}='{value}'\n"))
        .collect();
    SCRIPT
        .replace("{exports}", &exports)
        .replace("{flags}", &run.test_flags.join(" "))
        .replace("{minutes}", &LIFETIME_MINUTES.to_string())
        .replace("{poll}", &POLL.as_secs().to_string())
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

/// Waits for the runner instance to report the tests' exit code, and returns it. It
/// prints each top-level test's result as the instance reports it.
async fn wait(aws: &Aws, run: &Run, masker: &Masker, instance: &str) -> Result<i32> {
    let started = Instant::now();
    let limit = Duration::from_secs(LIFETIME_MINUTES * 60) + FIRST_REPORT;
    let progress = format!("runs/{}/progress.jsonl", run.id);
    let exit = format!("runs/{}/exit-code", run.id);
    let mut tally = Tally::default();
    // The report's lines printed so far, once there is a report.
    let mut reported: Option<usize> = None;
    let mut failures = 0;
    loop {
        tokio::time::sleep(POLL).await;
        let checked: Result<Check> = async {
            if let Some(report) = read_object(&aws.s3, &run.bucket, &progress).await? {
                for line in report.lines().skip(reported.unwrap_or(0)) {
                    if let Some(result) = tally.add(&masker.mask(line)) {
                        println!("{result}");
                    }
                }
                reported = Some(report.lines().count());
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
            Ok(Check::Exited(code)) => return Ok(code),
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
        if reported.is_none() && started.elapsed() > FIRST_REPORT {
            return Err("the runner instance didn't report in its first 15 minutes".into());
        }
        if started.elapsed() > limit {
            return Err("the runner instance outlived the tests' timeout".into());
        }
    }
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
    fn writes_the_runner_script() {
        let run = Run {
            id: "1700000000000".to_owned(),
            region: "eu-west-1".to_owned(),
            cluster: "abcdefghijklmnopqrstuvwxyz".to_owned(),
            table: "temporal-dsql-ci-1700000000000".to_owned(),
            bucket: "temporal-dsql-ci-bucket".to_owned(),
            test_flags: crate::test_flags(std::num::NonZeroU16::new(10)),
        };
        let script = script(&run);
        assert!(script.starts_with("#!/bin/bash\nshutdown -h +540\n"));
        assert!(script.contains("\nrun=s3://temporal-dsql-ci-bucket/runs/1700000000000\n"));
        assert!(script.contains(
            "\nexport DSQL_TEST_ENDPOINT='abcdefghijklmnopqrstuvwxyz.dsql.eu-west-1.on.aws'\n"
        ));
        assert!(script.contains("\nexport TEMPORAL_TEST_SHARED_CLUSTERS='3'\n"));
        assert!(script.contains(
            "\nexport TEMPORAL_TEST_CLUSTER_EVENTS_FILE='/var/tmp/ci/out/clusters.jsonl'\n"
        ));
        assert!(script.contains(
            "\n./functional.test -test.v=test2json -test.count=1 -test.timeout=8h -persistenceType=sql -persistenceDriver=dsql -test.parallel=10 -test.cpuprofile=out/cpu.pprof "
        ));
        assert!(script.contains("|| finish 125\n"));
        assert!(script.contains("\nwhile sleep 60; do\n"));
        assert!(script.contains("--key runs/1700000000000/stop "));
        // Every placeholder is filled in. The script's own expansions start with $.
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
}
