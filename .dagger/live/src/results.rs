//! Reads the functional tests' results, from test2json's events and from the samples the
//! test process takes of itself in pools.jsonl, and summarizes them.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use aws_smithy_types::DateTime;
use aws_smithy_types::date_time::Format;

use crate::json_field;

/// The most failed tests that a test process runs again on their own. More point at
/// something a second attempt can't separate from the tests, such as the process itself.
pub(crate) const MAX_RERUNS: usize = 20;
/// How long the test process must have stopped while a failed test ran for the summary to
/// say so: the test may have failed for want of time rather than for what it tests.
const STALL_SECONDS: f64 = 1.0;
/// How often the test process samples itself.
const SAMPLE_SECONDS: f64 = 5.0;

/// How a test ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Pass,
    Fail,
    Skip,
}

/// One test process's results, from test2json's events.
#[derive(Default)]
pub(crate) struct Results {
    /// The tests that started, with when, in seconds since the epoch.
    started: BTreeMap<String, Option<f64>>,
    /// The tests that ended, with how and when.
    ended: BTreeMap<String, (Outcome, Option<f64>)>,
    /// Set when test2json reports that the package failed, as it does when a test fails
    /// and when the test binary itself fails, such as on its timeout.
    package_failed: bool,
}

impl Results {
    /// Reads every event in test2json's output.
    pub(crate) fn read(events: &str) -> Self {
        let mut results = Self::default();
        for event in events.lines() {
            results.add(event);
        }
        results
    }

    /// Records an event that starts or ends a test, and returns a line about each
    /// top-level test that ends.
    pub(crate) fn add(&mut self, event: &str) -> Option<String> {
        let action = json_field(event, "Action")?;
        if action == "output" {
            return None;
        }
        let Some(test) = json_field(event, "Test") else {
            self.package_failed |= action == "fail";
            return None;
        };
        let time = json_field(event, "Time").and_then(seconds);
        let outcome = match action {
            "run" => {
                self.started.insert(test.to_owned(), time);
                return None;
            }
            "pass" => Outcome::Pass,
            "fail" => Outcome::Fail,
            "skip" => Outcome::Skip,
            _ => return None,
        };
        self.ended.insert(test.to_owned(), (outcome, time));
        if test.contains('/') {
            return None;
        }
        let elapsed = json_field(event, "Elapsed").unwrap_or("?");
        Some(format!("{action} {test} ({elapsed}s)"))
    }

    /// The top-level tests, with how each ended, or None if it started and has no result:
    /// the test binary stopped before it ended, or its result line ran into a server's log
    /// output, which test2json then reads as output.
    pub(crate) fn top_level(&self) -> BTreeMap<&str, Option<Outcome>> {
        let mut tests: BTreeMap<&str, Option<Outcome>> = self
            .started
            .keys()
            .filter(|test| !test.contains('/'))
            .map(|test| (test.as_str(), None))
            .collect();
        for (test, &(outcome, _)) in &self.ended {
            if !test.contains('/') {
                tests.insert(test, Some(outcome));
            }
        }
        tests
    }

    /// The failed tests none of whose subtests failed: the tests to look into, and to run
    /// again.
    pub(crate) fn failing_leaves(&self) -> Vec<&str> {
        let failed: Vec<&str> = self
            .ended
            .iter()
            .filter(|(_, (outcome, _))| *outcome == Outcome::Fail)
            .map(|(test, _)| test.as_str())
            .collect();
        let parents: BTreeSet<&str> = failed
            .iter()
            .flat_map(|test| test.match_indices('/').map(|(at, _)| &test[..at]))
            .collect();
        failed
            .into_iter()
            .filter(|test| !parents.contains(test))
            .collect()
    }

    /// How a test ended, if it did.
    fn outcome(&self, test: &str) -> Option<Outcome> {
        self.ended.get(test).map(|&(outcome, _)| outcome)
    }

    /// When a test started and ended, if its events say.
    fn span(&self, test: &str) -> Option<(f64, f64)> {
        let start = (*self.started.get(test)?)?;
        let end = self.ended.get(test)?.1?;
        Some((start, end))
    }
}

/// The test process's stop-the-world pauses other than the garbage collector's, from its
/// samples in pools.jsonl.
#[derive(Default)]
pub(crate) struct Pauses {
    /// When each sample was taken, and how many seconds the process stopped since the
    /// sample before it.
    samples: Vec<(f64, f64)>,
}

impl Pauses {
    /// Reads every sample in pools.jsonl, which also holds the pools' metrics.
    pub(crate) fn read(lines: &str) -> Self {
        let samples = lines
            .lines()
            .filter(|line| json_field(line, "metric") == Some("process_other_pause_ms"))
            .filter_map(|line| {
                let time = seconds(json_field(line, "time")?)?;
                let paused: f64 = json_field(line, "value")?.parse().ok()?;
                Some((time, paused / 1000.0))
            })
            .collect();
        Self { samples }
    }

    /// How long the process stopped while a test ran: the pauses in the samples whose
    /// intervals overlap the test's.
    fn during(&self, start: f64, end: f64) -> f64 {
        self.samples
            .iter()
            .filter(|&&(time, _)| time > start && time - SAMPLE_SECONDS < end)
            .map(|&(_, paused)| paused)
            .sum()
    }
}

/// An RFC 3339 time as seconds since the epoch.
fn seconds(time: &str) -> Option<f64> {
    DateTime::from_str(time, Format::DateTimeWithOffset)
        .ok()
        .map(|time| time.as_secs_f64())
}

/// What a test process runs again once it ends.
#[derive(Debug, PartialEq)]
pub(crate) enum Rerun {
    /// No test failed.
    Nothing,
    /// These failed tests, each on its own.
    Tests(Vec<String>),
    /// None of this many failed tests, since they're more than MAX_RERUNS.
    TooMany(usize),
}

impl Rerun {
    /// What to run again, from the test-level fail events in test2json's output.
    pub(crate) fn plan(failed: &str) -> Self {
        let results = Results::read(failed);
        let leaves = results.failing_leaves();
        match leaves.len() {
            0 => Self::Nothing,
            count if count > MAX_RERUNS => Self::TooMany(count),
            _ => Self::Tests(leaves.into_iter().map(str::to_owned).collect()),
        }
    }

    /// The -test.run pattern for the tests to run again, which is empty if there are none.
    pub(crate) fn pattern(&self) -> String {
        let Self::Tests(tests) = self else {
            return String::new();
        };
        // As upstream's test runner writes it: each test's name split at its slashes, each
        // part matched whole, and the tests joined with |.
        tests
            .iter()
            .map(|test| {
                test.split('/')
                    .map(|part| format!("^{}$", quote_meta(part)))
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .collect::<Vec<_>>()
            .join("|")
    }
}

impl std::fmt::Display for Rerun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nothing => write!(f, "no test failed"),
            Self::Tests(tests) => write!(
                f,
                "running {} again on their own",
                plural(tests.len(), "failed test")
            ),
            Self::TooMany(count) => write!(
                f,
                "{count} tests failed, too many to run again; the most is {MAX_RERUNS}"
            ),
        }
    }
}

/// Escapes text for a Go regular expression, as Go's regexp.QuoteMeta does.
fn quote_meta(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len());
    for c in text.chars() {
        if r"\.+*?()|[]{}^$".contains(c) {
            quoted.push('\\');
        }
        quoted.push(c);
    }
    quoted
}

/// One test process's run.
pub(crate) struct Attempt {
    /// What the run calls the process, such as "shard 2", or empty when it has only one.
    pub(crate) name: String,
    pub(crate) results: Results,
    pub(crate) pauses: Pauses,
    /// How the test binary exited, as far as the run knows.
    pub(crate) exit: String,
    /// Whether the process's failed tests were to run again on their own.
    pub(crate) reruns: bool,
    /// The process that ran them again, if one did.
    pub(crate) again: Option<Box<Attempt>>,
}

impl Attempt {
    /// A line with the process's top-level tallies.
    fn tallies(&self) -> String {
        let tests = self.results.top_level();
        let count = |outcome| tests.values().filter(|&&o| o == outcome).count();
        let unfinished = count(None);
        let failed = count(Some(Outcome::Fail));
        let name = if self.name.is_empty() {
            "functional tests".to_owned()
        } else {
            self.name.clone()
        };
        let mut line = format!(
            "--- {name}: {} passed, {failed} failed, {} skipped",
            count(Some(Outcome::Pass)),
            count(Some(Outcome::Skip)),
        );
        if unfinished > 0 {
            let _ = write!(line, ", {unfinished} without a result");
        }
        let _ = write!(line, "; the test binary exited with {}", self.exit);
        // Failed tests explain the package's failure unless the binary stopped early.
        if self.results.package_failed && (failed == 0 || unfinished > 0) {
            line.push_str(", reporting a failure of its own");
        }
        let leaves = self.results.failing_leaves().len();
        if self.reruns && self.again.is_none() && leaves > 0 {
            if leaves > MAX_RERUNS {
                let _ = write!(line, "; {leaves} tests failed, too many to run again");
            } else {
                line.push_str("; its failed tests didn't run again");
            }
        }
        line
    }

    /// How a failed test fared when it ran again, if it did.
    fn again(&self, test: &str) -> &'static str {
        let Some(again) = &self.again else {
            return "";
        };
        match again.results.outcome(test) {
            Some(Outcome::Pass) => ", passed when it ran again on its own",
            Some(Outcome::Fail) => ", failed again on its own",
            Some(Outcome::Skip) => ", skipped when it ran again on its own",
            None => ", had no result when it ran again on its own",
        }
    }

    /// Says how long the process stopped while a failed test ran, if long enough to
    /// matter.
    fn stall(&self, test: &str) -> String {
        let Some((start, end)) = self.results.span(test) else {
            return String::new();
        };
        let stopped = self.pauses.during(start, end);
        if stopped < STALL_SECONDS {
            return String::new();
        }
        format!("; the test process stopped for {stopped:.1} s while it ran")
    }
}

/// Summarizes the functional tests' processes: each one's top-level tallies, then, with
/// more than one, the top-level tests' tallies across them all, then every failed test none
/// of whose subtests failed, with how it fared when it ran again, and the top-level tests
/// without a result. A test that passes only when it runs again still counts as failed.
pub(crate) fn summary(attempts: &[Attempt]) -> String {
    let mut text = String::new();
    for attempt in attempts {
        let _ = writeln!(text, "{}", attempt.tallies());
        if let Some(again) = &attempt.again {
            let _ = writeln!(text, "{}", again.tallies());
        }
    }

    let named = attempts.len() > 1;
    if named {
        // A top-level test runs in every shard, and only its shard's subtests run there.
        // It failed if it failed in any process, and passed if it passed in one and has a
        // result in all.
        let mut tests: BTreeMap<&str, Option<Outcome>> = BTreeMap::new();
        let rank = |outcome: Option<Outcome>| match outcome {
            Some(Outcome::Skip) => 0,
            Some(Outcome::Pass) => 1,
            None => 2,
            Some(Outcome::Fail) => 3,
        };
        for attempt in attempts {
            for (test, outcome) in attempt.results.top_level() {
                let merged = tests.entry(test).or_insert(outcome);
                if rank(outcome) > rank(*merged) {
                    *merged = outcome;
                }
            }
        }
        let count = |outcome| tests.values().filter(|&&o| o == outcome).count();
        let _ = writeln!(
            text,
            "--- in all: {} top-level tests, {} passed, {} failed, {} skipped, {} without a result",
            tests.len(),
            count(Some(Outcome::Pass)),
            count(Some(Outcome::Fail)),
            count(Some(Outcome::Skip)),
            count(None),
        );
    }

    let mut failed = 0;
    let mut passed_again = 0;
    let mut lines = String::new();
    for attempt in attempts {
        let process = if named {
            format!(" ({})", attempt.name)
        } else {
            String::new()
        };
        for test in attempt.results.failing_leaves() {
            failed += 1;
            let again = attempt.again(test);
            if again.starts_with(", passed") {
                passed_again += 1;
            }
            let _ = writeln!(
                lines,
                "failed: {test}{process}{again}{}",
                attempt.stall(test)
            );
        }
    }
    for attempt in attempts {
        let process = if named {
            format!(" ({})", attempt.name)
        } else {
            String::new()
        };
        for (test, outcome) in attempt.results.top_level() {
            if outcome.is_none() {
                let _ = writeln!(lines, "no result: {test}{process}");
            }
        }
    }
    if failed > 0 {
        let _ = write!(
            text,
            "--- {} without failed subtests",
            plural(failed, "failed test")
        );
        if attempts.iter().any(|attempt| attempt.again.is_some()) {
            let _ = write!(text, "; run again on their own, {passed_again} passed");
        }
        text.push('\n');
    }
    text.push_str(&lines);
    text
}

/// Whether any test in the processes has a result.
pub(crate) fn any_ran(attempts: &[Attempt]) -> bool {
    attempts
        .iter()
        .any(|attempt| attempt.results.top_level().values().any(Option::is_some))
}

/// A count with its noun, such as "1 failed test" or "3 failed tests".
pub(crate) fn plural(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(time: &str, action: &str, test: &str) -> String {
        format!(
            r#"{{"Time":"2026-11-13T17:{time}Z","Action":"{action}","Package":"p","Test":"{test}","Elapsed":2}}"#
        )
    }

    #[test]
    fn reads_results() {
        let mut results = Results::default();
        assert_eq!(results.add(&event("00:00", "run", "TestA")), None);
        assert_eq!(results.add(&event("00:01", "run", "TestA/b")), None);
        assert_eq!(results.add(&event("00:09", "fail", "TestA/b")), None);
        assert_eq!(
            results.add(&event("00:10", "fail", "TestA")),
            Some("fail TestA (2s)".to_owned())
        );
        results.add(&event("00:11", "run", "TestC"));
        results.add(&event("00:12", "run", "TestC/d"));
        results.add(&event("00:13", "run", "TestC/d/e"));
        results.add(&event("00:14", "pass", "TestC/d/e"));
        results.add(&event("00:15", "fail", "TestC/d"));
        results.add(&event("00:16", "fail", "TestC"));
        results.add(&event("00:17", "run", "TestD"));
        assert_eq!(
            results.add(&event("00:18", "skip", "TestE")),
            Some("skip TestE (2s)".to_owned())
        );
        assert_eq!(
            results.add(r#"{"Action":"output","Test":"TestD","Output":"\"Action\":\"fail\""}"#),
            None
        );
        assert!(!results.package_failed);
        results.add(r#"{"Action":"fail","Package":"p","Elapsed":9}"#);
        assert!(results.package_failed);

        assert_eq!(results.failing_leaves(), ["TestA/b", "TestC/d"]);
        assert_eq!(
            results.top_level(),
            BTreeMap::from([
                ("TestA", Some(Outcome::Fail)),
                ("TestC", Some(Outcome::Fail)),
                ("TestD", None),
                ("TestE", Some(Outcome::Skip)),
            ])
        );
        let (start, end) = results.span("TestA/b").unwrap_or_default();
        assert!((end - start - 8.0).abs() < 1e-6, "{start} {end}");
        assert_eq!(results.span("TestD"), None);
    }

    #[test]
    fn plans_reruns() {
        let failed = [
            event("00:01", "fail", "TestA/b"),
            event("00:02", "fail", "TestA"),
            event("00:03", "fail", "TestF(x)"),
        ]
        .join("\n");
        let rerun = Rerun::plan(&failed);
        assert_eq!(
            rerun,
            Rerun::Tests(vec!["TestA/b".to_owned(), "TestF(x)".to_owned()])
        );
        assert_eq!(rerun.pattern(), r"^TestA$/^b$|^TestF\(x\)$");
        assert_eq!(
            rerun.to_string(),
            "running 2 failed tests again on their own"
        );

        assert_eq!(Rerun::plan(""), Rerun::Nothing);
        assert_eq!(Rerun::Nothing.pattern(), "");
        let many: Vec<String> = (0..=MAX_RERUNS)
            .map(|n| event("00:01", "fail", &format!("TestG/{n}")))
            .collect();
        let rerun = Rerun::plan(&many.join("\n"));
        assert_eq!(rerun, Rerun::TooMany(MAX_RERUNS + 1));
        assert_eq!(rerun.pattern(), "");
    }

    #[test]
    fn quotes_like_go() {
        assert_eq!(
            quote_meta(r"a.b+c*d?e(f)g|h[i]j{k}l^m$n\o#p-q"),
            r"a\.b\+c\*d\?e\(f\)g\|h\[i\]j\{k\}l\^m\$n\\o#p-q"
        );
    }

    #[test]
    fn reads_pauses() {
        let sample = |time: &str, metric: &str, value: f64| {
            format!(
                r#"{{"time":"2026-11-13T17:{time}Z","cluster":"","test":"","metric":"{metric}","value":{value}}}"#
            )
        };
        let pauses = Pauses::read(
            &[
                sample("00:05", "process_other_pause_ms", 400.0),
                sample("00:10", "process_other_pause_ms", 1500.0),
                sample("00:10", "process_gc_pause_ms", 9000.0),
                sample("00:15", "process_other_pause_ms", 700.0),
                r#"{"time":"2026-11-13T17:00:20Z","cluster":"corpus_1","test":"TestA","metric":"dsql_pool_acquires_total","value":3}"#.to_owned(),
            ]
            .join("\n"),
        );
        let at = |time: &str| seconds(&format!("2026-11-13T17:{time}Z")).unwrap_or_default();
        // The sample at :10 covers :05 to :10, and the one at :15 covers :10 to :15.
        assert!((pauses.during(at("00:06"), at("00:09")) - 1.5).abs() < 1e-9);
        assert!((pauses.during(at("00:09"), at("00:11")) - 2.2).abs() < 1e-9);
        assert!(pauses.during(at("00:15"), at("00:20")).abs() < 1e-9);
    }

    #[test]
    fn summarizes_one_process() {
        let mut results = Results::default();
        for line in [
            event("00:00", "run", "TestA"),
            event("00:01", "run", "TestA/b"),
            event("00:09", "fail", "TestA/b"),
            event("00:10", "fail", "TestA"),
            event("00:11", "pass", "TestB"),
            event("00:12", "run", "TestC"),
        ] {
            results.add(&line);
        }
        results.add(r#"{"Action":"fail","Package":"p","Elapsed":9}"#);
        let pauses = Pauses::read(
            r#"{"time":"2026-11-13T17:00:05Z","metric":"process_other_pause_ms","value":1250}"#,
        );
        let attempt = Attempt {
            name: String::new(),
            results,
            pauses,
            exit: "exit status: 1".to_owned(),
            reruns: false,
            again: None,
        };
        assert!(any_ran(std::slice::from_ref(&attempt)));
        assert_eq!(
            summary(&[attempt]),
            "--- functional tests: 1 passed, 1 failed, 0 skipped, 1 without a result; the test binary exited with exit status: 1, reporting a failure of its own\n\
             --- 1 failed test without failed subtests\n\
             failed: TestA/b; the test process stopped for 1.2 s while it ran\n\
             no result: TestC\n"
        );
    }

    #[test]
    fn summarizes_shards() {
        let attempt = |name: &str, lines: &[String], reruns, again: Option<Attempt>| Attempt {
            name: name.to_owned(),
            results: Results::read(&lines.join("\n")),
            pauses: Pauses::default(),
            exit: "exit status: 1".to_owned(),
            reruns,
            again: again.map(Box::new),
        };
        let shard1 = attempt(
            "shard 1",
            &[
                event("00:01", "fail", "TestA/b"),
                event("00:02", "fail", "TestA/c"),
                event("00:03", "fail", "TestA"),
                event("00:04", "skip", "TestB"),
            ],
            true,
            Some(attempt(
                "shard 1, run again",
                &[
                    event("00:05", "pass", "TestA/b"),
                    event("00:06", "fail", "TestA/c"),
                    event("00:07", "fail", "TestA"),
                ],
                false,
                None,
            )),
        );
        let shard2 = attempt(
            "shard 2",
            &[
                event("00:01", "pass", "TestA"),
                event("00:02", "pass", "TestB"),
                event("00:03", "run", "TestD"),
            ],
            true,
            None,
        );
        let shard3 = attempt("shard 3", &[event("00:01", "fail", "TestE")], true, None);
        let summary = summary(&[shard1, shard2, shard3]);
        assert_eq!(
            summary,
            "--- shard 1: 0 passed, 1 failed, 1 skipped; the test binary exited with exit status: 1\n\
             --- shard 1, run again: 0 passed, 1 failed, 0 skipped; the test binary exited with exit status: 1\n\
             --- shard 2: 2 passed, 0 failed, 0 skipped, 1 without a result; the test binary exited with exit status: 1\n\
             --- shard 3: 0 passed, 1 failed, 0 skipped; the test binary exited with exit status: 1; its failed tests didn't run again\n\
             --- in all: 4 top-level tests, 1 passed, 2 failed, 0 skipped, 1 without a result\n\
             --- 3 failed tests without failed subtests; run again on their own, 1 passed\n\
             failed: TestA/b (shard 1), passed when it ran again on its own\n\
             failed: TestA/c (shard 1), failed again on its own\n\
             failed: TestE (shard 3)\n\
             no result: TestD (shard 2)\n"
        );
    }

    #[test]
    fn counts_with_nouns() {
        assert_eq!(plural(1, "test"), "1 test");
        assert_eq!(plural(2, "failed test"), "2 failed tests");
        assert_eq!(plural(0, "cluster"), "0 clusters");
    }
}
