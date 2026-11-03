# adaptivereplay

`adaptivereplay` replays the signal plane's traces through the adaptive controllers' laws,
offline. Each decision in a trace records what its law stepped on. A replay with the
recorded parameters gives back the recorded proposals, and one with other parameters shows
what they would have proposed on the same signals. See
[Offline replay](../../docs/dsql/adaptive-behaviour.md#offline-replay) for how replay stays
exact and where it stops.

## Record

Enable the signal plane and set its trace directory before the host starts:

```yaml
adaptive.signals:
  - value:
      Enabled: true
      TraceDirectory: /var/tmp/temporal-signals
```

Each service writes `<service>-<host>-<pid>.jsonl` there, up to `TraceMaxBytes`, 1 GiB by
default. Run the controllers in shadow or enabled mode: a disabled controller records
nothing. Trace files name the host, so keep them with the host's other operational data.

## Run

From the repository root:

```bash
go run ./cmd/tools/adaptivereplay /var/tmp/temporal-signals/*.jsonl
go run ./cmd/tools/adaptivereplay -set worker_count.TargetDelay=250ms history-host1-4211.jsonl
go run ./cmd/tools/adaptivereplay -controller partition_scaler -csv steps.csv matching-*.jsonl
```

- `-set controller.Field=value` replays with one of a law's parameters set to a value.
  Repeat it to set more.
  - The controller part names a controller, or a family of them followed by `_`:
    `worker_count` covers `worker_count_transfer` and the other queues, and `memory_budget`
    covers the caches and the pending-task limits.
  - The field is one of the `Envelope` fields for `persistence_capacity` and one of the
    `Params` fields for the others. The controllers the signal plane drives take
    `MaxStepFrac` too.
  - Durations use Go's syntax, such as `250ms`.
  - The tool stops with an error when no controller in the traces matches an override, or
    when the law has no such field. The error lists the fields it has.
- `-controller prefix` replays only the controllers whose names start with the prefix.
- `-csv file` writes each replayed step to a CSV file, one row per step.
  - The row starts with the trace file, service and pid, the controller and its key.
  - Then come the time, the static value, the recorded and replayed proposals and reasons,
    and whether the law stepped.

## Output

Each recording in a file gets a line that names it. The line gives the file, the service on
its host and pid, when it starts and how long it covers, and its snapshots and decisions.
A table follows, with one row for each controller's series:

| Column | Meaning |
|---|---|
| CONTROLLER, KEY | The controller. Partition scaler series are keyed by task queue (`namespace/name/type`), and fair share series by limit and namespace |
| STEPS | Decisions replayed |
| MATCHED | Steps whose replayed proposal and reason are the recorded ones. With the recorded parameters, every step should match |
| CHANGES, REVERSALS | How often the proposal changed, and how often a change went the other way from the one before |
| MIN, MAX, FINAL | The lowest, highest and last proposals, as a share of the static value |
| REASONS | Steps by reason, the most frequent first |

With `-set`, each series gets a second row, which measures the recorded proposals for
comparison. A controller the tool doesn't know gets a row of its own, marked not replayed.

## Limits

- Replay is open-loop. A proposal under other parameters doesn't change the signals that
  follow. So a what-if shows direction and timing, as shadow mode does, but not where the
  system would have settled.
- A recording opened on a running host, by setting the trace directory then, starts each
  law from its static value. Its first steps can differ from the recorded ones until the
  law's state catches up.
- Fair share records a namespace's decision only when its mode or reason changes, or its
  share moves by `Deadband`, so its series holds those changes. A held or frozen step keeps
  the reason it recorded.

## Test

```bash
go test ./tools/adaptivereplay
```
