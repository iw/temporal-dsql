# pgxpool and DSQL simulator

`poolsim` is a deterministic, discrete-event simulation of pgxpool connections,
workload queues and DSQL connection limits. It makes no network calls and needs
no AWS credentials. Use it to inspect capacity, startup, connection expiry and
failure behavior. It is an approximation, not a benchmark of Temporal or DSQL.

The persistence plugin is **experimental and not recommended for production**.

## Run

From the repository root:

```bash
go run ./tools/poolsim -config tools/poolsim/scenarios/pgxpool-fleet-start.yaml -out /tmp/fleet-start.csv
go run ./tools/poolsim -config tools/poolsim/scenarios/pgxpool-400wps-peak-warm50.yaml -out /tmp/warm50.csv
go test ./tools/poolsim
```

Without `-config`, the simulator starts four service pools of 100 connections
for 20 simulated minutes. `-out` writes per-pool CSV samples; without it, only the
summary is printed. `-cpuprofile` writes a CPU profile. The seed defaults to 42;
arrival, handshake and lifetime randomness use independent seeded streams.

## What is modeled

- **DSQL cluster:** a configurable connection cap (default 10,000), an admission
  bucket of 100 connections/sec with capacity 1,000, handshake latency and a
  60-minute connection cutoff. Rejections consume handshake time: `53400` means
  rate pressure and `53300` means the connection cap.
- **pgxpool:** asynchronous minimum fill; constructing connections counted
  against `MaxConns`; LIFO idle acquisition; a ping after more than one second
  idle; on-demand construction; acquisition deadlines; and close/drain behavior.
  A constructor can complete after its requesting acquire times out.
- **Expiry:** positive whole-second lifetime jitter, expiry on return and during
  health checks, minimum fill before eviction, and a 500 ms follow-up health
  check after destruction. Acquires have no lifetime guard. Active queries run
  until completion or the DSQL cutoff. Failed minimum fill can defer eviction.
- **Instances:** replicas, startup delays, a shared connection-attempt limit,
  short-lived startup pools and replacement instances during a deployment.
- **Workload:** query hold time and jitter, arrival bursts, concurrency bounds,
  maximum acquisition waits and named phases on the simulation's global clock.
  Replacement instances enter the phase already in progress.

The simulator keeps three pacing strategies: `tokenBucket`, a shared bucket with
configurable simulated DynamoDB latency; `perSecond`, a strict global per-second
budget; and `adaptive`, a per-instance AIMD experiment. Adaptive pacing increases
its rate on success, halves it on `53400` and ignores `53300` as rate feedback.
The `ddb_outage` scenario permanently switches to adaptive pacing.

These strategies are simulation controls. The plugin offers local pacing and
DynamoDB GCRA reservations with a 30-second local fallback window. The simulated
bucket abstracts conditional writes, clock skew, reservation horizons and
recovery. The simulator holds an instance's attempt slot through the handshake;
the plugin frees its hook slot before the handshake. It does not simulate IAM,
TLS internals, SQL statements or transaction conflicts. Use plugin unit and live
tests for those behaviors.

## Configuration

YAML and JSON are accepted. Times in YAML are Go duration strings; JSON encodes
`time.Duration` values as integer nanoseconds.

```yaml
seed: 42
limiter: tokenBucket
duration: 3h
sampleEvery: 10s
steadyAfter: 90m
cluster:
  maxConnections: 10000
  handshake: {min: 50ms, max: 200ms}
limiterLatency: {min: 5ms, max: 10ms}
assertMaxConnectsPerSec: -1
assertConvergeWithin: 60s
assertMaxClosurePerSec: -1
assertZeroDSQLRejections: true
scenario: {kind: fleet_start, startWindow: 2s}
services:
  - name: history
    replicas: 4
    inflightLimit: 8
    ephemeral: {pools: 2, connections: 3, duration: 1s}
    pgxpool:
      maxConns: 100
      minConns: 100
      maxConnLifetime: 50m
      maxConnLifetimeJitter: 4m
      maxConnIdleTime: 30m
      healthCheckPeriod: 1m
    workload:
      enabled: true
      maxInUse: 30
      holdTime: 10ms
      holdJitter: 10ms
      spawnEvery: 1s
      arrivalBurst: 10
      maxWait: 10s
      phases:
        - name: peak
          at: 40m
          ramp: 5m
          duration: 30m
          maxInUse: 90
          arrivalBurst: 90
          spawnEvery: 100ms
          holdTime: 500ms
```

Each service defaults to one replica, eight concurrent attempts and a pgxpool
maximum of 100. `minConns` is explicit and can be zero; plugin service pools use
`minConns == maxConns`. The simulator defaults to a 50-minute lifetime, zero
lifetime jitter, zero idle timeout and one-minute health checks. Set the four-minute
jitter and full warm floor to match plugin service pools. A smaller warm floor
is a sizing experiment, not a plugin configuration option.

Phases are ordered, non-overlapping intervals. `at` starts the ramp, `ramp`
linearly changes concurrency, and `duration` holds the plateau. Other nonzero
phase settings override the base immediately; zero inherits the base. After a
phase, base demand resumes and in-flight work drains normally. Statistics stay
attributed to the phase in which each request started. Startup pools are excluded
from phase statistics.

## Scenarios

All supplied scenarios run for three simulated hours:

| File prefix `pgxpool-` | Purpose |
|---|---|
| `400wps` | 44 pools of 500 against a 10,000-connection cap; exposes oversubscription |
| `400wps-200` | 44 pools of 200 within the default cap |
| `400wps-cap25k` | 44 pools of 500 with a 25,000-connection cap |
| `400wps-peak` | Demand ramp and two short spikes against the larger cap |
| `400wps-peak-200` | Demand phases with 200-connection pools |
| `400wps-peak-warm50` | Minimum 50, maximum 500; exercises on-demand growth |
| `fleet-start`, `fleet-start-peak` | Startup spread over two seconds, with optional immediate peak |
| `mass-drop`, `mass-drop-peak` | Drop half the physical connections at 70 minutes |
| `rolling-deploy`, `rolling-deploy-peak` | Replace instances every 30 seconds starting at 70 minutes |
| `ddb-outage`, `ddb-outage-peak` | Switch from shared pacing to adaptive pacing at five seconds |

The `400wps` filenames identify a workload sizing family, not a measured workflow
rate. Peak scenarios stretch hold times to reduce event counts while preserving
pool-driving concurrency; their wait percentiles are not production latency
predictions. Deployment scenarios close each replaced instance before starting
its replacement; acquired connections drain during that overlap.

## Output and assertions

The summary reports completed and failed requests, acquire wait percentiles and
maximum, requests waiting for construction, peak and steady connection rates,
DSQL rejections, peak physical connections, one-hour cuts and lifetime churn.
Named phases have their own request, failure and wait statistics. CSV samples
include idle, in-use, constructing and physical connections per pool, cluster
usage, cumulative errors, maximum wait and instance peak attempt concurrency.

`assertMaxConnectsPerSec`, `assertMaxClosurePerSec` and `assertConvergeWithin` can
be disabled with negative values. Zero selects defaults: 100/sec, 200/sec and
60 seconds respectively. `assertStableFor` defaults to five seconds.
`assertZeroDSQLRejections` and `assertZeroDeadRequests` opt into failure checks.
A burst-capable bucket can exceed 100 opens in an individual second, so use the
DSQL rejection assertion for admission safety. Stress scenarios keep selected
assertions disabled to allow inspection of expected failures. Failed assertions
exit nonzero after producing output.
