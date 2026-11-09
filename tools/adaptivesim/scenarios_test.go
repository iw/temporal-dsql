package adaptivesim

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/adaptive/fairshare"
	"go.temporal.io/server/common/adaptive/memory"
	"go.temporal.io/server/common/adaptive/partitions"
	"go.temporal.io/server/common/adaptive/taskrate"
	workercount "go.temporal.io/server/common/adaptive/workers"
	"go.temporal.io/server/tools/adaptivereplay"
)

func runScenario(t *testing.T, name, variant string, opts Options) *Result {
	t.Helper()
	sc, err := Lookup(name, variant)
	require.NoError(t, err)
	r, err := Run(sc, opts)
	require.NoError(t, err)
	require.Len(t, r.Hosts, sc.Hosts)
	return r
}

// loadEnd is when the short pool's load ends.
const loadEnd = 150 * time.Second

// The local rig's burst (docs/dsql/adaptive-appreciation.md, finding 7): today's path
// completed 2 of 1,549 tasks and never cleared the burst; the gate and the pipelined read
// completed all of them and cleared it in about 31 s; the gate alone completed all of
// them, in 226 s.
func TestShortPoolBurst(t *testing.T) {
	t.Parallel()
	base := runScenario(t, "short-pool-burst", "base", Options{})
	gated := runScenario(t, "short-pool-burst", "gate", Options{})
	both := runScenario(t, "short-pool-burst", "pipeline-gate", Options{})

	today := base.Hosts[0].Tasks
	require.Less(t, today.Completed*20, today.Arrived, "today's path completes few of the tasks")
	require.Zero(t, today.ReloadCleared, "today's path never clears the burst")

	fixed := both.Hosts[0].Tasks
	require.Equal(t, fixed.Arrived, fixed.Completed)
	require.Positive(t, fixed.ReloadCleared)
	require.Less(t, fixed.ReloadCleared, time.Minute)

	gate := gated.Hosts[0].Tasks
	require.Equal(t, gate.Arrived, gate.Completed)
	require.Greater(t, gate.ReloadCleared, 2*fixed.ReloadCleared, "the gate alone clears the burst, more slowly")

	// Today's path cuts queries off at their deadlines, so it opens connections all the
	// time, half of its attempts cancel requests; the gate's callers aren't cut off.
	perSecond, cancels, total := base.Connections.Attempts(0, loadEnd)
	require.Greater(t, perSecond, 5.0)
	require.InDelta(t, 0.5, float64(cancels)/float64(total), 0.1)
	for _, r := range []*Result{gated, both} {
		perSecond, _, _ := r.Connections.Attempts(0, loadEnd)
		require.Less(t, perSecond, 1.0, r.Scenario)
	}
}

// The local rig's overload, 24 tasks/s, about 130% of what 4 connections serve: today's
// path completed 244 of 3,599, the gate alone 2,764, and the gate and the pipelined read
// about 3,540.
func TestShortPoolOverload(t *testing.T) {
	t.Parallel()
	base := runScenario(t, "short-pool-overload", "base", Options{})
	gated := runScenario(t, "short-pool-overload", "gate", Options{})
	both := runScenario(t, "short-pool-overload", "pipeline-gate", Options{})

	today, gate, fixed := base.Hosts[0].Tasks, gated.Hosts[0].Tasks, both.Hosts[0].Tasks
	require.Less(t, today.Completed*10, today.Arrived)
	require.Greater(t, fixed.Completed*100, fixed.Arrived*95)
	require.Greater(t, gate.Completed, 3*today.Completed)
	require.Less(t, gate.Completed, fixed.Completed)

	_, _, todayAttempts := base.Connections.Attempts(0, base.Duration+base.Drain)
	_, _, gateAttempts := both.Connections.Attempts(0, both.Duration+both.Drain)
	require.Greater(t, todayAttempts, 50*gateAttempts)
}

// Before #107, the queues a host's start-up work made on its pool cut the capacity
// controller's rate at most host starts. QueuedGrace keeps the queued share from cutting
// for 30 s after the controller's first step.
func TestStartupQueuedGrace(t *testing.T) {
	t.Parallel()
	grace := runScenario(t, "startup", "grace", Options{})
	for _, h := range grace.Hosts {
		decisions := h.Decisions.Of(capacity.Name)
		require.NotEmpty(t, decisions)
		for _, d := range decisions {
			require.False(t, capacity.Reason(d.Reason).Overloaded(), "%s cut at %s: %s", h.Name, d.Time, d.Reason)
			require.InDelta(t, d.Static, d.Applied, 1e-9)
		}
	}

	noGrace := runScenario(t, "startup", "no-grace", Options{})
	for _, h := range noGrace.Hosts {
		cut := firstCut(h)
		require.NotNil(t, cut, h.Name)
		require.Equal(t, adaptive.Reason(capacity.ReasonQueued), cut.Reason)
		require.Less(t, cut.Time.Sub(epoch)-h.Started, 10*time.Second)
		decisions := h.Decisions.Of(capacity.Name)
		last := decisions[len(decisions)-1]
		require.InEpsilon(t, last.Static/2, last.Applied, 0.05, "%s holds the cut", h.Name)
	}
}

func firstCut(h HostResult) *adaptive.Decision {
	decisions := h.Decisions.Of(capacity.Name)
	for i, d := range decisions {
		if isCut(d.Reason) {
			return &decisions[i]
		}
	}
	return nil
}

// The hosts' traces replay through tools/adaptivereplay, each capacity step to the
// proposal and reason the simulation recorded.
func TestTracesReplay(t *testing.T) {
	t.Parallel()
	for _, run := range []struct{ scenario, variant string }{
		{"startup", "no-grace"},
		{"short-pool-overload", "pipeline-gate"},
		{"slow-store", "cap-tsr"},
		{"slow-store", "cap-wrk"},
		{"surge", "tsr"},
		{"busy-queue", "wrk"},
		{"memory-pressure", "mem"},
		{"partitions", "partitions"},
		{"partitions", "pollers"},
		{"uneven-frontends", "all"},
		{"ramp", "all"},
	} {
		t.Run(run.scenario+"/"+run.variant, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			requireTracesReplay(t, dir, runScenario(t, run.scenario, run.variant, Options{TraceDir: dir}))
		})
	}
}

// requireTracesReplay checks that the trace each host of a run wrote to dir replays every
// decision the host recorded.
func requireTracesReplay(t *testing.T, dir string, r *Result) {
	t.Helper()
	for _, h := range tracedHosts(r) {
		path := filepath.Join(dir, fmt.Sprintf("%s-%s-%d.jsonl", h.service, h.name, h.pid))
		f, err := os.Open(path)
		require.NoError(t, err)
		traces, err := adaptivereplay.Read(filepath.Base(path), f)
		require.NoError(t, f.Close())
		require.NoError(t, err)
		require.Len(t, traces, 1)
		require.Equal(t, h.name, traces[0].Header.Host)
		require.Positive(t, traces[0].Ticks)

		requireReplays(t, traces[0], h.decisions)
	}
}

// tracedHost is a host whose plane wrote a trace, and the decisions it recorded.
type tracedHost struct {
	service, name string
	pid           int
	decisions     Decisions
}

func tracedHosts(r *Result) []tracedHost {
	var hosts []tracedHost
	for i, h := range r.Hosts {
		hosts = append(hosts, tracedHost{service: historyService, name: h.Name, pid: i + 1, decisions: h.Decisions})
	}
	if m := r.Matching; m != nil {
		hosts = append(hosts, tracedHost{service: matchingService, name: m.Name, pid: m.PID, decisions: m.Decisions})
	}
	for _, f := range r.Frontends {
		hosts = append(hosts, tracedHost{service: frontendService, name: f.Name, pid: f.PID, decisions: f.Decisions})
	}
	return hosts
}

// requireReplays checks that a trace replays every decision a host recorded, each to the
// proposal and reason the simulation recorded.
func requireReplays(t *testing.T, trace *adaptivereplay.Trace, decisions Decisions) {
	t.Helper()
	replay, err := adaptivereplay.Run(trace, nil, nil, nil)
	require.NoError(t, err)
	require.Empty(t, replay.Unknown)
	steps := 0
	for _, series := range replay.Series {
		steps += len(series.Steps)
		require.Equal(t, len(series.Steps), series.Matched(), "%s %s %s", trace.Header.Host, series.Controller, series.Key)
	}
	require.Equal(t, len(decisions), steps, trace.Header.Host)
}

// The task scheduler rate steps on the capacity controller's rate each second, as the
// appreciation found, unsmoothed: each coupled step reads the rate of capacity's latest
// decision, and the task rate falls within a few steps of capacity's first cut.
func TestTaskRateFollowsCapacity(t *testing.T) {
	t.Parallel()
	for _, variant := range []string{"cap", "cap-tsr"} {
		r := runScenario(t, "slow-store", variant, Options{})
		h := r.Hosts[0]
		rates := h.Decisions.Of(taskrate.Name)
		require.NotEmpty(t, rates)

		// The host's decisions are in the order the plane recorded them, so the latest
		// capacity decision before a task rate step is the one its snapshot carried.
		var latest *adaptive.Decision
		for i, d := range h.Decisions {
			if d.Controller == capacity.Name {
				latest = &h.Decisions[i]
				continue
			}
			if d.Controller != taskrate.Name {
				continue
			}
			require.GreaterOrEqual(t, d.Proposed, 0.02*d.Static, variant)
			require.LessOrEqual(t, d.Proposed, d.Static, variant)
			if d.Reason != adaptive.Reason(taskrate.ReasonCoupled) {
				continue
			}
			detail := d.Detail.(taskrate.Detail)
			require.NotNil(t, latest)
			require.InDelta(t, latest.Applied, detail.Signals.PersistenceRate, 1e-9, "%s at %s", variant, d.Time)
		}
		cut := firstCut(h)
		require.NotNil(t, cut, variant)
		before := proposedAt(rates, cut.Time)
		after := proposedAt(rates, cut.Time.Add(3*time.Second))
		require.Less(t, after, before, "%s: the task rate falls within 3 s of the cut at %s", variant, cut.Time)
	}
}

// appliedAt is the applied value of the latest decision at or before at after the run's
// start.
func appliedAt(ds Decisions, at time.Duration) float64 {
	applied := ds[0].Applied
	for _, d := range ds {
		if d.Time.Sub(epoch) > at {
			break
		}
		applied = d.Applied
	}
	return applied
}

// proposedAt is the proposal of the latest decision at or before at.
func proposedAt(ds Decisions, at time.Time) float64 {
	proposed := ds[0].Proposed
	for _, d := range ds {
		if d.Time.After(at) {
			break
		}
		proposed = d.Proposed
	}
	return proposed
}

// Through a surge past the static persistence rate, the task scheduler rate holds the
// tasks over its rate in the scheduler: the persistence rate limiter refuses almost
// nothing, the workflows' calls keep their share, and the backlog clears.
func TestTaskRateHoldsTasksInTheScheduler(t *testing.T) {
	t.Parallel()
	enabled := runScenario(t, "surge", "tsr", Options{})
	shadow := runScenario(t, "surge", "shadow", Options{})
	held, unheld := enabled.Hosts[0], shadow.Hosts[0]

	for _, h := range []HostResult{held, unheld} {
		require.Equal(t, h.Tasks.Arrived, h.Tasks.Completed, "the backlog clears")
		for _, d := range h.Decisions.Of(taskrate.Name) {
			require.GreaterOrEqual(t, d.Proposed, 0.02*d.Static)
			require.LessOrEqual(t, d.Proposed, d.Static)
		}
	}
	require.Positive(t, held.Tasks.Throttled)
	limited := func(h HostResult) int { return h.Reads.Limited + h.Writes.Limited }
	require.Less(t, limited(held)*20, limited(unheld), "the persistence rate limiter refuses far fewer calls")
	require.Less(t, held.Tasks.Attempts, unheld.Tasks.Attempts)
	require.Less(t, held.Tasks.Backlog, unheld.Tasks.Backlog)
	require.Less(t, held.Workflows.P99*10, unheld.Workflows.P99, "the workflows' calls keep their share")
}

// runCoupled runs a scenario's variant with the capacity controller's CoupledBindingShare
// at share.
func runCoupled(t *testing.T, name, variant string, share float64, opts Options) HostResult {
	t.Helper()
	sc, err := Lookup(name, variant)
	require.NoError(t, err)
	sc.Host.Capacity.Params.Capacity.CoupledBindingShare = share
	r, err := Run(sc, opts)
	require.NoError(t, err)
	require.Len(t, r.Hosts, 1)
	if opts.TraceDir != "" {
		requireTracesReplay(t, opts.TraceDir, r)
	}
	return r.Hosts[0]
}

// capacityDetails are a host's capacity decisions' details.
func capacityDetails(h HostResult) []capacity.Detail {
	var details []capacity.Detail
	for _, d := range h.Decisions.Of(capacity.Name) {
		details = append(details, d.Detail.(capacity.Detail))
	}
	return details
}

// With both enabled in the slow store, the task scheduler rate keeps persistence's calls at
// its Headroom of capacity's rate, so once the store recovers the persistence limit doesn't
// bind and capacity holds its cut as idle while tasks wait in the scheduler. With
// CoupledBindingShare at 0.9, the scheduler's refusals bind for capacity, which returns to
// what the store delivered and probes up: far fewer tasks are waiting when the load ends,
// and nearly all of them complete by the run's end. The task rate still follows capacity's
// cuts, and the trace, with the scheduler's signals, replays.
func TestCoupledRefusalsLetCapacityRecoverAfterTheSlowStore(t *testing.T) {
	t.Parallel()
	held := runCoupled(t, "slow-store", "cap-tsr", 0.9, Options{TraceDir: t.TempDir()})
	latched := runCoupled(t, "slow-store", "cap-tsr", 0, Options{})

	require.Less(t, held.Tasks.Backlog*4, latched.Tasks.Backlog, "far fewer tasks are waiting when the load ends")
	// A few tasks that failed many times can still be in their reschedule backoff when the
	// run ends.
	require.Less(t, held.Tasks.Arrived-held.Tasks.Completed, held.Tasks.Arrived/1000, "nearly all of the backlog clears")
	require.Less(t, latched.Tasks.Completed, latched.Tasks.Arrived)

	// After the store recovers at 150 s, the persistence limiter denies nothing while
	// capacity recovers and probes: the scheduler's refusals bind.
	recovered := 0
	for _, d := range capacityDetails(held) {
		reason := d.Decision.Reason
		if d.Signals.Now.Sub(epoch) < 150*time.Second || reason != capacity.ReasonRecover && reason != capacity.ReasonProbe {
			continue
		}
		if !d.Signals.Binding {
			require.Positive(t, d.Signals.CoupledRefusals, "at %s", d.Signals.Now.Sub(epoch))
			recovered++
		}
	}
	require.Positive(t, recovered)

	cut := firstCut(held)
	require.NotNil(t, cut)
	rates := held.Decisions.Of(taskrate.Name)
	require.Less(t, proposedAt(rates, cut.Time.Add(3*time.Second)), proposedAt(rates, cut.Time), "the task rate follows the cut")
}

// With CoupledRetry at 200ms as well, the rescheduler retries a task the coupled limit
// refused while its buckets refill, instead of 1–3 s later, so the waiting tasks reach the
// scheduler as the rate allows rather than in batches. Few tasks are waiting when the load
// ends, every task completes, and the trace replays.
func TestCoupledRetryClearsTheSlowStoresBacklog(t *testing.T) {
	t.Parallel()
	sc, err := Lookup("slow-store", "cap-tsr")
	require.NoError(t, err)
	sc.Host.Capacity.Params.Capacity.CoupledBindingShare = 0.9
	sc.Host.Controllers.TaskRate.CoupledRetry = 200 * time.Millisecond
	dir := t.TempDir()
	r, err := Run(sc, Options{TraceDir: dir})
	require.NoError(t, err)
	requireTracesReplay(t, dir, r)
	h := r.Hosts[0]

	require.Less(t, h.Tasks.Backlog, 100, "few tasks are waiting when the load ends")
	require.Equal(t, h.Tasks.Arrived, h.Tasks.Completed, "the backlog clears")
}

// In the mixed surge, the task scheduler rate's step limit lags a surge of tasks, so its
// coupled limit refuses tasks while the calls stay far below capacity's rate. The calls
// offered don't reach CoupledBindingShare of what the scheduler lets through, so capacity
// stays at its static rate.
func TestCoupledRefusalsDontBindWhileTheCallsStayBelowTheirShare(t *testing.T) {
	t.Parallel()
	h := runCoupled(t, "mixed-surge", "cap-tsr", 0.9, Options{})
	refused := 0
	for _, d := range h.Decisions.Of(capacity.Name) {
		require.LessOrEqual(t, d.Proposed, d.Static, "at %s", d.Time.Sub(epoch))
		if d.Detail.(capacity.Detail).Signals.CoupledRefusals > 0 {
			refused++
		}
	}
	require.Positive(t, refused, "the scheduler refused tasks")
	require.Equal(t, h.Tasks.Arrived, h.Tasks.Completed)
}

// Through a surge with both enabled, the scheduler's refusals let capacity probe while the
// limiter denies nothing, and capacity stays below its ceiling: only the coupled limit's
// refusals below the static rate count, not the scheduler's own limit's.
func TestCoupledRefusalsKeepCapacityBelowItsCeilingThroughASurge(t *testing.T) {
	t.Parallel()
	h := runCoupled(t, "surge", "cap-tsr", 0.9, Options{})
	probes := 0
	for _, d := range capacityDetails(h) {
		require.Less(t, d.Decision.Proposed, d.Envelope.MaxRPS, "at %s", d.Signals.Now.Sub(epoch))
		if d.Decision.Reason == capacity.ReasonProbe && !d.Signals.Binding {
			require.Positive(t, d.Signals.CoupledRefusals)
			probes++
		}
	}
	require.Positive(t, probes, "the scheduler's refusals bind")
	require.Equal(t, h.Tasks.Arrived, h.Tasks.Completed, "the backlog clears")
}

// While its tasks wait longer than the target, a queue's worker count grows, to at most
// twice its setting; while they wait less than a quarter of it, the count shrinks a step
// every IdleSteps, to half. The appreciation's queues never bound, so their counts only
// shrank.
func TestWorkerCountsGrowAndShrink(t *testing.T) {
	t.Parallel()
	enabled := runScenario(t, "busy-queue", "wrk", Options{})
	shadow := runScenario(t, "busy-queue", "shadow", Options{})
	h := enabled.Hosts[0]
	const static = 8

	transfer := h.Decisions.Of(workercount.NamePrefix + transferQueue)
	require.NotEmpty(t, transfer)
	grown, peak := false, 0.0
	for _, d := range transfer {
		require.GreaterOrEqual(t, d.Applied, static/2.0)
		require.LessOrEqual(t, d.Applied, 2.0*static)
		peak = max(peak, d.Applied)
		switch workercount.Reason(d.Reason) {
		case workercount.ReasonGrow:
			require.Less(t, d.Time.Sub(epoch), 90*time.Second, "it grows while the queue is busy")
			grown = true
		case workercount.ReasonShrink:
			require.True(t, grown)
		default:
		}
	}
	require.InDelta(t, 2*static, peak, 1e-9)
	require.Greater(t, appliedAt(transfer, 89*time.Second), float64(static), "the pool stays grown while the queue is busy")
	require.InDelta(t, static/2.0, transfer[len(transfer)-1].Applied, 1e-9)

	timer := h.Decisions.Of(workercount.NamePrefix + timerQueue)
	require.InDelta(t, static/2.0, timer[len(timer)-1].Applied, 1e-9, "the idle timer queue shrinks too")

	// The grown pool serves the busy phase that the static one falls behind in.
	require.Less(t, h.Tasks.P99*10, shadow.Hosts[0].Tasks.P99)
	require.Equal(t, shadow.Hosts[0].Tasks.Completed, h.Tasks.Completed)
}

// While persistence backs off, as after each capacity cut, the worker counts hold, or
// return to their setting, and never grow.
func TestWorkerCountsBackOffAfterCapacityCuts(t *testing.T) {
	t.Parallel()
	r := runScenario(t, "slow-store", "cap-wrk", Options{})
	h := r.Hosts[0]
	cuts := 0
	var cut *adaptive.Decision
	for i, d := range h.Decisions {
		switch {
		case d.Controller == capacity.Name && isCut(d.Reason):
			cut = &h.Decisions[i]
			cuts++
		case d.Controller == workercount.NamePrefix+transferQueue:
			require.GreaterOrEqual(t, d.Applied, d.Static/2)
			require.LessOrEqual(t, d.Applied, 2*d.Static)
			if cut != nil {
				require.Equal(t, adaptive.Reason(workercount.ReasonBackoff), d.Reason, "after the cut at %s", cut.Time)
				cut = nil
			}
			require.NotEqual(t, adaptive.Reason(workercount.ReasonGrow), d.Reason)
		default:
		}
	}
	require.Positive(t, cuts)
}

// The memory budget brings the cache and the pending tasks to their budgets in their first
// seconds, a step of MaxStepFrac at a time, as the appreciation found. While another part
// of the process presses the heap on GOMEMLIMIT, both shrink, the pending tasks to the
// floor that keeps each queue's critical count at 1,250; once it has gone, both grow back
// to their budgets. The heap stays under the limit, where in shadow the cache's setting
// takes it past.
func TestMemoryBudgetShrinksUnderPressureAndRecovers(t *testing.T) {
	t.Parallel()
	enabled := runScenario(t, "memory-pressure", "mem", Options{})
	shadow := runScenario(t, "memory-pressure", "shadow", Options{})
	sc, err := Lookup("memory-pressure", "mem")
	require.NoError(t, err)
	h, settings := enabled.Hosts[0], memory.DefaultSettings
	pressed, released := sc.Host.Memory.Extra.At, sc.Host.Memory.Extra.At+sc.Host.Memory.Extra.Duration

	cache := h.Decisions.Of(memory.NamePrefix + memory.HistoryCache)
	pending := h.Decisions.Of(memory.NamePrefix + memory.PendingTasks)
	cacheBudget := float64(int(settings.HistoryCacheShare * float64(sc.Host.Memory.Limit)))
	pendingBudget := appliedAt(pending, time.Minute)
	floor := 1250 / float64(sc.Host.History.CriticalPendingTasks)
	require.InDelta(t, cacheBudget, appliedAt(cache, time.Minute), 1)
	require.Greater(t, pendingBudget, floor)
	require.Less(t, pendingBudget, 1.0)

	for _, series := range []struct {
		ds       Decisions
		min, max float64
		budget   float64
	}{
		{cache, settings.MinMultiplier * float64(sc.Host.History.CacheMaxBytes), float64(sc.Host.History.CacheMaxBytes), cacheBudget},
		{pending, floor, 1, pendingBudget},
	} {
		steps, prev := 0, series.max
		var shrunk, grown bool
		for _, d := range series.ds {
			require.GreaterOrEqual(t, d.Proposed, series.min-1e-9)
			require.LessOrEqual(t, d.Proposed, series.max)
			require.InDelta(t, d.Proposed, d.Applied, 1e-9)
			at := d.Time.Sub(epoch)
			if at <= 10*time.Second && d.Applied < prev {
				require.GreaterOrEqual(t, d.Applied, (1-settings.MaxStepFrac)*prev*(1-1e-9), "it moves to its budget a step at a time")
				steps++
			}
			prev = d.Applied
			switch memory.Reason(d.Reason) {
			case memory.ReasonPressure:
				require.GreaterOrEqual(t, at, pressed)
				require.Less(t, at, released)
				shrunk = true
			case memory.ReasonHeadroom:
				require.GreaterOrEqual(t, at, released, "it grows back once the heap has room")
				require.True(t, shrunk)
				grown = true
			default:
			}
		}
		require.GreaterOrEqual(t, steps, 3)
		require.True(t, grown)
		require.Less(t, appliedAt(series.ds, released), series.budget)
		require.InDelta(t, series.budget, series.ds[len(series.ds)-1].Applied, 1e-9, "it recovers its budget")
	}
	lowest := pendingBudget
	for _, d := range pending {
		lowest = min(lowest, d.Applied)
	}
	require.InDelta(t, floor, lowest, 1e-9, "the pending tasks shrink to the critical count's floor")
	maxHeld := sc.Host.History.Shards * int(math.Round(pendingBudget*float64(sc.Host.History.MaxPendingTasks)))
	require.LessOrEqual(t, h.Memory.PeakLoaded, maxHeld, "the queues hold no more than their budget")

	off := shadow.Hosts[0]
	for _, d := range off.Decisions {
		require.InDelta(t, d.Static, d.Applied, 1e-9, "in shadow each value keeps its setting")
	}
	require.Zero(t, h.Memory.OverLimit)
	require.Positive(t, off.Memory.OverLimit)
	require.Less(t, h.Memory.PeakLive, off.Memory.PeakLive)
	require.Less(t, h.Memory.Pressed, off.Memory.Pressed)
	require.Less(t, h.Memory.PeakLoaded, off.Memory.PeakLoaded)
	// At the same goodput.
	require.Equal(t, h.Tasks.Arrived, h.Tasks.Completed)
	require.Equal(t, off.Tasks.Completed, h.Tasks.Completed)
	require.Equal(t, off.Workflows.Completed, h.Workflows.Completed)
}

func TestRunsAreDeterministic(t *testing.T) {
	t.Parallel()
	first := runScenario(t, "short-pool-burst", "base", Options{})
	second := runScenario(t, "short-pool-burst", "base", Options{})
	require.Equal(t, first, second)

	sc, err := Lookup("short-pool-burst", "base")
	require.NoError(t, err)
	sc.Seed = 2
	other, err := Run(sc, Options{})
	require.NoError(t, err)
	require.NotEqual(t, first.Hosts[0].Tasks, other.Hosts[0].Tasks)
}

func TestWorkflowsComplete(t *testing.T) {
	t.Parallel()
	r := runScenario(t, "workflows", "", Options{})
	for _, h := range r.Hosts {
		require.Equal(t, 1200, h.Workflows.Started)
		require.Equal(t, h.Workflows.Started, h.Workflows.Completed)
		require.Zero(t, h.Store.Refused)
	}
}

func TestTool(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	require.NoError(t, run([]string{"-list"}, &out))
	for _, name := range Names() {
		require.Contains(t, out.String(), name+":")
	}

	out.Reset()
	dir := t.TempDir()
	require.NoError(t, run([]string{"-scenario", "short-pool-burst", "-variant", "pipeline-gate", "-capacity", "enabled", "-queued-grace", "0s", "-coupled-binding-share", "0.9", "-trace", dir}, &out))
	require.Contains(t, out.String(), "Connections:")
	require.Contains(t, out.String(), "enabled")
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	require.NoError(t, err)
	require.Len(t, files, 1)

	// -coupled-binding-share sets the capacity controller's share; left out, the scenario's stays.
	sc, err := Lookup("slow-store", "cap-tsr")
	require.NoError(t, err)
	require.NoError(t, overrides{grace: -1, coupledShare: 0.9}.apply(&sc))
	require.InDelta(t, 0.9, sc.Host.Capacity.Params.Capacity.CoupledBindingShare, 0)
	require.NoError(t, overrides{grace: -1, coupledShare: -1}.apply(&sc))
	require.InDelta(t, 0.9, sc.Host.Capacity.Params.Capacity.CoupledBindingShare, 0)

	require.ErrorContains(t, run([]string{"-scenario", "nonesuch"}, &out), "no scenario")
	require.ErrorContains(t, run([]string{"-scenario", "startup", "-variant", "nonesuch"}, &out), "no variant")
	require.ErrorContains(t, run([]string{"-scenario", "startup", "-capacity", "on"}, &out), "capacity mode")
	require.ErrorContains(t, run([]string{"-scenario", "startup", "-task-rate", "on"}, &out), "task-rate mode")
	require.ErrorContains(t, run([]string{"-scenario", "startup", "-workers", "on"}, &out), "workers mode")
	require.ErrorContains(t, run([]string{"-scenario", "startup", "-memory", "on"}, &out), "memory mode")
	require.ErrorContains(t, run([]string{"-scenario", "partitions", "-partitions", "on"}, &out), "partitions mode")
	require.ErrorContains(t, run([]string{"-scenario", "startup", "-partitions", "enabled"}, &out), "through matching")
	require.ErrorContains(t, run([]string{"-scenario", "uneven-frontends", "-fair-share", "on"}, &out), "fair-share mode")
	require.ErrorContains(t, run([]string{"-scenario", "startup", "-fair-share", "enabled"}, &out), "has no frontends")

	out.Reset()
	require.NoError(t, run([]string{"-scenario", "uneven-frontends", "-variant", "shadow", "-fair-share", "enabled"}, &out))
	require.Regexp(t, `frontend-1 +fair_share execution/uneven +enabled`, out.String())
}

// partitionDecisions are the matching host's partition scaler decisions for the workflow
// task queue, with their details.
func partitionDecisions(t *testing.T, r *Result) ([]adaptive.Decision, []partitions.Detail) {
	t.Helper()
	require.NotNil(t, r.Matching)
	var ds []adaptive.Decision
	var details []partitions.Detail
	for _, d := range r.Matching.Decisions.Of(partitions.Name) {
		detail, ok := d.Detail.(partitions.Detail)
		require.True(t, ok)
		if detail.TaskQueueType == "Workflow" {
			ds = append(ds, d)
			details = append(details, detail)
		}
	}
	require.NotEmpty(t, ds)
	return ds, details
}

// firstTrial is the index of the first decision that starts a trial: one more partition
// than the step saw, for the reason trial.
func firstTrial(t *testing.T, ds []adaptive.Decision, details []partitions.Detail) int {
	t.Helper()
	for i, d := range ds {
		if d.Reason == adaptive.Reason(partitions.ReasonTrial) && d.Proposed > float64(details[i].Signals.Partitions) {
			return i
		}
	}
	require.Fail(t, "no trial")
	return -1
}

// A workflow task queue whose partitions each dispatch at most 15 tasks a second, while
// 80 a second arrive: the scaler's trial adds a partition, dispatch rises with it, so the
// trial measures each partition's capacity as 15 a second, and the partitions grow to
// dispatch the add rate. In shadow the partitions stay at 4 and the backlog grows.
func TestPartitionScalerKeepsATrialThatRaisesDispatch(t *testing.T) {
	t.Parallel()
	r := runScenario(t, "partitions", "partitions", Options{})
	sc, err := Lookup("partitions", "partitions")
	require.NoError(t, err)
	settings := sc.Matching.Scaler
	ds, details := partitionDecisions(t, r)

	i := firstTrial(t, ds, details)
	trial, at := ds[i], details[i].Signals
	require.Equal(t, 4, at.Partitions)
	require.InDelta(t, trial.Proposed, trial.Applied, 0)
	require.GreaterOrEqual(t, at.AddRate, settings.MinRate)
	require.Greater(t, at.AddRate, at.DispatchRate*(1+settings.BehindRatio))

	learned := -1
	for j := i + 1; j < len(ds); j++ {
		require.NotEqual(t, adaptive.Reason(partitions.ReasonNoGain), ds[j].Reason)
		if learned < 0 && ds[j].Reason == adaptive.Reason(partitions.ReasonLearned) {
			learned = j
		}
	}
	require.Positive(t, learned, "the trial raised dispatch")
	require.GreaterOrEqual(t, ds[learned].Time.Sub(trial.Time), settings.TrialWindow)
	require.InDelta(t, sc.Matching.PartitionRate, details[learned].Capacity, 0.01)
	require.Greater(t, ds[learned].Applied, trial.Applied)

	q := r.Matching.Queues[0]
	require.GreaterOrEqual(t, q.Read, 6)
	require.Equal(t, q.Added, q.Dispatched)
	wf := r.Hosts[0].Workflows
	require.Equal(t, wf.Started, wf.Completed)

	shadow := runScenario(t, "partitions", "shadow", Options{})
	ds, details = partitionDecisions(t, shadow)
	firstTrial(t, ds, details) // proposed, not applied
	for _, d := range ds {
		require.InDelta(t, d.Static, d.Applied, 0)
	}
	require.Equal(t, 4, shadow.Matching.Queues[0].Read)
	require.Less(t, shadow.Hosts[0].Workflows.Completed, wf.Completed)
	require.Greater(t, shadow.Matching.Queues[0].PeakBacklog, q.PeakBacklog)
}

// The same add rate where 14 pollers hold dispatch back: the trial's partition raises
// nothing, so the scaler undoes it once its window has passed, keeps the capacity it
// started from, and runs no trial for NoGainHold.
func TestPartitionScalerUndoesATrialThatDoesNot(t *testing.T) {
	t.Parallel()
	r := runScenario(t, "partitions", "pollers", Options{})
	sc, err := Lookup("partitions", "pollers")
	require.NoError(t, err)
	settings := sc.Matching.Scaler
	ds, details := partitionDecisions(t, r)

	i := firstTrial(t, ds, details)
	trial := ds[i]
	require.InDelta(t, trial.Proposed, trial.Applied, 0)
	undone := -1
	for j := i + 1; j < len(ds); j++ {
		require.NotEqual(t, adaptive.Reason(partitions.ReasonLearned), ds[j].Reason)
		if undone < 0 && ds[j].Reason == adaptive.Reason(partitions.ReasonNoGain) {
			undone = j
		}
	}
	require.Positive(t, undone, "the trial was undone")
	require.GreaterOrEqual(t, ds[undone].Time.Sub(trial.Time), settings.TrialWindow)
	require.InDelta(t, float64(details[i].Signals.Partitions), ds[undone].Applied, 0)
	for j := undone; j < len(ds); j++ {
		require.InDelta(t, settings.InitialCapacity, details[j].Capacity, 0)
		require.LessOrEqual(t, ds[j].Proposed, float64(details[j].Signals.Partitions), "no trial within NoGainHold")
	}
	require.Equal(t, 4, r.Matching.Queues[0].Read)
}

// uneven is a frontend's requests in the uneven frontends' second namespace.
func uneven(t *testing.T, r *Result, frontend int) NamespaceResult {
	t.Helper()
	require.Len(t, r.Frontends, 2)
	n, ok := r.Frontends[frontend].Namespace(unevenNamespace)
	require.True(t, ok)
	return n
}

// admittedBetween is the requests the frontends admitted for a namespace, a second, over
// [from, to) after the run's start.
func admittedBetween(t *testing.T, r *Result, from, to time.Duration) float64 {
	t.Helper()
	admitted := 0
	for i := range r.Frontends {
		n := uneven(t, r, i)
		for s := int(from / time.Second); s < int(to/time.Second) && s < len(n.PerSecond); s++ {
			admitted += n.PerSecond[s].Admitted
		}
	}
	return float64(admitted) / (to - from).Seconds()
}

// The appreciation's uneven frontends, and its finding 4. Without fair share, each frontend
// takes half the namespace's limit, so the busier throttles while the quieter leaves part of
// its half unused. With it, the busier frontend's share rises and the quieter's falls, and
// once the namespace's limiters take the shares, at their refresh, its admitted requests
// rise to its limit. The shares add up to no more than the limit throughout: the frontends
// keep equal shares while their demand windows fill, and then a share that falls applies at
// once, and one that rises waits a round, or by less than the deadband holds below its
// proposal.
func TestFairShareLiftsTheBusierFrontend(t *testing.T) {
	t.Parallel()
	fair := runScenario(t, "uneven-frontends", "fair", Options{})
	shadow := runScenario(t, "uneven-frontends", "shadow", Options{})
	all := runScenario(t, "uneven-frontends", "all", Options{})
	sc, err := Lookup("uneven-frontends", "fair")
	require.NoError(t, err)
	requests := sc.Frontends.Requests[0]
	end := requests.At + requests.Duration
	limit := float64(sc.Frontends.Limits[unevenNamespace])
	settings := sc.Frontends.FairShare

	admitted, throttled := uneven(t, shadow, 0).Rates()
	require.InDelta(t, limit/2, admitted, 0.5)
	require.InDelta(t, requests.Rates[0]-limit/2, throttled, 0.5)
	admitted, _ = uneven(t, shadow, 1).Rates()
	require.InDelta(t, requests.Rates[1], admitted, 0.5)
	require.Zero(t, uneven(t, shadow, 1).Throttled)

	// The frontends' decisions in time order, and the share each applied after each.
	type step struct {
		frontend int
		d        adaptive.Decision
	}
	var steps []step
	for i, f := range fair.Frontends {
		for _, d := range f.Decisions.Of(fairshare.Name) {
			require.Equal(t, adaptive.ModeOn, d.Mode)
			steps = append(steps, step{frontend: i, d: d})
		}
	}
	slices.SortStableFunc(steps, func(a, b step) int { return a.d.Time.Compare(b.d.Time) })
	shares := []float64{limit / 2, limit / 2}
	filled := requests.At + settings.Window + 2*settings.Interval
	filling := 0
	for _, s := range steps {
		shares[s.frontend] = s.d.Applied
		at := s.d.Time.Sub(epoch)
		require.LessOrEqual(t, shares[0]+shares[1], limit+1e-9, "the shares at %s", at)
		if s.d.Reason == adaptive.Reason(fairshare.ReasonFilling) {
			require.Less(t, at, filled)
			require.InDelta(t, limit/2, s.d.Applied, 1e-9)
			filling++
		}
	}
	require.Positive(t, filling)

	// Once their windows have filled, each frontend applies the law's share of the demand:
	// the busier more than half the limit, the quieter less.
	applied := make([]float64, len(requests.Rates))
	for i, rate := range requests.Rates {
		for _, s := range steps {
			if s.frontend == i && s.d.Time.Sub(epoch) < end {
				applied[i] = s.d.Applied
			}
		}
		want, reason := fairshare.Share(settings.Params(), limit, fairshare.Demand{
			Own: rate, Peers: requests.Rates[1-i], Members: 2, Known: 1,
		})
		require.Equal(t, fairshare.ReasonOver, reason)
		require.InDelta(t, want, applied[i], 1e-9, "frontend %d", i+1)
	}
	require.Greater(t, applied[0], limit/2)
	require.Less(t, applied[1], limit/2)

	// The limiters take the shares at their refresh, a minute after the requests start; a
	// minute later, they've spent the burst they saved.
	from := requests.At + 2*limiterRefresh
	require.InDelta(t, limit, admittedBetween(t, fair, from, end), 1)
	require.InDelta(t, limit/2+requests.Rates[1], admittedBetween(t, shadow, from, end), 1)

	// With every controller enabled, fair share does the same.
	for i, f := range all.Frontends {
		require.Equal(t, uneven(t, fair, i), uneven(t, all, i), f.Name)
		require.Equal(t, fair.Frontends[i].Decisions.Of(fairshare.Name), f.Decisions.Of(fairshare.Name), f.Name)
	}

	// The workflows' starts go through the same frontends, in a namespace without a limit
	// across the cluster: none is throttled, and every workflow completes.
	for _, r := range []*Result{fair, shadow} {
		completed := 0
		for _, h := range r.Hosts {
			completed += h.Workflows.Completed
		}
		require.Equal(t, 1500, completed)
		for _, f := range r.Frontends {
			n, ok := f.Namespace(workflowNamespace)
			require.True(t, ok)
			require.Zero(t, n.Throttled)
		}
	}
}

// The appreciation's ramp, and its finding 5. At 5 and 10 workflows a second, workflows
// complete as fast as they start. At 20 and 30, history's shards spend nearly all their
// time writing, and a write waits over half a second for its shard; the workflow task
// pollers, each held while history records its task as started, are busy all the time.
// Goodput falls below the starts, and matching's backlog grows. No controller reads
// either bound, so with every controller enabled the ramp runs as with every one disabled.
func TestRampIsBoundByShardsAndPollers(t *testing.T) {
	t.Parallel()
	off := runScenario(t, "ramp", "off", Options{})
	all := runScenario(t, "ramp", "all", Options{})
	for _, r := range []*Result{off, all} {
		completed := 0
		for _, h := range r.Hosts {
			require.Equal(t, h.Workflows.Started, h.Workflows.Completed, h.Name)
			completed += h.Workflows.Completed
			require.Zero(t, h.Reads.Limited+h.Writes.Limited, h.Name)
			require.Zero(t, h.Reads.Exhausted+h.Writes.Exhausted, h.Name)
			require.Zero(t, h.Tasks.Throttled, h.Name)
		}
		require.Equal(t, 4874, completed)

		require.Len(t, r.Phases, 4)
		quiet := r.Phases[0]
		for _, ph := range r.Phases[:2] {
			require.GreaterOrEqual(t, ph.Completed, 0.9*ph.Rate, "at %v a second", ph.Rate)
			require.Less(t, ph.ShardWait, 50*time.Millisecond, "at %v a second", ph.Rate)
		}
		for _, ph := range r.Phases[2:] {
			require.Less(t, ph.Completed, 0.6*ph.Rate, "at %v a second", ph.Rate)
			require.Greater(t, ph.ShardsBusy, 0.95, "at %v a second", ph.Rate)
			require.Greater(t, ph.ShardWait, 500*time.Millisecond, "at %v a second", ph.Rate)
			require.Greater(t, ph.PollersBusy, 0.9, "at %v a second", ph.Rate)
			require.Greater(t, ph.PollHeld, 3*quiet.PollHeld, "at %v a second", ph.Rate)
		}
		require.Greater(t, r.Phases[3].Backlog, 1000)
	}

	for i, ph := range all.Phases {
		require.InEpsilon(t, off.Phases[i].Completed, ph.Completed, 0.05, "at %v a second", ph.Rate)
	}
	for i, h := range all.Hosts {
		was := off.Hosts[i].Workflows
		require.InEpsilon(t, was.P50.Seconds(), h.Workflows.P50.Seconds(), 0.05, h.Name)
		require.InEpsilon(t, was.P99.Seconds(), h.Workflows.P99.Seconds(), 0.05, h.Name)
		require.Equal(t, off.Hosts[i].Memory.PeakLive, h.Memory.PeakLive, h.Name)
	}

	// With every controller disabled and the plane off, none decides.
	for _, h := range off.Hosts {
		require.Empty(t, h.Decisions, h.Name)
	}
	require.Empty(t, off.Matching.Decisions)
	for _, f := range off.Frontends {
		require.Empty(t, f.Decisions, f.Name)
	}

	// With every one enabled, capacity never cuts, and the task rate holds no task (above).
	// The workflow task queue falls behind, but dispatches under the 50 a second the
	// partition scaler's trials need, so it runs none: the pollers, not the partitions,
	// hold the queue back. No namespace has a limit for fair share to divide.
	for _, h := range all.Hosts {
		require.Nil(t, firstCut(h), h.Name)
	}
	ds, details := partitionDecisions(t, all)
	noGain := 0
	for i, d := range ds {
		require.NotEqual(t, adaptive.Reason(partitions.ReasonTrial), d.Reason)
		if d.Reason == adaptive.Reason(partitions.ReasonNoGain) {
			noGain++
			require.GreaterOrEqual(t, details[i].Signals.AddRate, details[i].Params.MinRate)
			require.Less(t, details[i].Signals.DispatchRate, details[i].Params.MinRate)
		}
	}
	require.Positive(t, noGain)
	for _, f := range all.Frontends {
		require.Empty(t, f.Decisions.Of(fairshare.Name), f.Name)
	}
}

// #110's slow store, with every controller its host runs enabled: the capacity controller
// and the task scheduler rate hold the cut as they do in cap-tsr. Once the store has
// recovered, the task rate keeps persistence's calls under capacity's cut, so capacity
// holds it as idle, and the task rate stays under the tasks' rate, so tasks are still
// waiting when the load ends. The worker counts back off after each cut, and change
// nothing else.
func TestSlowStoreHoldsItsCutWithEveryController(t *testing.T) {
	t.Parallel()
	pair := runScenario(t, "slow-store", "cap-tsr", Options{})
	all := runScenario(t, "slow-store", "all", Options{})
	sc, err := Lookup("slow-store", "all")
	require.NoError(t, err)
	p, a := pair.Hosts[0], all.Hosts[0]
	require.Equal(t, p.Tasks, a.Tasks)
	require.Equal(t, p.Workflows, a.Workflows)
	require.Equal(t, p.Decisions.Of(capacity.Name), a.Decisions.Of(capacity.Name))
	require.Equal(t, p.Decisions.Of(taskrate.Name), a.Decisions.Of(taskrate.Name))
	require.NotEmpty(t, a.Decisions.Of(workercount.NamePrefix+transferQueue))

	recovered, end := sc.Slow.At+sc.Slow.Duration, sc.Duration
	arriving := sc.Load.Tasks[0].Rate
	held := 0
	for _, d := range a.Decisions {
		at := d.Time.Sub(epoch)
		if at < recovered || at >= end {
			continue
		}
		switch d.Controller {
		case capacity.Name:
			require.Equal(t, adaptive.Reason(capacity.ReasonIdle), d.Reason, "at %s", at)
			require.Less(t, d.Applied, d.Static, "at %s", at)
			held++
		case taskrate.Name:
			require.Less(t, d.Applied, arriving, "at %s", at)
		default:
		}
	}
	require.Positive(t, held)
	require.Positive(t, a.Tasks.Backlog)
}
