package adaptivesim

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
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
	for _, run := range []struct{ scenario, variant string }{
		{"startup", "no-grace"},
		{"short-pool-overload", "pipeline-gate"},
	} {
		dir := t.TempDir()
		r := runScenario(t, run.scenario, run.variant, Options{TraceDir: dir})
		for i, h := range r.Hosts {
			path := filepath.Join(dir, fmt.Sprintf("%s-%s-%d.jsonl", historyService, h.Name, i+1))
			f, err := os.Open(path)
			require.NoError(t, err)
			traces, err := adaptivereplay.Read(filepath.Base(path), f)
			require.NoError(t, f.Close())
			require.NoError(t, err)
			require.Len(t, traces, 1)
			require.Equal(t, h.Name, traces[0].Header.Host)
			require.Positive(t, traces[0].Ticks)

			requireReplays(t, traces[0], h.Decisions)
		}
	}
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

func TestRunsAreDeterministic(t *testing.T) {
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
	r := runScenario(t, "workflows", "", Options{})
	for _, h := range r.Hosts {
		require.Equal(t, 1200, h.Workflows.Started)
		require.Equal(t, h.Workflows.Started, h.Workflows.Completed)
		require.Zero(t, h.Store.Refused)
	}
}

func TestTool(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, run([]string{"-list"}, &out))
	for _, name := range Names() {
		require.Contains(t, out.String(), name+":")
	}

	out.Reset()
	dir := t.TempDir()
	require.NoError(t, run([]string{"-scenario", "short-pool-burst", "-variant", "pipeline-gate", "-capacity", "enabled", "-queued-grace", "0s", "-trace", dir}, &out))
	require.Contains(t, out.String(), "Connections:")
	require.Contains(t, out.String(), "enabled")
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	require.NoError(t, err)
	require.Len(t, files, 1)

	require.ErrorContains(t, run([]string{"-scenario", "nonesuch"}, &out), "no scenario")
	require.ErrorContains(t, run([]string{"-scenario", "startup", "-variant", "nonesuch"}, &out), "no variant")
	require.ErrorContains(t, run([]string{"-scenario", "startup", "-capacity", "on"}, &out), "capacity mode")
}
