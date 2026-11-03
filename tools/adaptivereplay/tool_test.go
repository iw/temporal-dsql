package adaptivereplay

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
)

// traceFile writes the capacity recording, and a decision of a controller the tool
// doesn't know, to a trace file.
func traceFile(t *testing.T) string {
	t.Helper()
	rec := capacityRecording(t)
	rec.decision(t, adaptive.Decision{Controller: "something_new", Time: start, Static: 1, Proposed: 1})
	path := filepath.Join(t.TempDir(), "history.jsonl")
	require.NoError(t, os.WriteFile(path, rec.buf.Bytes(), 0o600))
	return path
}

func TestReportsEachSeries(t *testing.T) {
	path := traceFile(t)
	var out bytes.Buffer
	require.NoError(t, run([]string{path}, &out))

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 5)
	require.Equal(t, "history.jsonl: history on host, pid 1, 2023-11-14T22:13:20Z for 1m59s, 0 snapshots, 116 decisions", lines[0])
	require.Empty(t, lines[1])
	require.Equal(t, []string{"CONTROLLER", "KEY", "STEPS", "MATCHED", "CHANGES", "REVERSALS", "MIN", "MAX", "FINAL", "REASONS"}, strings.Fields(lines[2]))
	require.Equal(t, []string{"persistence_capacity", "-", "115", "115"}, strings.Fields(lines[3])[:4])
	require.Contains(t, lines[3], "frozen 5")
	require.Equal(t, []string{"something_new", "-", "1", "not", "replayed"}, strings.Fields(lines[4]))
}

func TestReportsRecordedAndReplayedWithOverrides(t *testing.T) {
	path := traceFile(t)
	var out bytes.Buffer
	require.NoError(t, run([]string{"-set", "persistence_capacity.MaxErrorRatio=0.3", path}, &out))

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 6)
	require.Contains(t, strings.Fields(lines[2]), "PROPOSALS")
	replayed, recorded := strings.Fields(lines[3]), strings.Fields(lines[4])
	require.Equal(t, "replayed", replayed[4])
	require.NotEqual(t, "115", replayed[3], "it no longer cuts on errors")
	require.Equal(t, "recorded", recorded[0])
	require.Contains(t, lines[4], "errors")
	require.NotContains(t, lines[3], "errors")
}

func TestWritesEachStepToCSV(t *testing.T) {
	path := traceFile(t)
	csvPath := filepath.Join(t.TempDir(), "steps.csv")
	require.NoError(t, run([]string{"-csv", csvPath, path}, &bytes.Buffer{}))

	f, err := os.Open(csvPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	rows, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 116, "a header and each of the capacity controller's steps")
	require.Equal(t, []string{"file", "service", "pid", "controller", "key", "time", "static", "recorded", "replayed", "recorded_reason", "replayed_reason", "stepped"}, rows[0])
	require.Equal(t, []string{"history.jsonl", "history", "1", "persistence_capacity", "", "2023-11-14T22:13:20Z", "1000"}, rows[1][:7])
	require.Equal(t, rows[1][7], rows[1][8])
	require.Equal(t, "true", rows[1][11])
	frozen := rows[56]
	require.Equal(t, []string{"frozen", "frozen", "false"}, frozen[9:])
}

func TestReplaysOnlyTheControllersAsked(t *testing.T) {
	path := traceFile(t)
	var out bytes.Buffer
	require.NoError(t, run([]string{"-controller", "worker_count", path}, &out))
	require.Equal(t, "history.jsonl: history on host, pid 1, 2023-11-14T22:13:20Z for 1m59s, 0 snapshots, 116 decisions\n", out.String(), "no table without series")

	err := run([]string{"-controller", "worker_count", "-set", "persistence_capacity.MaxErrorRatio=0.3", path}, &out)
	require.EqualError(t, err, "persistence_capacity.MaxErrorRatio=0.3: no controller in the traces has it")
}

func TestRejectsWhatItCantRun(t *testing.T) {
	path := traceFile(t)
	var out bytes.Buffer
	require.ErrorIs(t, run(nil, &out), errNoTraces)
	require.Contains(t, out.String(), "Usage: adaptivereplay")

	require.EqualError(t, run([]string{"-set", "worker_count.TargetDelay=1s", path}, &out), "worker_count.TargetDelay=1s: no controller in the traces has it")
	require.ErrorContains(t, run([]string{"-set", "TargetDelay=1s", path}, &out), errOverrideSyntax.Error())
	require.ErrorContains(t, run([]string{"-set", "persistence_capacity.Speed=1", path}, &out), "persistence_capacity has no parameter Speed")
	require.ErrorIs(t, run([]string{filepath.Join(t.TempDir(), "missing.jsonl")}, &out), os.ErrNotExist)

	out.Reset()
	require.NoError(t, run([]string{"-h"}, &out))
	require.Contains(t, out.String(), "-set controller.Field=value")
}
