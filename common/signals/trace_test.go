package signals

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/fx/fxtest"
)

const traceName = "history-history-1-42.jsonl"

// readTrace decodes a trace file's records, keeping each decision's detail as raw JSON.
func readTrace(t *testing.T, path string) []Record {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	var records []Record
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		var rec Record
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &rec), scanner.Text())
		records = append(records, rec)
	}
	require.NoError(t, scanner.Err())
	return records
}

func TestPlaneWritesSnapshotsAndDecisionsToATrace(t *testing.T) {
	f := newPlaneFixture(t, fakeRPC{meanMS: 20, p99MS: 85, errorRatio: 0.02, enabled: true, percentiles: true})
	f.settings.TraceDirectory = t.TempDir()

	f.plane.RecordDecision(adaptive.Decision{Controller: "persistence_capacity", Reason: "idle"}) // before the file opens
	first := f.tick()
	f.plane.RecordDecision(adaptive.Decision{
		Controller: "persistence_capacity",
		Time:       f.timeSource.Now(),
		Mode:       adaptive.ModeShadow,
		Reason:     "latency",
		Static:     1000,
		Proposed:   540,
		Applied:    1000,
		Detail:     struct{ P99 time.Duration }{P99: 200 * time.Millisecond},
	})
	f.tick()
	f.plane.Stop()

	records := readTrace(t, filepath.Join(f.settings.TraceDirectory, traceName))
	require.Len(t, records, 4)

	require.NotNil(t, records[0].Header)
	require.Equal(t, Header{Service: "history", Host: "history-1", PID: 42, Start: first.Time}, *records[0].Header)

	require.NotNil(t, records[1].Snapshot)
	require.Equal(t, first.Persistence, records[1].Snapshot.Persistence)
	require.Equal(t, first.RPC, records[1].Snapshot.RPC)
	require.Equal(t, first.Memory, records[1].Snapshot.Memory)
	require.Empty(t, records[1].Snapshot.Decisions, "decisions have their own lines")

	decision := records[2].Decision
	require.NotNil(t, decision)
	require.Equal(t, adaptive.ModeShadow, decision.Mode)
	require.Equal(t, adaptive.Reason("latency"), decision.Reason)
	require.InDelta(t, 540, decision.Proposed, 0.001)
	require.Equal(t, map[string]any{"P99": float64(200 * time.Millisecond)}, decision.Detail)

	require.NotNil(t, records[3].Snapshot)
}

func TestPlaneOpensItsTraceAsTheServiceCreatesIt(t *testing.T) {
	dir := t.TempDir()
	dc := dynamicconfig.NewMemoryClient()
	dc.OverrideSetting(Config, Settings{Enabled: true, Interval: time.Second, Windows: 3, TraceDirectory: dir, TraceMaxBytes: 1 << 20})
	timeSource := clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0).UTC())
	plane := PlaneProvider(PlaneParams{
		Lifecycle:   fxtest.NewLifecycle(t),
		Collection:  dynamicconfig.NewCollection(dc, log.NewNoopLogger()),
		ServiceName: primitives.HistoryService,
		TimeSource:  timeSource,
		Logger:      log.NewNoopLogger(),
		Persistence: persistence.NoopHealthSignalAggregator,
	})
	plane.RecordDecision(adaptive.Decision{Controller: "persistence_capacity", Reason: "idle"})
	plane.Stop()

	files, err := filepath.Glob(filepath.Join(dir, "history-*.jsonl"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	records := readTrace(t, files[0])
	require.Len(t, records, 2)
	require.NotNil(t, records[0].Header)
	require.True(t, timeSource.Now().Equal(records[0].Header.Start))
	require.NotNil(t, records[1].Decision, "a decision before the plane started")
}

func TestPlaneOpensItsTraceEarlyOnlyWhileEnabled(t *testing.T) {
	f := newPlaneFixture(t, nil)
	f.settings.Enabled = false
	f.settings.TraceDirectory = t.TempDir()
	f.plane.openTrace()
	f.plane.Stop()

	_, err := os.Stat(filepath.Join(f.settings.TraceDirectory, traceName))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestTraceStopsAtItsCap(t *testing.T) {
	f := newPlaneFixture(t, nil)
	f.settings.TraceDirectory = t.TempDir()
	f.settings.TraceMaxBytes = 4 << 10
	for range 100 {
		f.tick()
	}
	f.plane.Stop()

	info, err := os.Stat(filepath.Join(f.settings.TraceDirectory, traceName))
	require.NoError(t, err)
	require.LessOrEqual(t, info.Size(), f.settings.TraceMaxBytes)
	require.Greater(t, info.Size(), f.settings.TraceMaxBytes/2)
}

func TestTraceFollowsTheDirectorySetting(t *testing.T) {
	f := newPlaneFixture(t, nil)
	first, second := t.TempDir(), t.TempDir()
	f.settings.TraceDirectory = first
	f.tick()
	f.settings.TraceDirectory = second
	f.tick()
	f.tick()
	f.settings.TraceDirectory = ""
	f.tick()

	require.Len(t, readTrace(t, filepath.Join(first, traceName)), 2, "a header and one snapshot")
	require.Len(t, readTrace(t, filepath.Join(second, traceName)), 3, "a header and two snapshots")
}

func TestPlaneRunsOnWhenItsTraceCantBeOpened(t *testing.T) {
	f := newPlaneFixture(t, nil)
	f.settings.TraceDirectory = filepath.Join(t.TempDir(), "missing")
	require.NotNil(t, f.tick())
	require.NotNil(t, f.tick())
	require.NoDirExists(t, f.settings.TraceDirectory)
}
