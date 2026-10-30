package signals

import (
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
)

type fakePersistence struct {
	snapshot persistence.HealthSnapshot
	windows  int
}

func (f *fakePersistence) RecentSnapshot(windows int) persistence.HealthSnapshot {
	f.windows = windows
	return f.snapshot
}

type fakeRPC struct {
	meanMS, p99MS, errorRatio float64
	enabled, percentiles      bool
}

func (f fakeRPC) AverageLatency() float64                 { return f.meanMS }
func (f fakeRPC) LatencyQuantile(float64) (float64, bool) { return f.p99MS, f.percentiles }
func (f fakeRPC) ErrorRatio() (float64, bool)             { return f.errorRatio, f.enabled }

type planeFixture struct {
	plane       *Plane
	settings    Settings
	persistence *fakePersistence
	timeSource  *clock.EventTimeSource
}

func newPlaneFixture(t *testing.T, rpc RPCHealth) *planeFixture {
	t.Helper()
	f := &planeFixture{
		settings:    DefaultSettings,
		persistence: &fakePersistence{snapshot: persistence.HealthSnapshot{P99: 12 * time.Millisecond, Samples: 500, ScheduleP90: 40 * time.Millisecond}},
		timeSource:  clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0).UTC()),
	}
	f.settings.Enabled = true
	cgroup := fstest.MapFS{
		"proc/self/cgroup":             {Data: []byte("0::/\n")},
		"sys/fs/cgroup/memory.max":     {Data: []byte("1073741824\n")},
		"sys/fs/cgroup/memory.current": {Data: []byte("268435456\n")},
	}
	f.plane = newPlane(
		func() Settings { return f.settings },
		f.persistence,
		rpc,
		newCgroupMemory(cgroup),
		f.timeSource,
		Header{Service: "history", Host: "history-1", PID: 42},
		log.NewNoopLogger(),
	)
	t.Cleanup(f.plane.Stop)
	return f
}

func (f *planeFixture) tick() *Snapshot {
	f.timeSource.Advance(f.settings.Interval)
	f.plane.Tick(f.timeSource.Now())
	return f.plane.Latest()
}

func TestPlaneIsOffByDefault(t *testing.T) {
	f := newPlaneFixture(t, nil)
	f.settings = DefaultSettings
	called := false
	f.plane.Subscribe(func(*Snapshot) { called = true }, nil)

	require.Nil(t, f.tick())
	require.False(t, called)
	require.Zero(t, f.persistence.windows, "a disabled plane reads nothing")
}

func TestPlaneSnapshotsTheHost(t *testing.T) {
	f := newPlaneFixture(t, fakeRPC{meanMS: 20, p99MS: 85, errorRatio: 0.02, enabled: true, percentiles: true})
	f.settings.Windows = 5
	var published *Snapshot
	f.plane.Subscribe(func(s *Snapshot) { published = s }, nil)

	snap := f.tick()
	require.NotNil(t, snap)
	require.Same(t, snap, published)
	require.Equal(t, f.timeSource.Now(), snap.Time)

	require.Equal(t, 5, f.persistence.windows)
	require.Equal(t, f.persistence.snapshot, snap.Persistence)
	require.Equal(t, RPC{Reported: true, Mean: 20 * time.Millisecond, ErrorRatio: 0.02, P99: 85 * time.Millisecond}, snap.RPC)
	require.Equal(t, uint64(1<<30), snap.Memory.Limit)
	require.Equal(t, uint64(1<<28), snap.Memory.Usage)
	require.Positive(t, snap.Runtime.Goroutines)
	require.Positive(t, snap.Runtime.HeapGoal)
	require.Zero(t, snap.Runtime.SchedSamples, "the first snapshot sets the scheduling latency baseline")
}

func TestPlaneReportsRPCHealthWhereThereIsSome(t *testing.T) {
	require.Equal(t, RPC{}, newPlaneFixture(t, nil).tick().RPC)
	require.Equal(t, RPC{}, newPlaneFixture(t, fakeRPC{meanMS: 20, p99MS: 85, percentiles: true}).tick().RPC)

	// Without percentiles, history reports no p99, only the mean.
	rpc := newPlaneFixture(t, fakeRPC{meanMS: 20, errorRatio: 0.01, enabled: true}).tick().RPC
	require.Equal(t, RPC{Reported: true, Mean: 20 * time.Millisecond, ErrorRatio: 0.01}, rpc)
}

func TestPlaneKeepsEachControllersLatestDecision(t *testing.T) {
	f := newPlaneFixture(t, nil)
	f.plane.RecordDecision(adaptive.Decision{Controller: "persistence_capacity", Reason: "idle"})
	f.plane.RecordDecision(adaptive.Decision{Controller: "persistence_capacity", Reason: "latency", Applied: 540})
	f.plane.RecordDecision(adaptive.Decision{Controller: "worker_count", Reason: "grow"})

	snap := f.tick()
	require.Len(t, snap.Decisions, 2)
	require.Equal(t, adaptive.Reason("latency"), snap.Decisions["persistence_capacity"].Reason)
	require.InDelta(t, 540, snap.Decisions["persistence_capacity"].Applied, 0.001)

	// A published snapshot doesn't change.
	f.plane.RecordDecision(adaptive.Decision{Controller: "persistence_capacity", Reason: "probe"})
	require.Equal(t, adaptive.Reason("latency"), snap.Decisions["persistence_capacity"].Reason)
	require.Equal(t, adaptive.Reason("probe"), f.tick().Decisions["persistence_capacity"].Reason)
}

func TestPlaneCallsSubscribersInOrderUntilCancelled(t *testing.T) {
	f := newPlaneFixture(t, nil)
	var calls []string
	cancelFirst := f.plane.Subscribe(func(*Snapshot) { calls = append(calls, "first") }, nil)
	f.plane.Subscribe(func(*Snapshot) { calls = append(calls, "second") }, nil)

	f.tick()
	cancelFirst()
	f.tick()
	require.Equal(t, []string{"first", "second", "second"}, calls)
}

func TestPlaneStopsPublishingWhenDisabled(t *testing.T) {
	f := newPlaneFixture(t, nil)
	require.NotNil(t, f.tick())
	f.settings.Enabled = false
	require.Nil(t, f.tick())
}

func TestPlaneTicksOnItsOwnUntilStopped(t *testing.T) {
	f := newPlaneFixture(t, nil)
	f.plane.Start()
	await.RequireTrue(t, func() bool {
		f.timeSource.Advance(f.settings.Interval)
		return f.plane.Latest() != nil
	}, 5*time.Second, 10*time.Millisecond)
	f.plane.Stop()
	f.plane.Stop()
}
