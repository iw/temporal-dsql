package adaptivereplay

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/adaptive/fairshare"
	"go.temporal.io/server/common/adaptive/memory"
	"go.temporal.io/server/common/adaptive/partitions"
	"go.temporal.io/server/common/adaptive/taskrate"
	"go.temporal.io/server/common/adaptive/workers"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/signals"
	"go.uber.org/fx/fxtest"
)

var start = time.Unix(1_700_000_000, 0)

type (
	// health is the persistence health a test sets.
	health struct {
		persistence.HealthSignalAggregator
		snapshot persistence.HealthSnapshot
	}

	// resizable is a cache's size, as the memory budget sets it.
	resizable struct {
		size int
	}
)

func (h *health) RecentSnapshot(int) persistence.HealthSnapshot {
	return h.snapshot
}

func (r *resizable) SetMaxSize(size int) {
	r.size = size
}

// The signal plane's trace replays exactly: each controller it drives gives the
// proposals it recorded, through its envelope's step limit, a freeze, and a mode turned
// off and on again.
func TestReplaysWhatTheSignalPlaneRecorded(t *testing.T) {
	previous := debug.SetMemoryLimit(512 << 20)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	dir := t.TempDir()
	dc := dynamicconfig.NewMemoryClient()
	dc.OverrideSetting(signals.Config, signals.Settings{Enabled: true, Interval: time.Second, Windows: 3, TraceDirectory: dir, TraceMaxBytes: 1 << 30})
	workerSettings := workers.DefaultSettings
	workerSettings.Mode = adaptive.ModeNameEnabled
	workerSettings.IdleSteps = 3
	workerSettings.MinSamples = 5
	workerSettings.Cooldown = 2 * time.Second
	dc.OverrideSetting(workers.Config, workerSettings)
	memorySettings := memory.DefaultSettings
	memorySettings.Mode = adaptive.ModeNameShadow
	memorySettings.BytesPerTask = 8192
	memorySettings.Interval = 3 * time.Second
	dc.OverrideSetting(memory.Config, memorySettings)
	rateSettings := taskrate.DefaultSettings
	rateSettings.Mode = adaptive.ModeNameShadow
	rateSettings.MinSamples = 10
	dc.OverrideSetting(taskrate.Config, rateSettings)
	collection := dynamicconfig.NewCollection(dc, log.NewNoopLogger())

	h := &health{
		HealthSignalAggregator: persistence.NoopHealthSignalAggregator,
		snapshot:               persistence.HealthSnapshot{Samples: 900, ScheduleSamples: 200},
	}
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(start)
	plane := signals.PlaneProvider(signals.PlaneParams{
		Lifecycle:   fxtest.NewLifecycle(t),
		Collection:  collection,
		ServiceName: primitives.HistoryService,
		TimeSource:  timeSource,
		Logger:      log.NewNoopLogger(),
		Persistence: h,
	})
	queues := workers.NewHistoryQueues(plane, collection, h, metrics.NoopMetricsHandler)
	_, _ = queues.WorkerCount("transfer", func(func(int)) (int, func()) { return 64, func() {} })(func(int) {})
	transfer := queues.ScheduleLatencyRecorder("transfer")
	queues.PendingTaskLimits("transfer", func() int { return 10000 }, func() int { return 9000 })
	queues.SetShardCount(func() int { return 4 })
	memory.DriveCache(plane, collection, memory.HistoryCache, 512<<20, &resizable{}, metrics.NoopMetricsHandler)
	taskrate.NewSchedulerLimiter(quotas.NoopRequestRateLimiter, plane, collection, func() float64 { return 9000 }, nil, metrics.NoopMetricsHandler)

	busy := func(i int) bool { return i < 20 || 35 <= i && i < 45 }
	for i := range 60 {
		now := start.Add(time.Duration(i) * time.Second)
		timeSource.Update(now)
		if busy(i) {
			for range 20 {
				transfer.RecordScheduleLatency(300 * time.Millisecond)
			}
		}
		switch i {
		case 12:
			dc.OverrideSetting(adaptive.Frozen, true)
		case 16:
			dc.OverrideSetting(adaptive.Frozen, false)
		case 30:
			off := workerSettings
			off.Mode = adaptive.ModeNameDisabled
			dc.OverrideSetting(workers.Config, off)
		case 35:
			dc.OverrideSetting(workers.Config, workerSettings)
		case 40:
			h.snapshot.ScheduleSamples = 600
		case 50:
			h.snapshot.Rejected = 100
		default:
		}
		plane.Tick(now)
	}
	plane.Stop()

	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	f, err := os.Open(files[0])
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	traces, err := Read(filepath.Base(files[0]), f)
	require.NoError(t, err)
	require.Len(t, traces, 1)
	require.Equal(t, 60, traces[0].Ticks)

	r, err := Run(traces[0], nil, nil, nil)
	require.NoError(t, err)
	require.Empty(t, r.Unknown)
	byName := make(map[string]*Series)
	for _, s := range r.Series {
		require.Equal(t, len(s.Steps), s.Matched(), "%s replays as it was recorded", s.Controller)
		byName[s.Controller] = s
	}
	require.Len(t, byName, 4)

	count := byName[workers.NamePrefix+"transfer"].Replayed()
	require.Equal(t, 55, count.Steps, "no decisions while it was off")
	require.Equal(t, 4, count.Reasons[adaptive.ReasonFrozen])
	require.Positive(t, count.Reasons[adaptive.Reason(workers.ReasonGrow)])
	require.Positive(t, count.Reasons[adaptive.Reason(workers.ReasonShrink)])
	require.Positive(t, count.Reasons[adaptive.Reason(workers.ReasonBackoff)])
	require.GreaterOrEqual(t, count.Reversals, 2, "grew, shrank, grew again from where it started over, and shrank")
	require.InDelta(t, 2, count.Max, 1e-9)

	cache := byName[memory.NamePrefix+memory.HistoryCache].Replayed()
	require.Greater(t, cache.Changes, 1, "the step limit spread the cut to the budget over steps")
	rate := byName[taskrate.Name].Replayed()
	require.Greater(t, rate.Changes, 1, "the step limit spread the coupling over steps")
	require.Contains(t, byName, memory.NamePrefix+memory.PendingTasks)

	// Without its step limit, the cache would have gone to its budget at once.
	r, err = Run(traces[0], []Override{{Controller: memory.NamePrefix + memory.HistoryCache, Field: "MaxStepFrac", Value: "0"}}, nil, nil)
	require.NoError(t, err)
	for _, s := range r.Series {
		if s.Controller == memory.NamePrefix+memory.HistoryCache {
			require.Less(t, s.Matched(), len(s.Steps))
			require.Less(t, s.Steps[0].Replayed, s.Steps[0].Recorded, "its first step goes to the budget")
		}
	}

	// With a longer target, the pool wouldn't have grown.
	used := make(map[Override]bool)
	slower := Override{Controller: "worker_count", Field: "TargetDelay", Value: "1s"}
	r, err = Run(traces[0], []Override{slower}, nil, used)
	require.NoError(t, err)
	require.True(t, used[slower])
	for _, s := range r.Series {
		if s.Controller == workers.NamePrefix+"transfer" {
			require.Less(t, s.Matched(), len(s.Steps))
			require.Zero(t, s.Replayed().Reasons[adaptive.Reason(workers.ReasonGrow)])
		} else {
			require.Equal(t, len(s.Steps), s.Matched())
		}
	}

	// GrowFrac is the envelope's step limit too, so growing by half, the pool's first step
	// would have taken it from 64 to 96.
	r, err = Run(traces[0], []Override{{Controller: "worker_count", Field: "GrowFrac", Value: "0.5"}}, nil, nil)
	require.NoError(t, err)
	for _, s := range r.Series {
		if s.Controller == workers.NamePrefix+"transfer" {
			require.InDelta(t, 80, s.Steps[1].Recorded, 0)
			require.InDelta(t, 96, s.Steps[1].Replayed, 0)
		}
	}
}

// recording writes a trace file's lines, as the signal plane's trace writer does.
type recording struct {
	buf bytes.Buffer
}

func (r *recording) write(t *testing.T, rec signals.Record) {
	t.Helper()
	line, err := json.Marshal(rec)
	require.NoError(t, err)
	r.buf.Write(line)
	r.buf.WriteByte('\n')
}

func (r *recording) decision(t *testing.T, d adaptive.Decision) {
	t.Helper()
	r.write(t, signals.Record{Decision: &d})
}

func (r *recording) read(t *testing.T) *Trace {
	t.Helper()
	traces, err := Read("trace.jsonl", &r.buf)
	require.NoError(t, err)
	require.Len(t, traces, 1)
	return traces[0]
}

// capacityRecording records the capacity controller in shadow mode for 120 seconds, as the
// persistence rate limiter steps it: it starts the law at 800, the static rate when it
// started, which is 1,000 by the first step. Errors run from 40s to 50s, and the limit binds
// from 50s. The controller is off from 55s to 60s, and frozen from 60s to 65s.
func capacityRecording(t *testing.T) *recording {
	t.Helper()
	env := capacity.DefaultEnvelope(1000)
	env.Mode = capacity.ModeShadow
	ctrl := capacity.New(800)
	from := 800.0
	rec := &recording{}
	rec.write(t, signals.Record{Header: &signals.Header{Service: "history", Host: "host", PID: 1, Start: start}})
	for i := range 120 {
		now := start.Add(time.Duration(i) * time.Second)
		switch {
		case 55 <= i && i < 60:
			ctrl.Step(capacity.Envelope{Mode: capacity.ModeOff}, 1000, capacity.Signals{})
			from = 1000
			continue
		case 60 <= i && i < 65:
			var detail any
			if from > 0 {
				detail, from = capacity.Detail{Start: from}, 0
			}
			rec.decision(t, adaptive.Decision{Controller: capacity.Name, Time: now, Mode: adaptive.ModeShadow, Reason: adaptive.ReasonFrozen, Static: 1000, Proposed: ctrl.Rate(), Applied: 1000, Detail: detail})
			continue
		default:
		}
		s := capacity.Signals{Now: now, P99: 5 * time.Millisecond, Throughput: 900, Span: 3 * time.Second, Samples: 1000, Binding: i >= 50}
		if 40 <= i && i < 50 {
			s.ErrorRatio, s.Throughput = 0.2, 300
		}
		d := ctrl.Step(env, 1000, s)
		rec.decision(t, adaptive.Decision{
			Controller: capacity.Name,
			Time:       now,
			Mode:       adaptive.ModeShadow,
			Reason:     adaptive.Reason(d.Reason),
			Static:     1000,
			Proposed:   d.Proposed,
			Applied:    d.RPS,
			Detail:     capacity.Detail{Envelope: env, Signals: s, Decision: d, Start: from},
		})
		from = 0
	}
	return rec
}

func TestReplaysTheCapacityController(t *testing.T) {
	trace := capacityRecording(t).read(t)

	r, err := Run(trace, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, r.Series, 1)
	s := r.Series[0]
	require.Len(t, s.Steps, 115, "none while it was off")
	require.Equal(t, 115, s.Matched())
	require.InDelta(t, 800, s.Steps[0].Replayed, 0, "the rate the law started at")
	require.InDelta(t, 1000, s.Steps[55].Replayed, 0, "frozen at the static rate the law tracked while it was off")
	m := s.Replayed()
	require.Positive(t, m.Reasons[adaptive.Reason(capacity.ReasonErrors)])
	require.Equal(t, 5, m.Reasons[adaptive.ReasonFrozen])
	require.Positive(t, m.Reasons[adaptive.Reason(capacity.ReasonRecover)])
	require.InDelta(t, 0.27, m.Min, 1e-9, "cut to 0.9 of the 300 delivered")

	// Had it allowed 30% errors, it wouldn't have cut below the rate it started at.
	r, err = Run(trace, []Override{{Controller: capacity.Name, Field: "MaxErrorRatio", Value: "0.3"}}, nil, nil)
	require.NoError(t, err)
	m = r.Series[0].Replayed()
	require.Zero(t, m.Reasons[adaptive.Reason(capacity.ReasonErrors)])
	require.InDelta(t, 0.8, m.Min, 1e-9)
}

func TestReplaysEachTaskQueuesPartitionScaler(t *testing.T) {
	params := partitions.DefaultSettings.Params(false)
	params.Settle, params.DownHold = 0, time.Minute
	rec := &recording{}
	laws := map[string]*partitions.Controller{
		"Workflow": partitions.New(500),
		"Activity": partitions.New(0),
	}
	count := map[string]int{"Workflow": 4, "Activity": 4}
	for i := range 40 {
		now := start.Add(time.Duration(i) * 10 * time.Second)
		for _, tqType := range []string{"Workflow", "Activity"} {
			sig := partitions.Signals{Now: now, Partitions: count[tqType], Described: count[tqType], AddRate: 1800, DispatchRate: 1800}
			detail := partitions.Detail{Namespace: "ns", TaskQueue: "tq", TaskQueueType: tqType, Params: params, Signals: sig, Started: i == 0}
			switch {
			case tqType == "Workflow" && i == 0:
				detail.Loaded = 500
			case tqType == "Workflow" && i == 20:
				// Matching loaded the task queue again, and its new scaler's law has no
				// learned capacity.
				laws[tqType] = partitions.New(0)
				detail.Started = true
			default:
			}
			target, reason := count[tqType], adaptive.Reason(partitions.ReasonNoData)
			if i == 7 {
				detail.Held = true
			} else {
				var r partitions.Reason
				target, r = laws[tqType].Step(params, sig)
				reason = adaptive.Reason(r)
			}
			detail.Capacity = laws[tqType].Capacity(params)
			rec.decision(t, adaptive.Decision{Controller: partitions.Name, Time: now, Mode: adaptive.ModeOn, Reason: reason, Static: 4, Proposed: float64(target), Applied: float64(target), Detail: detail})
			count[tqType] = target
		}
	}

	r, err := Run(rec.read(t), nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, r.Series, 2)
	for _, s := range r.Series {
		require.Equal(t, 40, s.Matched(), s.Key)
	}
	require.Equal(t, "ns/tq/Workflow", r.Series[0].Key)
	require.Equal(t, "ns/tq/Activity", r.Series[1].Key)
	workflow := r.Series[0]
	require.Greater(t, workflow.Steps[19].Replayed, workflow.Steps[39].Replayed, "at the 500 a partition it loaded, the task queue needs more partitions than at the default 1,000 it started again from")
	require.Equal(t, 1, workflow.Replayed().Reasons[adaptive.Reason(partitions.ReasonNoData)], "the held step")
}

func TestReplaysFairShare(t *testing.T) {
	params := fairshare.Params{MinShare: 0.2, Headroom: 0.2}
	rec := &recording{}
	for i, own := range []float64{0, 40, 40, 60, 0} {
		demand := fairshare.Demand{Own: own, Peers: 20, Members: 2, Known: 1}
		share, reason := fairshare.Share(params, 100, demand)
		recorded := adaptive.Reason(reason)
		if i == 2 {
			recorded = adaptive.Reason(fairshare.ReasonHeld)
		}
		rec.decision(t, adaptive.Decision{
			Controller: fairshare.Name,
			Time:       start.Add(time.Duration(i) * 5 * time.Second),
			Mode:       adaptive.ModeOn,
			Reason:     recorded,
			Static:     50,
			Proposed:   share,
			Applied:    share,
			Detail:     fairshare.Detail{Class: "namespace", Namespace: "ns", Limit: 100, Params: params, Demand: demand},
		})
	}

	trace := rec.read(t)
	r, err := Run(trace, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, r.Series, 1)
	require.Equal(t, "namespace/ns", r.Series[0].Key)
	require.Equal(t, 5, r.Series[0].Matched())
	require.Equal(t, 1, r.Series[0].Replayed().Reasons[adaptive.Reason(fairshare.ReasonHeld)])

	r, err = Run(trace, []Override{{Controller: fairshare.Name, Field: "MinShare", Value: "0.5"}}, nil, nil)
	require.NoError(t, err)
	require.Less(t, r.Series[0].Matched(), 5)
}

func TestLeavesControllersItDoesntKnow(t *testing.T) {
	rec := &recording{}
	rec.decision(t, adaptive.Decision{Controller: "something_new", Time: start, Mode: adaptive.ModeShadow, Static: 1, Proposed: 2})
	r, err := Run(rec.read(t), nil, nil, nil)
	require.NoError(t, err)
	require.Empty(t, r.Series)
	require.Equal(t, map[string]int{"something_new": 1}, r.Unknown)
}
