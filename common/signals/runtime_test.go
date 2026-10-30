package signals

import (
	"math"
	"runtime/debug"
	"runtime/metrics"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSchedulingLatencyCoversOnlyTheNewestWaits(t *testing.T) {
	buckets := []float64{0, 1e-6, 1e-5, 1e-4, 1e-3, math.Inf(1)}
	histogram := func(counts ...uint64) *metrics.Float64Histogram {
		return &metrics.Float64Histogram{Counts: counts, Buckets: buckets}
	}
	r := newRuntimeSource()

	p99, n := r.schedSince(histogram(1000, 0, 0, 900, 0))
	require.Zero(t, p99, "the first read sets the baseline")
	require.Zero(t, n)

	// 100 new waits, two of them in the 0.1–1ms bucket.
	p99, n = r.schedSince(histogram(1090, 8, 0, 902, 0))
	require.Equal(t, time.Millisecond, p99)
	require.Equal(t, uint64(100), n)

	p99, n = r.schedSince(histogram(1090, 8, 0, 902, 0))
	require.Zero(t, p99, "no new waits")
	require.Zero(t, n)

	// A wait in the unbounded bucket reads as its lower bound.
	p99, n = r.schedSince(histogram(1090, 8, 0, 902, 1))
	require.Equal(t, time.Millisecond, p99)
	require.Equal(t, uint64(1), n)

	r.reset()
	p99, n = r.schedSince(histogram(5000, 8, 0, 902, 1))
	require.Zero(t, p99, "a reset sets a new baseline")
	require.Zero(t, n)
}

func TestRuntimeSourceReadsTheRuntime(t *testing.T) {
	previous := debug.SetMemoryLimit(3 << 30)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	r := newRuntimeSource()
	r.read()
	rt, goLimit := r.read()
	require.Positive(t, rt.Goroutines)
	require.Positive(t, rt.HeapGoal)
	require.Positive(t, rt.Mapped)
	require.Equal(t, uint64(3<<30), goLimit)

	debug.SetMemoryLimit(math.MaxInt64)
	_, goLimit = r.read()
	require.Zero(t, goLimit, "no limit set")
}
