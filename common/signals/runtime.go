package signals

import (
	"math"
	"runtime/metrics"
	"time"
)

const (
	metricSchedLatencies = "/sched/latencies:seconds"
	metricGoroutines     = "/sched/goroutines:goroutines"
	metricHeapLive       = "/gc/heap/live:bytes"
	metricHeapGoal       = "/gc/heap/goal:bytes"
	metricMapped         = "/memory/classes/total:bytes"
	metricGoMemLimit     = "/gc/gomemlimit:bytes"
)

// runtimeSource reads the Go runtime's metrics. It isn't safe for concurrent use.
type runtimeSource struct {
	samples []metrics.Sample
	// prevSched holds the scheduling latency histogram's counts at the previous read,
	// since the runtime's counts run from the start of the process.
	prevSched []uint64
}

func newRuntimeSource() *runtimeSource {
	names := []string{metricSchedLatencies, metricGoroutines, metricHeapLive, metricHeapGoal, metricMapped, metricGoMemLimit}
	samples := make([]metrics.Sample, len(names))
	for i, name := range names {
		samples[i].Name = name
	}
	return &runtimeSource{samples: samples}
}

// read returns the runtime's view and GOMEMLIMIT. The first read after newRuntimeSource
// or reset sets the baseline for scheduling latency, so it reports none.
func (r *runtimeSource) read() (Runtime, uint64) {
	metrics.Read(r.samples)
	var rt Runtime
	var goLimit uint64
	for _, s := range r.samples {
		switch s.Value.Kind() {
		case metrics.KindFloat64Histogram:
			if s.Name == metricSchedLatencies {
				rt.SchedP99, rt.SchedSamples = r.schedSince(s.Value.Float64Histogram())
			}
		case metrics.KindUint64:
			v := s.Value.Uint64()
			switch s.Name {
			case metricGoroutines:
				rt.Goroutines = v
			case metricHeapLive:
				rt.HeapLive = v
			case metricHeapGoal:
				rt.HeapGoal = v
			case metricMapped:
				rt.Mapped = v
			case metricGoMemLimit:
				if v < math.MaxInt64 { // the runtime reports an unset limit as MaxInt64
					goLimit = v
				}
			default:
			}
		default:
		}
	}
	return rt, goLimit
}

func (r *runtimeSource) reset() {
	r.prevSched = nil
}

// schedSince returns the p99 of the scheduling latencies recorded since the previous
// read, as the upper bound of the bucket it falls in, and how many there were.
func (r *runtimeSource) schedSince(h *metrics.Float64Histogram) (time.Duration, uint64) {
	if len(r.prevSched) != len(h.Counts) {
		r.prevSched = append(r.prevSched[:0], h.Counts...)
		return 0, 0
	}
	var total uint64
	for i, count := range h.Counts {
		total += count - r.prevSched[i]
	}
	rank := uint64(math.Ceil(0.99 * float64(total)))
	var p99 time.Duration
	var seen uint64
	for i, count := range h.Counts {
		seen += count - r.prevSched[i]
		if total > 0 && seen >= rank {
			p99 = bucketBound(h.Buckets, i)
			break
		}
	}
	copy(r.prevSched, h.Counts)
	return p99, total
}

// bucketBound is bucket i's upper bound, or its lower bound for the last, unbounded
// bucket.
func bucketBound(buckets []float64, i int) time.Duration {
	bound := buckets[i+1]
	if math.IsInf(bound, 1) {
		bound = buckets[i]
	}
	if math.IsInf(bound, 0) || bound < 0 {
		return 0
	}
	return time.Duration(bound * float64(time.Second))
}
