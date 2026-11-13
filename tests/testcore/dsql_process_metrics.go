package testcore

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	rtmetrics "runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"time"
)

// processSamplePeriod is how often the test process samples itself into the pool metrics
// file. It is shorter than the pools' 15 seconds, so that a stall of a few seconds shows
// among their samples.
const processSamplePeriod = 5 * time.Second

// The Go runtime metrics the process samples. The sampler writes a counter or histogram as
// its change since the last sample.
const (
	rtCPUUser      = "/cpu/classes/user:cpu-seconds"
	rtCPUGC        = "/cpu/classes/gc/total:cpu-seconds"
	rtCPUGCPause   = "/cpu/classes/gc/pause:cpu-seconds"
	rtCPUIdle      = "/cpu/classes/idle:cpu-seconds"
	rtGCCycles     = "/gc/cycles/total:gc-cycles"
	rtMutexWait    = "/sync/mutex/wait/total:seconds"
	rtGoroutines   = "/sched/goroutines:goroutines"
	rtRunnable     = "/sched/goroutines/runnable:goroutines"
	rtNotInGo      = "/sched/goroutines/not-in-go:goroutines"
	rtThreads      = "/sched/threads/total:threads"
	rtGOMAXPROCS   = "/sched/gomaxprocs:threads"
	rtHeapLive     = "/gc/heap/live:bytes"
	rtHeapGoal     = "/gc/heap/goal:bytes"
	rtMemory       = "/memory/classes/total:bytes"
	rtSchedLatency = "/sched/latencies:seconds"
	rtGCPauses     = "/sched/pauses/total/gc:seconds"
	rtOtherPauses  = "/sched/pauses/total/other:seconds"
)

// The kernel reports a process's CPU time in USER_HZ, which is 100 a second.
const userHZ = 100

// A goroutine that waited this long to run counts as a long scheduling wait.
const longSchedulingWait = 100 * time.Millisecond

type (
	// processSampler writes the test process's own state to the pool metrics file, so that a
	// run shows whether a burst of timeouts comes from DSQL, the network or the process. It
	// writes how late each sample ran, the Go scheduler's latencies, stop-the-world pauses,
	// CPU, memory, goroutines and lock waits, and, where procRoot holds them, the process's
	// CPU time and resident memory, the host's pressure stall times and its TCP segments.
	// Its lines name no cluster or test.
	processSampler struct {
		file     *poolMetricsFile
		procRoot string
		period   time.Duration
		pageSize int

		samples []rtmetrics.Sample
		last    time.Time
		// The counters' values and the histograms' counts at the last sample.
		counters   map[string]float64
		histograms map[string][]uint64
	}

	// histogramChange is a histogram's change between two samples.
	histogramChange struct {
		// buckets has a boundary more than counts. The first may be -Inf and the last +Inf.
		buckets []float64
		counts  []uint64
	}
)

func newProcessSampler(file *poolMetricsFile, procRoot string, period time.Duration) *processSampler {
	supported := make(map[string]bool)
	for _, description := range rtmetrics.All() {
		supported[description.Name] = true
	}
	var samples []rtmetrics.Sample
	for _, name := range []string{
		rtCPUUser, rtCPUGC, rtCPUGCPause, rtCPUIdle, rtGCCycles, rtMutexWait, rtGoroutines,
		rtRunnable, rtNotInGo, rtThreads, rtGOMAXPROCS, rtHeapLive, rtHeapGoal, rtMemory,
		rtSchedLatency, rtGCPauses, rtOtherPauses,
	} {
		if supported[name] {
			samples = append(samples, rtmetrics.Sample{Name: name})
		}
	}
	return &processSampler{
		file:       file,
		procRoot:   procRoot,
		period:     period,
		pageSize:   os.Getpagesize(),
		samples:    samples,
		counters:   make(map[string]float64),
		histograms: make(map[string][]uint64),
	}
}

// run samples the process every period while the process lives.
func (s *processSampler) run() {
	ticker := time.NewTicker(s.period)
	defer ticker.Stop()
	s.sample(time.Now())
	for range ticker.C {
		s.sample(time.Now())
	}
}

// sample writes the process's state at now. A counter or histogram needs an earlier
// sample, so the first sample writes only gauges.
func (s *processSampler) sample(now time.Time) {
	put := func(name string, value float64) {
		s.file.write(poolMetric{Time: now, Metric: name, Value: value})
	}
	if !s.last.IsZero() {
		put("process_sample_late_ms", milliseconds(max(0, now.Sub(s.last)-s.period).Seconds()))
	}
	s.last = now

	rtmetrics.Read(s.samples)
	values := make(map[string]rtmetrics.Value, len(s.samples))
	for _, sample := range s.samples {
		values[sample.Name] = sample.Value
	}
	gauge := func(name, metric string, scale float64) {
		if value, ok := values[metric]; ok {
			put(name, number(value)*scale)
		}
	}
	counter := func(name, key string, value, scale float64) {
		if change, ok := s.change(key, value); ok {
			put(name, change*scale)
		}
	}
	runtimeCounter := func(name, metric string, scale float64) {
		if value, ok := values[metric]; ok {
			counter(name, metric, number(value), scale)
		}
	}

	const mib = 1 << 20
	gauge("process_goroutines", rtGoroutines, 1)
	gauge("process_goroutines_runnable", rtRunnable, 1)
	gauge("process_goroutines_not_in_go", rtNotInGo, 1)
	gauge("process_threads", rtThreads, 1)
	gauge("process_gomaxprocs", rtGOMAXPROCS, 1)
	gauge("process_heap_live_mb", rtHeapLive, 1.0/mib)
	gauge("process_heap_goal_mb", rtHeapGoal, 1.0/mib)
	gauge("process_memory_mb", rtMemory, 1.0/mib)

	runtimeCounter("process_go_cpu_user_seconds", rtCPUUser, 1)
	runtimeCounter("process_go_cpu_gc_seconds", rtCPUGC, 1)
	runtimeCounter("process_go_cpu_idle_seconds", rtCPUIdle, 1)
	runtimeCounter("process_gc_cycles", rtGCCycles, 1)
	runtimeCounter("process_mutex_wait_seconds", rtMutexWait, 1)
	// The GC's pause CPU is GOMAXPROCS times the time the world stood still.
	if procs, ok := values[rtGOMAXPROCS]; ok && number(procs) > 0 {
		runtimeCounter("process_gc_pause_ms", rtCPUGCPause, 1000/number(procs))
	}

	if latencies, ok := s.histogramChange(values, rtSchedLatency); ok {
		put("process_sched_latency_p99_ms", milliseconds(latencies.quantile(0.99)))
		put("process_sched_latency_max_ms", milliseconds(latencies.largest()))
		put("process_sched_waits_over_100ms", float64(latencies.atLeast(longSchedulingWait.Seconds())))
	}
	if pauses, ok := s.histogramChange(values, rtGCPauses); ok {
		put("process_gc_pause_max_ms", milliseconds(pauses.largest()))
	}
	// Pauses other than the GC's, such as a dump of every goroutine's stack.
	if pauses, ok := s.histogramChange(values, rtOtherPauses); ok {
		put("process_other_pauses", float64(pauses.total()))
		put("process_other_pause_ms", milliseconds(pauses.sum()))
		put("process_other_pause_max_ms", milliseconds(pauses.largest()))
	}

	if cpu, err := readProcessCPU(filepath.Join(s.procRoot, "self", "stat")); err == nil {
		counter("process_cpu_seconds", "process cpu", cpu, 1)
	}
	if pages, err := readResidentPages(filepath.Join(s.procRoot, "self", "statm")); err == nil {
		put("process_rss_mb", pages*float64(s.pageSize)/mib)
	}
	for _, resource := range []string{"cpu", "memory", "io"} {
		totals, err := readPressure(filepath.Join(s.procRoot, "pressure", resource))
		if err != nil {
			continue
		}
		for _, kind := range []string{"some", "full"} {
			if total, ok := totals[kind]; ok {
				// Stall totals are in microseconds.
				counter(fmt.Sprintf("host_pressure_%s_%s_ms", resource, kind), "pressure "+resource+" "+kind, total, 1.0/1000)
			}
		}
	}
	if tcp, err := readTCPCounters(filepath.Join(s.procRoot, "net", "snmp")); err == nil {
		for _, field := range []struct{ metric, counter string }{
			{"host_tcp_out_segs", "OutSegs"},
			{"host_tcp_retrans_segs", "RetransSegs"},
		} {
			if value, ok := tcp[field.counter]; ok {
				counter(field.metric, "tcp "+field.counter, value, 1)
			}
		}
	}
}

// change records value for key and returns its change since the last sample, if there was
// one.
func (s *processSampler) change(key string, value float64) (float64, bool) {
	last, ok := s.counters[key]
	s.counters[key] = value
	return value - last, ok
}

// histogramChange records the counts of the histogram metric and returns their change since
// the last sample, if there was one.
func (s *processSampler) histogramChange(values map[string]rtmetrics.Value, metric string) (histogramChange, bool) {
	value, ok := values[metric]
	if !ok || value.Kind() != rtmetrics.KindFloat64Histogram {
		return histogramChange{}, false
	}
	histogram := value.Float64Histogram()
	last, ok := s.histograms[metric]
	counts := slices.Clone(histogram.Counts)
	s.histograms[metric] = counts
	if !ok || len(last) != len(counts) {
		return histogramChange{}, false
	}
	change := histogramChange{buckets: slices.Clone(histogram.Buckets), counts: make([]uint64, len(counts))}
	for i := range counts {
		change.counts[i] = counts[i] - last[i]
	}
	return change, true
}

func (h histogramChange) total() uint64 {
	var total uint64
	for _, count := range h.counts {
		total += count
	}
	return total
}

// quantile returns a bound on the q-quantile: the upper boundary of the bucket that holds
// it, or the lower boundary of an unbounded bucket. It returns 0 for no values.
func (h histogramChange) quantile(q float64) float64 {
	total := h.total()
	if total == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(total)))
	var seen uint64
	for i, count := range h.counts {
		seen += count
		if seen >= rank {
			return h.bound(i)
		}
	}
	return h.bound(len(h.counts) - 1)
}

// largest returns a bound on the largest value, as quantile does, or 0 for no values.
func (h histogramChange) largest() float64 {
	for i := len(h.counts) - 1; i >= 0; i-- {
		if h.counts[i] > 0 {
			return h.bound(i)
		}
	}
	return 0
}

// atLeast counts the values in the buckets whose lower boundary is at least least.
func (h histogramChange) atLeast(least float64) uint64 {
	var count uint64
	for i, c := range h.counts {
		if h.buckets[i] >= least {
			count += c
		}
	}
	return count
}

// sum estimates the values' total from each bucket's midpoint, or from its finite boundary
// when the other is infinite.
func (h histogramChange) sum() float64 {
	var sum float64
	for i, count := range h.counts {
		if count == 0 {
			continue
		}
		lower, upper := h.buckets[i], h.buckets[i+1]
		var value float64
		switch {
		case math.IsInf(lower, -1):
			value = upper
		case math.IsInf(upper, 1):
			value = lower
		default:
			value = (lower + upper) / 2
		}
		sum += float64(count) * value
	}
	return sum
}

// bound returns bucket i's upper boundary, or its lower one when the upper is infinite.
func (h histogramChange) bound(i int) float64 {
	if upper := h.buckets[i+1]; !math.IsInf(upper, 1) {
		return upper
	}
	return h.buckets[i]
}

func number(value rtmetrics.Value) float64 {
	switch value.Kind() {
	case rtmetrics.KindUint64:
		return float64(value.Uint64())
	case rtmetrics.KindFloat64:
		return value.Float64()
	default:
		return 0
	}
}

func milliseconds(seconds float64) float64 {
	return seconds * 1000
}

// readProcessCPU returns the user and system CPU time, in seconds, from a process's stat
// file.
func readProcessCPU(path string) (float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	// The command name, in parentheses, may hold spaces, so count fields after it. The
	// first is the line's third, the state; utime and stime are its 14th and 15th.
	end := bytes.LastIndexByte(data, ')')
	if end < 0 {
		return 0, errors.New("no command name")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 13 {
		return 0, fmt.Errorf("%d fields after the command name", len(fields))
	}
	var ticks float64
	for _, field := range fields[11:13] {
		value, err := strconv.ParseFloat(field, 64)
		if err != nil {
			return 0, err
		}
		ticks += value
	}
	return ticks / userHZ, nil
}

// readResidentPages returns the resident set, in pages, from a process's statm file.
func readResidentPages(path string) (float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, fmt.Errorf("%d fields", len(fields))
	}
	return strconv.ParseFloat(fields[1], 64)
}

// readPressure returns a pressure stall file's totals, in microseconds, by line: some and,
// where the kernel reports it, full.
func readPressure(path string) (map[string]float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	totals := make(map[string]float64, 2)
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		for _, field := range fields[1:] {
			if total, ok := strings.CutPrefix(field, "total="); ok {
				value, err := strconv.ParseFloat(total, 64)
				if err != nil {
					return nil, err
				}
				totals[fields[0]] = value
			}
		}
	}
	return totals, nil
}

// readTCPCounters returns the host's TCP counters by name, from its net/snmp file, where a
// line of names precedes a line of values.
func readTCPCounters(path string) (map[string]float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var names []string
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "Tcp:" {
			continue
		}
		if names == nil {
			names = fields[1:]
			continue
		}
		counters := make(map[string]float64, len(names))
		for i, name := range names {
			if i+1 < len(fields) {
				if value, err := strconv.ParseFloat(fields[i+1], 64); err == nil {
					counters[name] = value
				}
			}
		}
		return counters, nil
	}
	return nil, errors.New("no TCP counters")
}
