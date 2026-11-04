package adaptivesim

import (
	"math"
	"slices"
	"time"

	"go.temporal.io/server/common/persistence"
)

// numFacts is how many kinds of store facts there are.
const numFacts = int(persistence.StoreFactAcquired) + 1

type (
	// health is the history host's persistence health signal aggregator, as
	// common/persistence's healthSignalAggregatorImpl keeps it, on simulated time: calls,
	// unhealthy errors, rejections, facts and task attempts counted in fixed windows, and
	// latencies in windows that start at their first sample. The real aggregator reads the
	// wall clock, so the simulator keeps its own, and takes exact quantiles where the real
	// one uses t-digests.
	health struct {
		size          time.Duration
		buckets       []counterBucket
		latency       windowedSamples
		schedule      windowedSamples
		factsDeclared bool
	}

	counterBucket struct {
		window int64
		windowCounts
	}

	windowCounts struct {
		total, errors, rejected, attempts uint64
		facts                             [numFacts]uint64
	}

	// windowedSamples keeps samples in windows of a fixed size, each starting at its first
	// sample, as stats.NewWindowedTDigest does.
	windowedSamples struct {
		size    time.Duration
		windows []sampleWindow // a ring; head is the newest
		head    int
	}

	sampleWindow struct {
		start, end time.Time
		values     []float64
		used       bool
	}
)

func newHealth(cfg Signals) *health {
	return &health{
		size:     cfg.Window,
		buckets:  make([]counterBucket, cfg.Windows),
		latency:  windowedSamples{size: cfg.Window, windows: make([]sampleWindow, cfg.Windows)},
		schedule: windowedSamples{size: cfg.Window, windows: make([]sampleWindow, cfg.Windows)},
	}
}

// recordCall is RecordCall for a call that reached the persistence rate limiter. A call
// that failed after its deadline passed counts as unhealthy; so do context errors. The
// limiter's own rejections are left out of the windows' calls.
func (h *health) recordCall(now time.Time, latency time.Duration, f failure, deadlinePassed bool) {
	if f == limited {
		h.bucket(now).rejected++
		return
	}
	h.latency.record(float64(latency.Milliseconds()), now)
	b := h.bucket(now)
	b.total++
	if f == timedOut || (f != ok && deadlinePassed) {
		b.errors++
	}
}

// recordSchedule is RecordScheduleLatency: a task attempt's wait in its scheduler.
func (h *health) recordSchedule(now time.Time, wait time.Duration) {
	h.schedule.record(float64(wait.Milliseconds()), now)
	h.bucket(now).attempts++
}

func (h *health) reportFact(now time.Time, fact persistence.StoreFact, count int64) {
	if count <= 0 {
		return
	}
	h.bucket(now).facts[fact] += uint64(count)
}

func (h *health) bucket(now time.Time) *counterBucket {
	w := now.UnixNano() / int64(h.size)
	b := &h.buckets[w%int64(len(h.buckets))]
	if b.window != w {
		*b = counterBucket{window: w}
	}
	return b
}

// snapshot is RecentSnapshot over the newest windows.
func (h *health) snapshot(now time.Time, windows int) persistence.HealthSnapshot {
	n := min(max(windows, 1), len(h.buckets))
	cur := now.UnixNano() / int64(h.size)
	var counts windowCounts
	for k := range int64(n) {
		w := cur - k
		if b := h.buckets[w%int64(len(h.buckets))]; b.window == w {
			counts.total += b.total
			counts.errors += b.errors
			counts.rejected += b.rejected
			counts.attempts += b.attempts
			for fact, count := range b.facts {
				counts.facts[fact] += count
			}
		}
	}
	span := time.Duration(n-1)*h.size + time.Duration(now.UnixNano()-cur*int64(h.size))
	p99 := h.latency.quantileRecent(0.99, windows, now)
	snap := persistence.HealthSnapshot{
		P99:               time.Duration(p99 * float64(time.Millisecond)),
		Samples:           int(counts.total),
		Span:              span,
		StoreReportsFacts: h.factsDeclared,
		Throttled:         int(counts.facts[persistence.StoreFactThrottled]),
		Contention:        int(counts.facts[persistence.StoreFactContention]),
		Limit:             int(counts.facts[persistence.StoreFactLimit]),
		Queued:            int(counts.facts[persistence.StoreFactQueued]),
		Acquired:          int(counts.facts[persistence.StoreFactAcquired]),
		Rejected:          int(counts.rejected),
		ScheduleSamples:   int(counts.attempts),
	}
	if counts.total > 0 {
		snap.ErrorRatio = float64(counts.errors) / float64(counts.total)
	}
	if span > 0 {
		delivered := counts.total - counts.errors
		delivered -= min(delivered, counts.facts[persistence.StoreFactThrottled])
		snap.Throughput = float64(delivered) / span.Seconds()
	}
	if counts.attempts > 0 {
		p90 := h.schedule.quantileRecent(0.90, windows, now)
		snap.ScheduleP90 = time.Duration(p90 * float64(time.Millisecond))
	}
	return snap
}

// record adds a sample. Simulated time never goes back, so a sample is in the newest
// window or starts the next.
func (w *windowedSamples) record(v float64, at time.Time) {
	latest := &w.windows[w.head]
	if latest.used && at.Before(latest.end) {
		latest.values = append(latest.values, v)
		return
	}
	w.head = (w.head + 1) % len(w.windows)
	next := &w.windows[w.head]
	next.start, next.end, next.used = at, at.Add(w.size), true
	next.values = append(next.values[:0], v)
}

// quantileRecent is the q-quantile over the newest n windows that ended after
// now - n×size, or 0 without samples.
func (w *windowedSamples) quantileRecent(q float64, n int, now time.Time) float64 {
	cutoff := now.Add(-time.Duration(n) * w.size)
	var merged []float64
	idx := w.head
	for range min(n, len(w.windows)) {
		win := &w.windows[idx]
		if win.used && win.end.After(cutoff) {
			merged = append(merged, win.values...)
		}
		idx = (idx - 1 + len(w.windows)) % len(w.windows)
	}
	if len(merged) == 0 {
		return 0
	}
	slices.Sort(merged)
	rank := int(math.Ceil(q*float64(len(merged)))) - 1
	return merged[min(max(rank, 0), len(merged)-1)]
}
