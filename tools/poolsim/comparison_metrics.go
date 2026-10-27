package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Bounded by the configured wait horizon, with microsecond resolution. Zero
// waits remain exactly zero; positive waits round up rather than disappear.
type waitHistogram struct {
	buckets map[int64]int64
	count   int64
	maximum time.Duration
}

func (h *waitHistogram) add(d time.Duration) {
	if h.buckets == nil {
		h.buckets = make(map[int64]int64)
	}
	bucket := int64((d + time.Microsecond - 1) / time.Microsecond)
	h.buckets[bucket]++
	h.count++
	h.maximum = max(h.maximum, d)
}
func (h *waitHistogram) percentile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	keys := make([]int64, 0, len(h.buckets))
	for k := range h.buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	remaining := int64(math.Ceil(float64(h.count) * q))
	for _, k := range keys {
		remaining -= h.buckets[k]
		if remaining <= 0 {
			return time.Duration(k) * time.Microsecond
		}
	}
	return h.maximum
}

type poolStats struct {
	requests, completed, waitedNew, opens, churnOpens int64
	cutIdle, cutInUse                                 int64
	peak                                              int
	failures, rejections                              map[string]int64
	wait                                              waitHistogram
}

func newPoolStats() *poolStats {
	return &poolStats{failures: make(map[string]int64), rejections: make(map[string]int64)}
}

type comparisonStats struct {
	poolStats
	attempts, opens, closures map[int]int
	rebuilds                  int64
	samples                   [][]string
}

func newComparisonStats() *comparisonStats {
	return &comparisonStats{poolStats: *newPoolStats(), attempts: make(map[int]int), opens: make(map[int]int), closures: make(map[int]int)}
}

func decimal(n int64) string { return strconv.FormatInt(n, 10) }
func (r *comparisonRun) sample() {
	for _, p := range r.pools {
		if p.closed && p.physical == 0 {
			if p.finalSampled {
				continue
			}
			p.finalSampled = true
		}
		inUse := 0
		for _, c := range p.owned {
			if c.request != nil {
				inUse++
			}
		}
		s := p.stats
		r.stats.samples = append(r.stats.samples, []string{
			r.sim.Now().Format(time.RFC3339Nano), r.name, p.name, p.model,
			strconv.Itoa(len(p.ready)), strconv.Itoa(len(p.idle)), strconv.Itoa(inUse), strconv.Itoa(p.constructing), strconv.Itoa(p.physical), strconv.Itoa(s.peak), strconv.Itoa(p.target()), strconv.Itoa(p.limit()),
			strconv.Itoa(r.cluster.open), strconv.Itoa(r.cluster.peak), strconv.Itoa(r.cluster.cfg.MaxConnections),
			decimal(s.requests), decimal(s.completed), decimal(s.waitedNew), decimal(s.failures["empty"]), decimal(s.failures["timeout"]), decimal(s.failures["dead"]), decimal(s.failures["53400"] + s.failures["53300"]), decimal(s.failures["closed"]),
			decimal(s.opens), decimal(s.rejections["53400"]), decimal(s.rejections["53300"]), decimal(s.cutIdle), decimal(s.cutInUse), decimal(s.churnOpens),
			strconv.FormatInt(int64(s.wait.maximum), 10), strconv.Itoa(p.instance.peakInflight), strconv.FormatFloat(p.instance.adaptive.rate, 'f', 4, 64),
		})
		r.observeConvergence(p)
	}
	r.sim.After(r.cfg.SampleEvery, "comparison-sample", r.sample)
}

func (r *comparisonRun) observeConvergence(p *modelPool) {
	state := r.convergence[p.name]
	if state == nil || p.closed {
		return
	}
	count := len(p.ready)
	if p.isPGX() || p.model == "reservoir" {
		count = len(p.owned)
	}
	now := r.sim.Now()
	if state.reached.IsZero() && count >= state.target {
		state.reached = now
		if now.After(state.started.Add(r.cfg.AssertConvergeWithin)) {
			state.failure = "reached target too late"
		}
	}
	if !state.reached.IsZero() && now.Before(state.reached.Add(r.cfg.AssertStableFor)) && count < state.target {
		state.stable = false
	}
}

func (r *comparisonRun) assertions() error {
	var failures []error
	r.legacy.globalConnects = make(map[int64]int, len(r.stats.opens))
	for sec, n := range r.stats.opens {
		r.legacy.globalConnects[int64(sec)] = n
	}
	if err := AssertConnectRate(r.cfg, r.legacy); err != nil {
		failures = append(failures, err)
	}
	if r.cfg.AssertConvergeWithin > 0 {
		names := make([]string, 0, len(r.convergence))
		for name := range r.convergence {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			c := r.convergence[name]
			if c.reached.IsZero() || c.failure != "" || !c.stable || c.reached.Add(r.cfg.AssertStableFor).After(r.sim.End()) {
				failures = append(failures, fmt.Errorf("%s did not converge and remain stable at %d", name, c.target))
			}
		}
	}
	if r.cfg.AssertZeroEmptyEvents && r.stats.failures["empty"] > 0 {
		failures = append(failures, fmt.Errorf("empty reservoir requests: %d", r.stats.failures["empty"]))
	}
	if r.cfg.AssertZeroDSQLRejections && r.stats.rejections["53400"]+r.stats.rejections["53300"] > 0 {
		failures = append(failures, fmt.Errorf("DSQL rejections: 53400=%d 53300=%d", r.stats.rejections["53400"], r.stats.rejections["53300"]))
	}
	if r.cfg.AssertZeroDeadRequests && r.stats.failures["dead"] > 0 {
		failures = append(failures, fmt.Errorf("dead connection requests: %d", r.stats.failures["dead"]))
	}
	if r.cfg.AssertMaxClosurePerSec > 0 && peakRate(r.stats.closures) > r.cfg.AssertMaxClosurePerSec {
		failures = append(failures, fmt.Errorf("closure rate %d exceeds %d", peakRate(r.stats.closures), r.cfg.AssertMaxClosurePerSec))
	}
	return errors.Join(failures...)
}

func (r *comparisonRun) writeCSV(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	header := []string{"time", "variant", "pool", "model", "ready", "idle", "in_use", "constructing", "physical", "peak_physical", "target", "max_open", "cluster_physical", "cluster_peak", "cluster_cap", "requests", "completed", "waited_new", "failed_empty", "failed_timeout", "failed_dead", "failed_connect", "failed_closed", "opens", "rejected_53400", "rejected_53300", "cut_idle", "cut_in_use", "steady_lifetime_opens", "max_acquire_wait_ns", "instance_peak_inflight", "adaptive_rate"}
	writeErr := w.Write(header)
	if writeErr == nil {
		writeErr = w.WriteAll(r.stats.samples)
	}
	w.Flush()
	return errors.Join(writeErr, w.Error(), f.Close())
}

func peakRate(counts map[int]int) int {
	peak := 0
	for _, n := range counts {
		peak = max(peak, n)
	}
	return peak
}
func (r *comparisonRun) steadyRate() float64 {
	total := 0
	for sec, n := range r.stats.opens {
		if time.Duration(sec)*time.Second >= r.cfg.SteadyAfter {
			total += n
		}
	}
	return float64(total) / (r.cfg.Duration - r.cfg.SteadyAfter).Seconds()
}
func (r *comparisonRun) summary() [][2]string {
	s := r.stats
	poolPeak, ephemeralPeak, target, maxOpen := 0, 0, 0, 0
	for _, p := range r.pools {
		if p.ephemeral {
			ephemeralPeak = max(ephemeralPeak, p.stats.peak)
		} else {
			poolPeak = max(poolPeak, p.stats.peak)
		}
	}
	for _, i := range r.instances {
		target += i.main.target()
		maxOpen += i.main.limit()
	}
	failureCount := int64(0)
	for _, v := range s.failures {
		failureCount += v
	}
	return [][2]string{
		{"Requests / completed", fmt.Sprintf("%d / %d", s.requests, s.completed)},
		{"Acquire p50 / p99", fmt.Sprintf("%s / %s", s.wait.percentile(.5), s.wait.percentile(.99))},
		{"Acquire p99.9 / max", fmt.Sprintf("%s / %s", s.wait.percentile(.999), s.wait.maximum.Round(time.Microsecond))},
		{"Waited on new connection", decimal(s.waitedNew)},
		{"Failed total", decimal(failureCount)},
		{"Failed empty / timeout / dead", fmt.Sprintf("%d / %d / %d", s.failures["empty"], s.failures["timeout"], s.failures["dead"])},
		{"Failed connect / closed", fmt.Sprintf("%d / %d", s.failures["53400"]+s.failures["53300"], s.failures["closed"])},
		{"Opens/s peak / steady", fmt.Sprintf("%d / %.2f", peakRate(s.opens), r.steadyRate())},
		{"Dial attempts/s peak", strconv.Itoa(peakRate(s.attempts))},
		{"Rejected 53400 / 53300", fmt.Sprintf("%d / %d", s.rejections["53400"], s.rejections["53300"])},
		{"Peak main / ephemeral physical", fmt.Sprintf("%d / %d", poolPeak, ephemeralPeak)},
		{"Sum main targets / max open", fmt.Sprintf("%d / %d", target, maxOpen)},
		{"Peak cluster / cap", fmt.Sprintf("%d / %d", r.cluster.peak, r.cluster.cfg.MaxConnections)},
		{"60m cuts idle / in use", fmt.Sprintf("%d / %d", s.cutIdle, s.cutInUse)},
		{"Steady lifetime churn budget", fmt.Sprintf("%.2f%%", float64(s.churnOpens)/(r.cfg.Duration-r.cfg.SteadyAfter).Seconds())},
		{"Pool rebuilds", decimal(s.rebuilds)},
	}
}

func comparisonTable(runs []*comparisonRun) string {
	var w strings.Builder
	fmt.Fprint(&w, "| Metric |")
	for _, r := range runs {
		fmt.Fprintf(&w, " %s |", r.name)
	}
	fmt.Fprint(&w, "\n|---|")
	for range runs {
		fmt.Fprint(&w, "---:|")
	}
	fmt.Fprintln(&w)
	summaries := make([][][2]string, len(runs))
	for j, r := range runs {
		summaries[j] = r.summary()
	}
	for j, row := range summaries[0] {
		fmt.Fprintf(&w, "| %s |", row[0])
		for _, s := range summaries {
			fmt.Fprintf(&w, " %s |", s[j][1])
		}
		fmt.Fprintln(&w)
	}
	return w.String()
}

func runComparisons(cfg *Config, compare, out string) error {
	variants := []string{""}
	if compare != "" {
		variants = strings.Split(compare, ",")
	}
	runs := make([]*comparisonRun, 0, len(variants))
	var failures []error
	seen := make(map[string]bool)
	for _, variant := range variants {
		variant = strings.TrimSpace(variant)
		if compare != "" && (variant == "" || seen[variant]) {
			return fmt.Errorf("empty or duplicate comparison variant %q", variant)
		}
		seen[variant] = true
		r, err := newComparison(cfg, variant)
		if err != nil {
			return err
		}
		r.sim.Run()
		if err := r.assertions(); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", r.name, err))
		}
		path := out
		if path == "" {
			path = "poolsim.csv"
		}
		if compare != "" || out == "" {
			ext := filepath.Ext(path)
			path = strings.TrimSuffix(path, ext) + "-" + r.name + ".csv"
		}
		if err := r.writeCSV(path); err != nil {
			return err
		}
		fmt.Printf("CSV (%s): %s\n", r.name, path)
		// Keep summaries, not all per-pool time series, across variants.
		r.stats.samples = nil
		runs = append(runs, r)
	}
	fmt.Print(comparisonTable(runs))
	return errors.Join(failures...)
}
