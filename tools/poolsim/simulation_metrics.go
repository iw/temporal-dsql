package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
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

type simulationStats struct {
	poolStats
	phaseNames                []string
	phases                    map[string]*poolStats
	startDelay                waitHistogram
	attempts, opens, closures map[int]int
	samples                   [][]string
}

func newSimulationStats() *simulationStats {
	return &simulationStats{poolStats: *newPoolStats(), phases: make(map[string]*poolStats), attempts: make(map[int]int), opens: make(map[int]int), closures: make(map[int]int)}
}

func decimal(n int64) string { return strconv.FormatInt(n, 10) }
func (r *simulationRun) sample() {
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
			r.sim.Now().Format(time.RFC3339Nano), p.name,
			strconv.Itoa(len(p.idle)), strconv.Itoa(inUse), strconv.Itoa(p.constructing), strconv.Itoa(p.physical), strconv.Itoa(s.peak), strconv.Itoa(p.target()), strconv.Itoa(p.limit()),
			strconv.Itoa(r.cluster.open), strconv.Itoa(r.cluster.peak), strconv.Itoa(r.cluster.cfg.MaxConnections),
			decimal(s.requests), decimal(s.completed), decimal(s.waitedNew), decimal(s.failures["timeout"]), decimal(s.failures["dead"]), decimal(s.failures["53400"] + s.failures["53300"]), decimal(s.failures["closed"]),
			decimal(s.opens), decimal(s.rejections["53400"]), decimal(s.rejections["53300"]), decimal(s.cutIdle), decimal(s.cutInUse), decimal(s.churnOpens),
			strconv.FormatInt(int64(s.wait.maximum), 10), strconv.Itoa(p.instance.peakInflight), strconv.FormatFloat(p.instance.adaptive.rate, 'f', 4, 64),
		})
		r.observeConvergence(p)
	}
	r.sim.After(r.cfg.SampleEvery, "sample", r.sample)
}

func (r *simulationRun) observeConvergence(p *modelPool) {
	state := r.convergence[p.name]
	if state == nil || p.closed {
		return
	}
	count := len(p.owned)
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

func (r *simulationRun) assertions() error {
	var failures []error
	if r.cfg.AssertMaxConnectsPerSec > 0 && peakRate(r.stats.opens) > r.cfg.AssertMaxConnectsPerSec {
		failures = append(failures, fmt.Errorf("connection rate %d exceeds %d", peakRate(r.stats.opens), r.cfg.AssertMaxConnectsPerSec))
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

func (r *simulationRun) writeCSV(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	header := []string{"time", "pool", "idle", "in_use", "constructing", "physical", "peak_physical", "target", "max_open", "cluster_physical", "cluster_peak", "cluster_cap", "requests", "completed", "waited_new", "failed_timeout", "failed_dead", "failed_connect", "failed_closed", "opens", "rejected_53400", "rejected_53300", "cut_idle", "cut_in_use", "steady_lifetime_opens", "max_acquire_wait_ns", "instance_peak_inflight", "adaptive_rate"}
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
func (r *simulationRun) steadyRate() float64 {
	total := 0
	for sec, n := range r.stats.opens {
		if time.Duration(sec)*time.Second >= r.cfg.SteadyAfter {
			total += n
		}
	}
	return float64(total) / (r.cfg.Duration - r.cfg.SteadyAfter).Seconds()
}
func (r *simulationRun) summary() [][2]string {
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
	rows := [][2]string{
		{"Requests / completed", fmt.Sprintf("%d / %d", s.requests, s.completed)},
		{"Acquire p50 / p99", fmt.Sprintf("%s / %s", s.wait.percentile(.5), s.wait.percentile(.99))},
		{"Acquire p99.9 / max", fmt.Sprintf("%s / %s", s.wait.percentile(.999), s.wait.maximum.Round(time.Microsecond))},
		{"Waited on new connection", decimal(s.waitedNew)},
		{"Failed total", decimal(failureCount)},
		{"Failed timeout / dead", fmt.Sprintf("%d / %d", s.failures["timeout"], s.failures["dead"])},
		{"Failed connect / closed", fmt.Sprintf("%d / %d", s.failures["53400"]+s.failures["53300"], s.failures["closed"])},
		{"Opens/s peak / steady", fmt.Sprintf("%d / %.2f", peakRate(s.opens), r.steadyRate())},
		{"Dial attempts/s peak", strconv.Itoa(peakRate(s.attempts))},
		{"Rejected 53400 / 53300", fmt.Sprintf("%d / %d", s.rejections["53400"], s.rejections["53300"])},
		{"Peak main / ephemeral physical", fmt.Sprintf("%d / %d", poolPeak, ephemeralPeak)},
		{"Sum main targets / max open", fmt.Sprintf("%d / %d", target, maxOpen)},
		{"Peak cluster / cap", fmt.Sprintf("%d / %d", r.cluster.peak, r.cluster.cfg.MaxConnections)},
		{"60m cuts idle / in use", fmt.Sprintf("%d / %d", s.cutIdle, s.cutInUse)},
		{"Steady lifetime churn budget", fmt.Sprintf("%.2f%%", float64(s.churnOpens)/(r.cfg.Duration-r.cfg.SteadyAfter).Seconds())},
		{"Workload start delay p50 / max", fmt.Sprintf("%s / %s", s.startDelay.percentile(.5), s.startDelay.maximum.Round(time.Microsecond))},
	}
	for _, name := range s.phaseNames {
		phase := s.phases[name]
		failed := int64(0)
		for _, count := range phase.failures {
			failed += count
		}
		rows = append(rows,
			[2]string{name + ": requests / failed", fmt.Sprintf("%d / %d", phase.requests, failed)},
			[2]string{name + ": failed timeout / dead", fmt.Sprintf("%d / %d", phase.failures["timeout"], phase.failures["dead"])},
			[2]string{name + ": acquire p99 / p99.9 / max", fmt.Sprintf("%s / %s / %s", phase.wait.percentile(.99), phase.wait.percentile(.999), phase.wait.maximum.Round(time.Microsecond))},
			[2]string{name + ": waited on new connection", decimal(phase.waitedNew)},
		)
	}
	return rows
}

func (r *simulationRun) summaryTable() string {
	var w strings.Builder
	fmt.Fprintln(&w, "| Metric | pgxpool |\n|---|---:|")
	for _, row := range r.summary() {
		fmt.Fprintf(&w, "| %s | %s |\n", row[0], row[1])
	}
	return w.String()
}

func runSimulation(cfg *Config, out string) error {
	r, err := newSimulation(cfg)
	if err != nil {
		return err
	}
	r.sim.Run()
	if out != "" {
		if err := r.writeCSV(out); err != nil {
			return err
		}
		fmt.Printf("CSV: %s\n", out)
	}
	fmt.Print(r.summaryTable())
	return r.assertions()
}
