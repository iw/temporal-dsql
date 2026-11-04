package adaptivesim

import (
	"fmt"
	"time"

	"go.temporal.io/server/common/adaptive"
)

// epoch is when every run starts, so runs are reproducible to the nanosecond.
var epoch = time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)

type (
	// Options are how a run is recorded.
	Options struct {
		// TraceDir, when set, is where each host writes its signal plane trace.
		TraceDir string
	}

	world struct {
		sc     *Scenario
		opts   Options
		l      *loop
		clock  simClock
		start  time.Time
		log    *runLog
		dsql   *dsql
		shared *tokenBucket
		hosts  []*process
	}

	// Result is what a run did.
	Result struct {
		Scenario    string
		Seed        uint64
		Duration    time.Duration
		Drain       time.Duration
		Hosts       []HostResult
		Connections ConnectionResult
	}

	// HostResult is what one history host did.
	HostResult struct {
		Name string
		// Started is when the host started, after the run's start.
		Started       time.Duration
		Tasks         TaskStats
		Workflows     WorkflowStats
		Reads, Writes CallStats
		Store         StoreStats
		Pool          PoolStats
		PoolLoad      PoolLoadStats
		// Decisions are the host's controllers' decisions, in the order they made them.
		Decisions Decisions
	}

	// Decisions are controllers' decisions, in the order they made them.
	Decisions []adaptive.Decision

	// PoolStats is what the host's pool did, as pgxpool's Stat reports it, with the
	// connections it opened and the gate's admissions that waited.
	PoolStats struct {
		Opened, LifetimeDestroys                  int
		Acquires, EmptyAcquires, CanceledAcquires int64
		GateWaited                                int64
	}

	decisionSeries struct {
		controller, key string
		decisions       Decisions
	}

	// ConnectionResult is the cluster's connection attempts, each second and in all.
	ConnectionResult struct {
		PerSecond []ConnCounts
		Total     ConnCounts
		// PeakOpen is the most connections DSQL held at once.
		PeakOpen int
	}
)

// Run runs a scenario.
func Run(sc Scenario, opts Options) (*Result, error) {
	if err := sc.validate(); err != nil {
		return nil, fmt.Errorf("scenario %q: %w", sc.Name, err)
	}
	l := newLoop(epoch)
	w := &world{sc: &sc, opts: opts, l: l, clock: simClock{l: l}, start: epoch, log: newRunLog()}
	w.dsql = newDSQL(l, sc.Cluster, sc.Network, sc.Seed)
	if sc.Host.Pacing.Shared {
		w.shared = newTokenBucket(sc.Host.Pacing.Rate, sc.Host.Pacing.Burst, epoch)
	}
	starts := randoms(sc.Seed, "starts")
	for i := range sc.Hosts {
		p := &process{w: w, name: fmt.Sprintf("history-%d", i+1), pid: i + 1, seed: sc.Seed}
		w.hosts = append(w.hosts, p)
		l.at(epoch.Add(between(starts, 0, sc.StartWindow)), p.start)
	}
	l.run(epoch.Add(sc.Duration + sc.Drain))
	w.stop()
	if err := w.log.err(); err != nil {
		return nil, fmt.Errorf("scenario %q: %w", sc.Name, err)
	}
	return w.result(), nil
}

// stop stops the planes, which closes their traces.
func (w *world) stop() {
	for _, p := range w.hosts {
		if p.plane != nil {
			p.plane.stop()
		}
	}
}

func (w *world) result() *Result {
	r := &Result{Scenario: w.sc.Name, Seed: w.sc.Seed, Duration: w.sc.Duration, Drain: w.sc.Drain}
	for _, p := range w.hosts {
		if p.history == nil {
			continue // it hadn't started
		}
		r.Hosts = append(r.Hosts, p.result())
	}
	r.Connections.PerSecond = w.dsql.counts.secs
	for _, c := range w.dsql.counts.secs {
		r.Connections.Total.add(c)
	}
	r.Connections.PeakOpen = w.dsql.peakOpen
	return r
}

// Of returns one controller's decisions.
func (ds Decisions) Of(controller string) Decisions {
	var of Decisions
	for _, d := range ds {
		if d.Controller == controller {
			of = append(of, d)
		}
	}
	return of
}

// series returns the decisions by controller, and for a controller with a decision for
// each of several keys, by key, in the order each first decided.
func (ds Decisions) series() []decisionSeries {
	var all []decisionSeries
	index := make(map[[2]string]int)
	for _, d := range ds {
		id := [2]string{d.Controller, decisionKey(d)}
		i, ok := index[id]
		if !ok {
			i = len(all)
			index[id] = i
			all = append(all, decisionSeries{controller: id[0], key: id[1]})
		}
		all[i].decisions = append(all[i].decisions, d)
	}
	return all
}

// decisionKey is what a controller decides for, when it decides for several things.
func decisionKey(adaptive.Decision) string {
	return ""
}

func (c *ConnCounts) add(o ConnCounts) {
	c.Dials += o.Dials
	c.Cancels += o.Cancels
	c.Opens += o.Opens
	c.RejectedRate += o.RejectedRate
	c.RejectedCap += o.RejectedCap
	c.Cuts += o.Cuts
}

// Opens is the connections opened a second over [from, to) after the run's start.
func (r ConnectionResult) Opens(from, to time.Duration) float64 {
	opens := 0
	for i, c := range r.PerSecond {
		if at := time.Duration(i) * time.Second; at >= from && at < to {
			opens += c.Opens
		}
	}
	if to <= from {
		return 0
	}
	return float64(opens) / (to - from).Seconds()
}

// Attempts sums the connection attempts, dials and cancel requests, over [from, to) after
// the run's start, and returns them a second.
func (r ConnectionResult) Attempts(from, to time.Duration) (perSecond float64, cancels int, total int) {
	for i, c := range r.PerSecond {
		at := time.Duration(i) * time.Second
		if at < from || at >= to {
			continue
		}
		total += c.Dials + c.Cancels
		cancels += c.Cancels
	}
	if to > from {
		perSecond = float64(total) / (to - from).Seconds()
	}
	return perSecond, cancels, total
}
