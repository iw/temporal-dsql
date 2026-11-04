package adaptivesim

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/signals"
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
		l      *loop
		clock  simClock
		start  time.Time
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
		// Decisions are the host's capacity controller's decisions, while it wasn't off.
		Decisions []adaptive.Decision
	}

	// PoolStats is what the host's pool did, as pgxpool's Stat reports it, with the
	// connections it opened and the gate's admissions that waited.
	PoolStats struct {
		Opened, LifetimeDestroys                  int
		Acquires, EmptyAcquires, CanceledAcquires int64
		GateWaited                                int64
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
	w := &world{sc: &sc, l: l, clock: simClock{l: l}, start: epoch}
	w.dsql = newDSQL(l, sc.Cluster, sc.Network, sc.Seed)
	if sc.Host.Pacing.Shared {
		w.shared = newTokenBucket(sc.Host.Pacing.Rate, sc.Host.Pacing.Burst, epoch)
	}
	starts := randoms(sc.Seed, "starts")
	for i := range sc.Hosts {
		p := &process{w: w, name: fmt.Sprintf("history-%d", i+1), seed: sc.Seed}
		at := epoch.Add(between(starts, 0, sc.StartWindow))
		if opts.TraceDir != "" {
			header := signals.Header{Service: traceService, Host: p.name, PID: i + 1, Start: at}
			trace, err := openTrace(opts.TraceDir, header)
			if err != nil {
				return nil, errors.Join(err, w.closeTraces())
			}
			p.trace = trace
		}
		w.hosts = append(w.hosts, p)
		l.at(at, p.start)
	}
	l.run(epoch.Add(sc.Duration + sc.Drain))
	if err := w.closeTraces(); err != nil {
		return nil, err
	}
	return w.result(), nil
}

func (w *world) closeTraces() error {
	var errs []error
	for _, p := range w.hosts {
		errs = append(errs, p.trace.close())
	}
	return errors.Join(errs...)
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
