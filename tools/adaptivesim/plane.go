package adaptivesim

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/signals"
)

type (
	// plane is a simulated host's signal plane: the real signals.Plane, which reads the
	// host's signals from the simulation and is ticked on simulated time. The controllers
	// that step on its snapshots subscribe to it through signals.Drive, as they do on a
	// real host, and it writes its trace with the real trace writer, so
	// tools/adaptivereplay reads it. It also keeps each decision for the run's result.
	plane struct {
		l         *loop
		interval  time.Duration
		signals   *signals.Plane
		decisions []adaptive.Decision
	}

	// runLog is the logger the simulated planes write to. It keeps their errors, such as
	// a trace file that can't be written, so a run can return them.
	runLog struct {
		log.Logger
		errs []error
	}
)

var _ adaptive.DecisionSink = (*plane)(nil)

// newPlane returns a host's plane. Its trace, when traceDir is set, is named as the
// signal plane names its files, from header.
func newPlane(l *loop, cfg Signals, source signals.Source, header signals.Header, traceDir string, logger log.Logger) *plane {
	pl := &plane{l: l, interval: cfg.PlaneInterval}
	settings := signals.Settings{
		Enabled:        cfg.PlaneInterval > 0,
		Interval:       cfg.PlaneInterval,
		Windows:        cfg.PlaneWindows,
		TraceDirectory: traceDir,
		TraceMaxBytes:  signals.DefaultSettings.TraceMaxBytes,
	}
	pl.signals = signals.NewPlane(func() signals.Settings { return settings }, source, pl, simClock{l: l}, header, logger)
	return pl
}

func (pl *plane) start() {
	if pl.interval > 0 {
		pl.l.after(pl.interval, pl.tick)
	}
}

func (pl *plane) tick() {
	pl.signals.Tick(pl.l.now)
	pl.l.after(pl.interval, pl.tick)
}

// RecordDecision keeps a decision the plane passes on, for the run's result.
func (pl *plane) RecordDecision(d adaptive.Decision) {
	pl.decisions = append(pl.decisions, d)
}

// report reports a decision of a controller that doesn't step on the plane's snapshots,
// as the persistence rate limiter reports its capacity controller's.
func (pl *plane) report(d adaptive.Decision) {
	pl.signals.RecordDecision(d)
}

// stop closes the plane's trace.
func (pl *plane) stop() {
	pl.signals.Stop()
}

func newRunLog() *runLog {
	return &runLog{Logger: log.NewNoopLogger()}
}

func (r *runLog) Error(msg string, tags ...tag.Tag) {
	parts := []string{msg}
	for _, t := range tags {
		parts = append(parts, fmt.Sprintf("%s=%v", t.Key(), t.Value()))
	}
	r.errs = append(r.errs, errors.New(strings.Join(parts, " ")))
}

func (r *runLog) err() error {
	return errors.Join(r.errs...)
}
