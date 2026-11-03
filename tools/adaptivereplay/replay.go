// Package adaptivereplay replays the signal plane's traces through the adaptive
// controllers' laws offline. Each decision in a trace records what its law stepped on,
// so a replay with the recorded parameters gives the recorded proposals, and one with
// other parameters shows what they would have proposed on the same signals. See
// docs/dsql/adaptive-behaviour.md, "Offline replay".
package adaptivereplay

import (
	"math"
	"time"

	"go.temporal.io/server/common/adaptive"
)

type (
	// Replay is a recording replayed: its controllers' series, and the decisions of
	// controllers the tool doesn't know, by controller.
	Replay struct {
		Trace   *Trace
		Series  []*Series
		Unknown map[string]int
	}

	// Series is one controller's decisions in a recording, replayed: for the partition
	// scaler, one task queue's, and for fair share, one namespace's under one limit.
	Series struct {
		Controller string
		Key        string
		Steps      []Step
	}

	// Step is a decision and its replay.
	Step struct {
		Time   time.Time
		Static float64
		// Recorded and Replayed are the proposals, and RecordedReason and ReplayedReason
		// their reasons.
		Recorded, Replayed             float64
		RecordedReason, ReplayedReason adaptive.Reason
		// Stepped is set when the replayed reason is the law's. On a decision the host held,
		// as while the controller was frozen, it's the recorded one.
		Stepped bool
	}

	seriesKey struct {
		controller, key string
	}

	replaying struct {
		series *Series
		law    law
	}
)

// Run replays a recording's decisions, of the controllers include accepts, with the
// overrides. It reports the overrides that applied to a decision in used.
func Run(t *Trace, overrides []Override, include func(controller string) bool, used map[Override]bool) (*Replay, error) {
	r := &Replay{Trace: t, Unknown: make(map[string]int)}
	byKey := make(map[seriesKey]*replaying)
	for i := range t.Decisions {
		d := &t.Decisions[i]
		if include != nil && !include(d.Controller) {
			continue
		}
		kind, ok := kindOf(d.Controller)
		if !ok {
			r.Unknown[d.Controller]++
			continue
		}
		key := seriesKey{controller: d.Controller}
		if kind.key != nil && len(d.Detail) > 0 {
			var err error
			if key.key, err = kind.key(d); err != nil {
				return nil, err
			}
		}
		s, ok := byKey[key]
		if !ok {
			s = &replaying{series: &Series{Controller: key.controller, Key: key.key}, law: kind.newLaw()}
			byKey[key] = s
			r.Series = append(r.Series, s.series)
		}
		proposal, reason, stepped, err := s.law.step(d, overrides)
		if err != nil {
			return nil, err
		}
		for _, o := range overrides {
			if used != nil && o.appliesTo(d.Controller) {
				used[o] = true
			}
		}
		s.series.Steps = append(s.series.Steps, Step{
			Time:           d.Time,
			Static:         d.Static,
			Recorded:       d.Proposed,
			Replayed:       proposal,
			RecordedReason: d.Reason,
			ReplayedReason: reason,
			Stepped:        stepped,
		})
	}
	return r, nil
}

// Matches reports whether the replay gave the step's recorded proposal and reason.
func (s Step) Matches() bool {
	return same(s.Recorded, s.Replayed) && s.RecordedReason == s.ReplayedReason
}

// Matched is how many of the series' steps the replay reproduced.
func (s *Series) Matched() int {
	n := 0
	for _, step := range s.Steps {
		if step.Matches() {
			n++
		}
	}
	return n
}

// same reports whether two proposals are equal, but for rounding: a law built where the
// compiler fuses multiply-adds can differ in the last bits from one built where it doesn't.
func same(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*max(1, math.Abs(a), math.Abs(b))
}
