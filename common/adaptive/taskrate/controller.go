// Package taskrate couples history's task scheduler to persistence: the scheduler
// admits tasks at the rate the persistence rate supports, given how many persistence
// calls each task attempt makes. Tasks then wait in the scheduler, which is cheap,
// instead of failing on persistence's limit mid-execution and going back through the
// rescheduler. See docs/dsql/adaptive-behaviour.md, Proposal 3.
//
// Like the capacity controller, controller.go is stdlib-only, and Step is a pure
// function of state, parameters and signals.
package taskrate

import "math"

type (
	// Params are the control law's parameters.
	Params struct {
		Min, Max float64
		// Headroom is the share of the persistence rate the scheduler plans to use, so the
		// persistence limit doesn't bind at the margin.
		Headroom float64
		// MinSamples is the persistence calls a step needs to measure the mix in full.
		MinSamples int
		// Deadband holds the mix the rate was set from while the measured mix is within this
		// share of it. The rate still follows the persistence rate.
		Deadband float64
	}

	// Signals are what the controller steps on. Attempts and Calls cover the same span.
	Signals struct {
		// PersistenceRate is the host's persistence rate, in calls per second: the capacity
		// controller's while it's enabled, otherwise the static limit.
		PersistenceRate float64
		Attempts        int // task attempts the host's schedulers started
		Calls           int // persistence calls, including those the rate limiter rejected
	}

	// Detail is a step as its decision's detail records it, so a trace can replay it:
	// the law's parameters and signals, and the envelope's step limit, which holds the
	// proposal as the signal plane drives it.
	Detail struct {
		Params      Params
		Signals     Signals
		MaxStepFrac float64
	}

	// Reason explains a step.
	Reason string

	// Controller holds the scheduler's rate. It isn't safe for concurrent use.
	Controller struct {
		rate float64
		mix  float64 // the task attempts per call the rate was set from, 0 before the first
	}
)

const (
	// ReasonCoupled sets the rate from the persistence rate and the measured mix.
	ReasonCoupled Reason = "coupled"
	// ReasonPartial sets the rate from the persistence rate and a mix moved part of the way
	// to the measured one: the step saw fewer than MinSamples calls.
	ReasonPartial Reason = "partial"
	// ReasonHeld sets the rate from the persistence rate and the mix it was set from before,
	// which the step's measurement is within Deadband of.
	ReasonHeld Reason = "held"
	// ReasonLastMix sets the rate from the persistence rate and the mix it was set from
	// before: the step saw calls, but no task attempts to measure the mix.
	ReasonLastMix Reason = "last-mix"
	// ReasonNoData holds the rate: there's no persistence rate, the step saw no calls, or no
	// step with MinSamples calls has measured a mix since the controller started or was reset.
	ReasonNoData Reason = "no-data"
)

// New returns a controller at rate tasks per second.
func New(rate float64) *Controller {
	return &Controller{rate: rate}
}

// Rate is the scheduler rate the controller proposes, in tasks per second.
func (c *Controller) Rate() float64 {
	return c.rate
}

// Reset sets the rate, and forgets the mix it was set from.
func (c *Controller) Reset(rate float64) {
	c.rate = rate
	c.mix = 0
}

// Step proposes the scheduler's rate: the share of the persistence rate it plans to use,
// divided by the calls each task attempt makes, which Attempts/Calls measures. Calls
// that aren't the tasks' count against them, so API traffic keeps its share. The deadband
// holds only the mix, so the rate follows each change of the persistence rate, the
// capacity controller's cuts and probes among them.
func (c *Controller) Step(p Params, s Signals) (float64, Reason) {
	reason := ReasonNoData
	if s.PersistenceRate > 0 {
		reason = c.measure(p, s)
	}
	if reason != ReasonNoData {
		c.rate = s.PersistenceRate * p.Headroom * c.mix
	}
	c.rate = min(max(c.rate, p.Min), p.Max)
	return c.rate, reason
}

// measure updates the mix from a step's attempts and calls. While the scheduler's limit
// binds, each measurement moves the rate toward the calls that make up Headroom of the
// persistence rate, where the capacity controller sees its limit bind. At a low rate few
// calls reach persistence, so a step with fewer than MinSamples calls moves the mix by its
// calls' share of MinSamples: far enough that the rate keeps moving, and little enough
// that few calls' noise mostly stays within the deadband. The first mix needs MinSamples.
//
// A step without calls holds the rate. The capacity controller steps on calls, so without
// them its decision ages past DecisionHold and the persistence rate falls back to the
// static rate, which an idle host's scheduler would otherwise climb toward.
func (c *Controller) measure(p Params, s Signals) Reason {
	if s.Calls <= 0 || c.mix == 0 && (s.Attempts <= 0 || s.Calls < p.MinSamples) {
		return ReasonNoData
	}
	if s.Attempts <= 0 {
		return ReasonLastMix
	}
	measured, weight := float64(s.Attempts)/float64(s.Calls), 1.0
	if s.Calls < p.MinSamples {
		weight = float64(s.Calls) / float64(p.MinSamples)
	}
	next := measured
	if c.mix > 0 {
		if weight < 1 {
			next = c.mix + weight*(measured-c.mix)
		}
		if math.Abs(next-c.mix) < p.Deadband*c.mix {
			return ReasonHeld
		}
	}
	c.mix = next
	if weight < 1 {
		return ReasonPartial
	}
	return ReasonCoupled
}
