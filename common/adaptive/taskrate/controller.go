// Package taskrate couples history's task scheduler to persistence: the scheduler
// admits tasks at the rate the persistence rate supports, given how many persistence
// calls each task attempt makes. Tasks then wait in the scheduler, which is cheap,
// instead of failing on persistence's limit mid-execution and going back through the
// rescheduler. See docs/dsql/adaptive-behaviour.md, Proposal 3.
//
// Like the capacity controller, controller.go is stdlib-only, and Step is a pure
// function of state, parameters and signals.
package taskrate

type (
	// Params are the control law's parameters.
	Params struct {
		Min, Max float64
		// Headroom is the share of the persistence rate the scheduler plans to use, so the
		// persistence limit doesn't bind at the margin.
		Headroom float64
		// MinSamples is the fewest persistence calls a step needs to measure the mix.
		MinSamples int
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
	}
)

const (
	// ReasonCoupled sets the rate from the persistence rate and the measured mix.
	ReasonCoupled Reason = "coupled"
	// ReasonNoData holds the rate: the step saw too few calls or no task attempts.
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

// Reset sets the rate.
func (c *Controller) Reset(rate float64) {
	c.rate = rate
}

// Step proposes the scheduler's rate: the share of the persistence rate it plans to use,
// divided by the calls each task attempt makes, which Attempts/Calls measures. Calls
// that aren't the tasks' count against them, so API traffic keeps its share.
func (c *Controller) Step(p Params, s Signals) (float64, Reason) {
	reason := ReasonNoData
	if s.Calls >= p.MinSamples && s.Attempts > 0 && s.PersistenceRate > 0 {
		c.rate = s.PersistenceRate * p.Headroom * float64(s.Attempts) / float64(s.Calls)
		reason = ReasonCoupled
	}
	c.rate = min(max(c.rate, p.Min), p.Max)
	return c.rate, reason
}
