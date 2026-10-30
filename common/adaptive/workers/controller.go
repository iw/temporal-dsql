// Package workers sizes a task scheduler's worker pool. It lifts goro.AdaptivePool's
// law into a controller that steps on the host's signals: the pool grows while tasks
// wait longer than a target to start, unless persistence is backing off, and shrinks
// slowly while it sits idle. See docs/dsql/adaptive-behaviour.md, Proposal 4.
//
// Like the capacity controller, controller.go is stdlib-only, and Step is a pure
// function of state, parameters and signals.
package workers

import "time"

type (
	// Params are the control law's parameters.
	Params struct {
		Min, Max int
		// TargetDelay is the p90 queueing delay above which the pool grows. Below a
		// quarter of it the pool counts as idle.
		TargetDelay time.Duration
		// GrowFrac and ShrinkFrac are how much the pool grows or shrinks in one step, as a
		// share of its size, but at least one worker.
		GrowFrac, ShrinkFrac float64
		// IdleSteps is how many steps in a row the pool sits idle before it shrinks.
		IdleSteps int
		// MinSamples is the fewest delays a step needs to read the pool as busy.
		MinSamples int
		// Cooldown is the least time between two changes. It should be at least the
		// span the delay covers, so the pool doesn't change twice on the same signal.
		Cooldown time.Duration
	}

	// Signals are what the controller steps on.
	Signals struct {
		Now     time.Time
		Delay   time.Duration // p90 of how long the queue's tasks waited to start
		Samples int           // how many delays Delay covers
		// BackingOff is set while persistence is backing off, when more workers would only
		// make more calls fail.
		BackingOff bool
	}

	// Reason explains a step.
	Reason string

	// Controller holds a pool's size and the state its law keeps. It isn't safe for
	// concurrent use.
	Controller struct {
		count      int
		idle       int // consecutive idle steps
		lastChange time.Time
	}
)

const (
	ReasonGrow     Reason = "grow"
	ReasonShrink   Reason = "shrink"
	ReasonBackoff  Reason = "backoff" // persistence is backing off: hold, or return to the static size
	ReasonCooldown Reason = "cooldown"
	ReasonIdle     Reason = "idle" // idle, but not yet for IdleSteps
	ReasonSteady   Reason = "steady"
)

// New returns a controller for a pool of count workers.
func New(count int) *Controller {
	return &Controller{count: count}
}

// Count is the pool size the controller proposes.
func (c *Controller) Count() int {
	return c.count
}

// Reset sets the pool size and clears the law's state.
func (c *Controller) Reset(count int) {
	*c = Controller{count: count}
}

// Step proposes the pool size after one step. static is the configured size, which
// the pool returns to, but not below, while persistence backs off.
func (c *Controller) Step(p Params, static int, s Signals) (int, Reason) {
	reason := c.step(p, static, s)
	c.count = min(max(c.count, p.Min), p.Max)
	return c.count, reason
}

func (c *Controller) step(p Params, static int, s Signals) Reason {
	busy := s.Samples >= p.MinSamples
	idle := !busy || s.Delay < p.TargetDelay/4
	if !idle || s.BackingOff {
		c.idle = 0
	}
	cooling := !c.lastChange.IsZero() && s.Now.Sub(c.lastChange) < p.Cooldown

	switch {
	case s.BackingOff:
		if c.count <= static {
			return ReasonBackoff
		}
		if cooling {
			return ReasonCooldown
		}
		c.change(s.Now, -min(c.count-static, stepOf(c.count, p.GrowFrac)))
		return ReasonBackoff
	case busy && s.Delay > p.TargetDelay:
		if cooling {
			return ReasonCooldown
		}
		c.change(s.Now, stepOf(c.count, p.GrowFrac))
		return ReasonGrow
	case idle:
		c.idle++
		if c.idle < p.IdleSteps || cooling {
			return ReasonIdle
		}
		c.idle = 0
		c.change(s.Now, -stepOf(c.count, p.ShrinkFrac))
		return ReasonShrink
	default:
		return ReasonSteady
	}
}

func (c *Controller) change(now time.Time, delta int) {
	c.count += delta
	c.lastChange = now
}

// stepOf is frac of n, but at least one.
func stepOf(n int, frac float64) int {
	return max(1, int(frac*float64(n)))
}
