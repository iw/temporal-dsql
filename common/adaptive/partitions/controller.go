// Package partitions sizes a task queue's partitions from the tasks a second each
// partition can dispatch, which it learns. Matching's simple partition scaler is told that
// capacity, as TargetRate. This controller starts from a capacity it's given and measures
// it: when the task queue falls behind, adding tasks faster than it dispatches them, it adds
// partitions as a trial, and keeps them only if dispatch rises. A trial that raised dispatch
// measures the capacity. A task queue held back by its workers gains nothing from more
// partitions, so its trials fail and the controller stops running them for a while. See
// docs/dsql/adaptive-behaviour.md, Proposal 6.
//
// Like the other controllers, controller.go is stdlib-only, and Step is a pure function of
// state, parameters and signals.
package partitions

import (
	"math"
	"time"
)

type (
	// Params are the control law's parameters.
	Params struct {
		Min, Max int
		// MaxStepFrac is the largest change in one step, as a share of the partitions, but
		// at least one partition.
		MaxStepFrac float64
		// Capacity is the tasks a second one partition is taken to dispatch until the
		// controller measures it.
		Capacity float64
		// Headroom is the share of capacity the partitions keep spare: they grow to dispatch
		// the add rate with Headroom to spare, and shrink only while they'd keep twice that.
		Headroom float64
		// MinRate is the add rate below which partitions don't hold a task queue back, so
		// the controller runs no trials.
		MinRate float64
		// BehindRatio is how far the add rate must pass the dispatch rate, as a share of
		// the dispatch rate, for the task queue to be behind. A trial needs it behind on two
		// steps in a row.
		BehindRatio float64
		// TrialWindow is how long a trial runs before the controller judges it. A trial
		// succeeds when dispatch rose by MinGain of the rise in proportion to the partitions
		// it added.
		TrialWindow time.Duration
		MinGain     float64
		// NoGainHold is how long the controller runs no trials after one that didn't raise
		// dispatch. It doubles with each such trial in a row, up to eight times.
		NoGainHold time.Duration
		// DownHold is how long the partitions stay before they shrink, after the task
		// queue was last behind or the partitions last changed.
		DownHold time.Duration
		// Settle is how long after the partitions change the controller waits before it
		// grows them, learns from their rates or judges a trial. Each partition's rates
		// cover its last 30 seconds, so until then the partitions it had before the change
		// still count the tasks they took then: their sum reads high while the partitions
		// grow, and low once a partition that drained stops being counted.
		Settle time.Duration
		// Shadow is set when the controller's output isn't applied. Its proposals can't
		// change dispatch, so it runs no trials.
		Shadow bool
	}

	// Signals are what the controller steps on.
	Signals struct {
		Now time.Time
		// Partitions is how many partitions tasks are added to, and Described how many
		// partitions the rates cover. They differ while partitions drain, or when a
		// partition doesn't answer.
		Partitions, Described int
		// AddRate and DispatchRate are the tasks a second added to and dispatched from all
		// the task queue's partitions.
		AddRate, DispatchRate float64
		// RateLimited is set while a partition's dispatch is rate limited, which holds
		// dispatch to a rate an operator set.
		RateLimited bool
		// BackingOff is set while persistence is backing off, when more partitions would
		// only add calls.
		BackingOff bool
	}

	// Reason explains a step.
	Reason string

	// Controller holds a task queue's learned capacity and the state its law keeps. It
	// isn't safe for concurrent use.
	Controller struct {
		capacity     float64 // tasks a second per partition, once measured or seen
		behind       int     // steps in a row the task queue was behind
		trial        trial
		failed       int // trials in a row that didn't raise dispatch
		noTrialUntil time.Time
		lastBehind   time.Time
		// partitions and described are the counts the previous step saw, and lastChange
		// when either last changed. Steps see the counts, so a proposal that isn't
		// applied isn't a change. peak is the highest dispatch rate since the rates
		// settled after it.
		partitions, described int
		lastChange            time.Time
		peak                  float64
	}

	// trial is a step up the controller is testing.
	trial struct {
		start    time.Time
		from, to int
		dispatch float64 // the highest settled dispatch rate before it started
	}
)

const (
	ReasonNoData  Reason = "no-data"
	ReasonSteady  Reason = "steady"
	ReasonRate    Reason = "rate" // sized from the add rate and the capacity
	ReasonTrial   Reason = "trial"
	ReasonLearned Reason = "learned" // a trial raised dispatch, which measured the capacity
	// ReasonNoGain is a trial that didn't raise dispatch, which the controller undoes, or a
	// task queue behind while trials are on hold or nothing dispatches.
	ReasonNoGain      Reason = "no-gain"
	ReasonBackoff     Reason = "backoff" // persistence is backing off: no more partitions
	ReasonRateLimited Reason = "rate-limited"
	ReasonMax         Reason = "max"
	ReasonBounds      Reason = "bounds"
	ReasonSettle      Reason = "settle" // the partitions changed less than Settle ago
)

// New returns a controller that has learned capacity, or none when it's 0.
func New(capacity float64) *Controller {
	return &Controller{capacity: capacity}
}

// Learned is the capacity the controller measured or saw, or 0 before it has.
func (c *Controller) Learned() float64 {
	return c.capacity
}

// Capacity is the tasks a second the controller takes one partition to dispatch.
func (c *Controller) Capacity(p Params) float64 {
	if c.capacity > 0 {
		return c.capacity
	}
	return p.Capacity
}

// Step proposes how many partitions tasks should be added to.
func (c *Controller) Step(p Params, s Signals) (int, Reason) {
	n := s.Partitions
	if n <= 0 || c.Capacity(p) <= 0 {
		return n, ReasonNoData
	}
	settled, behind := c.observe(p, s)
	learned, undo, undone := c.judge(p, s, settled, behind)
	if undone {
		return undo, ReasonNoGain
	}
	if settled && !behind && s.AddRate >= p.MinRate && s.DispatchRate/float64(n) > c.Capacity(p) {
		// The partitions dispatched more than the capacity without falling behind.
		c.capacity = s.DispatchRate / float64(n)
	}

	target, reason, startTrial := c.size(p, s, n, behind, settled)
	if learned && (reason == ReasonSteady || reason == ReasonRate) {
		reason = ReasonLearned
	}
	target, reason = c.bound(p, n, target, reason)
	if startTrial && !p.Shadow && target > n {
		c.trial = trial{start: s.Now, from: n, to: target, dispatch: c.peak}
	}
	return target, reason
}

// observe keeps the step's partition counts and dispatch rate, and reads whether the
// rates have settled and the task queue is behind.
func (c *Controller) observe(p Params, s Signals) (settled, behind bool) {
	if s.Partitions != c.partitions || s.Described != c.described {
		c.partitions, c.described = s.Partitions, s.Described
		c.lastChange = s.Now
		c.peak = 0
	}
	settled = s.Now.Sub(c.lastChange) >= p.Settle
	if settled {
		c.peak = max(c.peak, s.DispatchRate)
	}
	behind = s.AddRate >= p.MinRate && s.AddRate > s.DispatchRate*(1+p.BehindRatio)
	if behind {
		c.behind++
		c.lastBehind = s.Now
	} else {
		c.behind = 0
	}
	return settled, behind
}

// judge ends a trial once its window has passed and the rates have settled. A trial that
// raised dispatch measures the capacity. One that didn't, while the task queue is still
// behind, is undone, and undo is the target that undoes it.
func (c *Controller) judge(p Params, s Signals, settled, behind bool) (learned bool, undo int, undone bool) {
	t := c.trial
	if t.to == 0 || !settled || s.Now.Sub(t.start) < p.TrialWindow {
		return false, 0, false
	}
	c.trial = trial{}
	n := s.Partitions
	switch {
	case n < t.to:
		// Its partitions never took effect, so it shows nothing.
		return false, 0, false
	case s.DispatchRate-t.dispatch >= p.MinGain*t.dispatch*float64(t.to-t.from)/float64(t.from):
		c.capacity = t.dispatch / float64(t.from)
		c.failed = 0
		return true, 0, false
	case behind:
		c.failed++
		c.noTrialUntil = s.Now.Add(p.NoGainHold << min(c.failed-1, 3))
		// Undo the trial's step, but not partitions the add rate needs.
		back := max(n-(t.to-t.from), need(s.AddRate*(1+p.Headroom), c.Capacity(p)))
		return false, c.clamp(p, n, min(n, back)), true
	default:
		// The task queue caught up because fewer tasks came, which shows nothing either.
		return false, 0, false
	}
}

// bound keeps a target within one step and the bounds, and says when they held it back.
func (c *Controller) bound(p Params, n, target int, reason Reason) (int, Reason) {
	bounded := c.clamp(p, n, target)
	switch {
	case bounded == target:
		return target, reason
	case bounded == n && target > n:
		return bounded, ReasonMax
	case bounded == n || reason == ReasonSteady:
		return bounded, ReasonBounds
	default:
		return bounded, reason
	}
}

// size is the target the add rate and the capacity give, and whether it's a trial.
func (c *Controller) size(p Params, s Signals, n int, behind, settled bool) (int, Reason, bool) {
	capacity := c.Capacity(p)
	up := need(s.AddRate*(1+p.Headroom), capacity)
	down := need(s.AddRate*(1+2*p.Headroom), capacity)
	switch {
	case up > n:
		switch {
		case s.BackingOff:
			return n, ReasonBackoff, false
		case !settled:
			return n, ReasonSettle, false
		}
		return up, ReasonRate, false
	case c.trial.to > 0:
		return n, ReasonTrial, false
	case behind && c.behind >= 2:
		switch {
		case s.RateLimited:
			return n, ReasonRateLimited, false
		case s.BackingOff:
			return n, ReasonBackoff, false
		case s.DispatchRate < p.MinRate || s.Now.Before(c.noTrialUntil):
			return n, ReasonNoGain, false
		case n >= p.Max:
			return n, ReasonMax, false
		case !settled:
			return n, ReasonSettle, false
		}
		return n + stepOf(n, p.MaxStepFrac), ReasonTrial, true
	case down < n && s.Now.Sub(c.lastBehind) >= p.DownHold && s.Now.Sub(c.lastChange) >= p.DownHold:
		return down, ReasonRate, false
	default:
		return n, ReasonSteady, false
	}
}

// clamp keeps a target within one step of n, then within [Min, Max]. If the bounds cross,
// Max wins.
func (c *Controller) clamp(p Params, n, target int) int {
	step := stepOf(n, p.MaxStepFrac)
	target = min(max(target, n-step), n+step)
	return min(max(target, p.Min), p.Max)
}

// need is how many partitions dispatch rate at capacity each, but at least one.
func need(rate, capacity float64) int {
	return max(1, int(math.Ceil(rate/capacity)))
}

// stepOf is frac of n, but at least one.
func stepOf(n int, frac float64) int {
	return max(1, int(frac*float64(n)))
}
