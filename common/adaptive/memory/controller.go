// Package memory sizes history's caches and the tasks its queues hold from the process's
// memory budget. Each gets a share of the memory limit, capped at its settings, and gives
// way while the heap presses on the limit, so they shrink before the garbage collector
// thrashes. See docs/dsql/adaptive-behaviour.md, Proposal 5.
//
// Like the capacity controller, controller.go is stdlib-only, and Step is a pure
// function of state, parameters and signals.
package memory

import "time"

type (
	// Value is what a controller sizes: a cache's size limit in bytes, or the multiplier
	// of the queues' pending-task limits.
	Value interface {
		~int | ~float64
	}

	// Params are the control law's parameters, for one value.
	Params[T Value] struct {
		Min, Max T // the value's bounds
		// Share is the value's share of the memory limit, and BytesPerUnit the bytes each
		// unit of the value holds: 1 for a cache's size. Its budget is that share, in
		// units, within Min and Max; with no bytes per unit, it's Max.
		Share        float64
		BytesPerUnit float64
		// HighWater and LowWater bound the heap's pressure on the limit (see Pressure).
		// Above HighWater the value shrinks by ShrinkFrac; below LowWater it grows back
		// towards its budget by GrowFrac.
		HighWater, LowWater  float64
		ShrinkFrac, GrowFrac float64
		// Interval is the least time between two changes under pressure or headroom. The
		// heap shows a change only after a collection, so it's minutes.
		Interval time.Duration
	}

	// Signals are what the controller steps on.
	Signals struct {
		Now time.Time
		// CgroupLimit is the process's cgroup memory limit, and GoLimit is GOMEMLIMIT.
		// Either is 0 when it isn't set.
		CgroupLimit, GoLimit uint64
		HeapGoal             uint64 // the heap size at which the next collection starts
		HeapLive             uint64 // the heap live after the last collection
	}

	// Reason explains a step.
	Reason string

	// Controller holds a value and the state its law keeps. It isn't safe for concurrent
	// use.
	Controller[T Value] struct {
		size       T
		lastChange time.Time
	}
)

const (
	ReasonBudget   Reason = "budget"   // over its budget: the value moves to it
	ReasonPressure Reason = "pressure" // the heap presses on the limit: the value shrinks
	ReasonHeadroom Reason = "headroom" // the heap has room: the value grows towards its budget
	ReasonCooldown Reason = "cooldown"
	ReasonSteady   Reason = "steady"
	ReasonNoData   Reason = "no-data" // no memory limit is set
)

// New returns a controller for a value of size.
func New[T Value](size T) *Controller[T] {
	return &Controller[T]{size: size}
}

// Size is the value the controller proposes.
func (c *Controller[T]) Size() T {
	return c.size
}

// Reset sets the value and clears the law's state.
func (c *Controller[T]) Reset(size T) {
	*c = Controller[T]{size: size}
}

// Step proposes the value after one step.
func (c *Controller[T]) Step(p Params[T], s Signals) (T, Reason) {
	reason := c.step(p, s)
	c.size = min(max(c.size, p.Min), p.Max)
	return c.size, reason
}

func (c *Controller[T]) step(p Params[T], s Signals) Reason {
	limit := s.Limit()
	if limit == 0 {
		return ReasonNoData
	}
	budget := p.budget(limit)
	pressure := s.Pressure()
	cooling := !c.lastChange.IsZero() && s.Now.Sub(c.lastChange) < p.Interval

	switch {
	case c.size > budget:
		c.change(s.Now, budget)
		return ReasonBudget
	case pressure > p.HighWater:
		if cooling {
			return ReasonCooldown
		}
		c.change(s.Now, c.size-stepOf(c.size, p.ShrinkFrac))
		return ReasonPressure
	case pressure < p.LowWater && c.size < budget:
		if cooling {
			return ReasonCooldown
		}
		c.change(s.Now, min(budget, c.size+stepOf(c.size, p.GrowFrac)))
		return ReasonHeadroom
	default:
		return ReasonSteady
	}
}

// budget is the value's share of limit, in units, within its bounds.
func (p Params[T]) budget(limit uint64) T {
	if p.BytesPerUnit <= 0 {
		return p.Max
	}
	return T(min(max(p.Share*float64(limit)/p.BytesPerUnit, float64(p.Min)), float64(p.Max)))
}

// Limit is the process's memory limit: the smaller of its cgroup's limit and GOMEMLIMIT,
// or 0 when neither is set.
func (s Signals) Limit() uint64 {
	switch {
	case s.CgroupLimit == 0:
		return s.GoLimit
	case s.GoLimit == 0:
		return s.CgroupLimit
	default:
		return min(s.CgroupLimit, s.GoLimit)
	}
}

// Pressure is how close the heap is to the most the limit allows. While GOMEMLIMIT is
// the limit, the runtime holds the heap goal under it, so the pressure is the live heap's
// share of the goal: a half at the default GOGC, and near 1 as collections run back to
// back, with GOGC off too. A cgroup limit alone is unknown to the runtime, so the
// pressure is the heap goal's share of it.
func (s Signals) Pressure() float64 {
	limit := s.Limit()
	switch {
	case limit == 0:
		return 0
	case s.GoLimit == limit:
		if s.HeapGoal == 0 {
			return 0
		}
		return float64(s.HeapLive) / float64(s.HeapGoal)
	default:
		return float64(s.HeapGoal) / float64(limit)
	}
}

func (c *Controller[T]) change(now time.Time, size T) {
	c.size = size
	c.lastChange = now
}

// stepOf is frac of n. An integer value moves by at least one.
func stepOf[T Value](n T, frac float64) T {
	step := T(frac * float64(n))
	if half := 0.5; T(half) == 0 {
		step = max(step, 1)
	}
	return step
}
