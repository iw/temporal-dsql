// Package memory sizes history's caches from the process's memory budget. Each cache
// gets a share of the memory limit, capped at its setting, and gives way while the heap
// presses on the limit, so caches shrink before the garbage collector thrashes. See
// docs/dsql/adaptive-behaviour.md, Proposal 5.
//
// Like the capacity controller, controller.go is stdlib-only, and Step is a pure
// function of state, parameters and signals.
package memory

import "time"

type (
	// Params are the control law's parameters, for one cache.
	Params struct {
		Min, Max int // the cache's bounds, in bytes
		// Share is the cache's share of the memory limit. Its budget is that share,
		// within Min and Max.
		Share float64
		// HighWater and LowWater bound the heap's pressure on the limit (see Pressure).
		// Above HighWater the cache shrinks by ShrinkFrac of its size; below LowWater it
		// grows back towards its budget by GrowFrac.
		HighWater, LowWater  float64
		ShrinkFrac, GrowFrac float64
		// Interval is the least time between two changes under pressure or headroom. The
		// heap shows a change in the caches only after a collection, so it's minutes.
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

	// Controller holds a cache's size limit and the state its law keeps. It isn't safe
	// for concurrent use.
	Controller struct {
		size       int
		lastChange time.Time
	}
)

const (
	ReasonBudget   Reason = "budget"   // over its budget: the cache moves to it
	ReasonPressure Reason = "pressure" // the heap presses on the limit: the cache shrinks
	ReasonHeadroom Reason = "headroom" // the heap has room: the cache grows towards its budget
	ReasonCooldown Reason = "cooldown"
	ReasonSteady   Reason = "steady"
	ReasonNoData   Reason = "no-data" // no memory limit is set
)

// New returns a controller for a cache of size bytes.
func New(size int) *Controller {
	return &Controller{size: size}
}

// Size is the cache size limit the controller proposes, in bytes.
func (c *Controller) Size() int {
	return c.size
}

// Reset sets the cache size and clears the law's state.
func (c *Controller) Reset(size int) {
	*c = Controller{size: size}
}

// Step proposes the cache's size limit after one step.
func (c *Controller) Step(p Params, s Signals) (int, Reason) {
	reason := c.step(p, s)
	c.size = min(max(c.size, p.Min), p.Max)
	return c.size, reason
}

func (c *Controller) step(p Params, s Signals) Reason {
	limit := s.Limit()
	if limit == 0 {
		return ReasonNoData
	}
	budget := min(max(int(p.Share*float64(limit)), p.Min), p.Max)
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

func (c *Controller) change(now time.Time, size int) {
	c.size = size
	c.lastChange = now
}

// stepOf is frac of n, but at least one.
func stepOf(n int, frac float64) int {
	return max(1, int(frac*float64(n)))
}
