// Package capacity is a single per-host persistence capacity controller.
//
// It replaces the per-priority HealthRequestRateLimiterImpl instances built in
// common/persistence/client/quotas.go (see the TODO at line 238 in v1.32.1).
// Instead of an operator-chosen absolute LatencyThreshold on *average*
// latency, it learns a latency baseline (sliding-window minimum of recent p99)
// and treats baseline×Tolerance as the target.
//
// The control law borrows from TCP BBR and Cubic:
//   - On overload, cut to a fraction of the throughput the database actually
//     delivered (the best live estimate of capacity), never more than
//     MaxDecrease in one step, at most once per Cooldown.
//   - After a cut, keep the most the database delivered, as BBR's max-filter
//     keeps a path's bandwidth. During a timeout storm the delivered rate
//     collapses, so a cut can go below capacity; once the database is healthy
//     again, the rate returns to that fraction of what it delivered after the cut.
//   - Probe upward only while the limit is binding (anti-windup); probe
//     slowly near the rate where overload was last seen, accelerating the
//     longer the system stays healthy. With CoupledBindingShare set, the
//     limit of history's task scheduler, whose rate is coupled to this one,
//     binds for it too.
//
// The controller reports when its learned baseline moves by BaselineAlert, since a
// database that slows down raises the target with it.
//
// When the store's plugin reports facts (docs/dsql/adaptive-store-facts.md), the
// controller acts on those and on timeouts instead, and runs the latency law only
// in shadow.
//
// Dynamic config supplies an Envelope, not the value.
//
// The package is deliberately stdlib-only and side-effect free: Step is a pure
// function of (state, envelope, signals), which makes it trivially testable
// against recorded or simulated signal traces.
package capacity

import (
	"math"
	"time"
)

// Mode selects how the controller's output is used.
type Mode int

const (
	// ModeOff applies the static dynamic-config value, exactly as today.
	ModeOff Mode = iota
	// ModeShadow runs the controller and reports proposals but applies the static value.
	ModeShadow
	// ModeOn applies the controller's output, clamped to the envelope.
	ModeOn
)

// Envelope is the only thing an operator configures. Every field has a
// precedent somewhere in 1.32 (shadow mode, MaxRate, Min/Max, hysteresis).
type Envelope struct {
	Mode Mode

	// Absolute bounds on the admitted rate. MaxRPS may exceed the static
	// MaxQPS: the static value becomes a starting point, not a ceiling.
	MinRPS, MaxRPS float64

	// Target p99 = baseline p99 × Tolerance, capped at LatencyCeiling.
	// The ceiling guards against slow baseline drift on a degrading database.
	Tolerance      float64
	LatencyCeiling time.Duration
	// Baselines below this are treated as this (avoids reacting to noise on
	// very fast stores).
	LatencyFloor time.Duration

	// MaxErrorRatio of unhealthy errors (timeouts, deadline exceeded), plus calls
	// the store reports as throttled, that triggers a decrease regardless of latency.
	MaxErrorRatio float64

	// MaxQueuedRatio of the store plugin's connection acquires that had to wait
	// for a free connection, which triggers a decrease: the plugin's own pool is
	// short.
	MaxQueuedRatio float64

	// QueuedGrace is how long after the controller's first step the queued share
	// doesn't trigger a decrease. A host's first seconds bring a burst of start-up
	// work, such as history reloading each shard's queues as it takes the shard,
	// which queues on a pool sized for the load that follows. 0 turns it off.
	QueuedGrace time.Duration

	// MaxDecrease is the smallest multiplicative factor applied in one
	// decision (0.5 = at most halve). DeliveryBackoff is the fraction of
	// measured throughput to cut to on overload.
	MaxDecrease     float64
	DeliveryBackoff float64

	// DeliveryWindow is how long after a cut the controller keeps the most the
	// store delivered, over signal windows that start after the cut. While the store
	// is healthy and the limit binds within it, the rate returns at once to
	// DeliveryBackoff times that, never above the rate before the cut. 0 turns it off.
	DeliveryWindow time.Duration

	// IncreaseFrac is the multiplicative probe step per Step while healthy and
	// binding. Within 15% of the last overload rate the step starts at 10% of
	// IncreaseFrac and ramps to the full step over ProbeRamp.
	IncreaseFrac float64
	ProbeRamp    time.Duration

	// Cooldown is the minimum time between decreases. It should be >= the
	// signal window, so the controller never acts twice on the same stale data.
	Cooldown time.Duration

	// BaselineWindow is how long the learned minimum is remembered.
	BaselineWindow time.Duration

	// BaselineAlert is the factor by which the learned baseline moves, up or down,
	// before Step reports it. It's measured from the first baseline learned over a
	// whole BaselineWindow, then from each one reported. 0 turns it off.
	BaselineAlert float64

	// MinSamples below which the controller holds.
	MinSamples int

	// CoupledBindingShare counts the refusals of history's task scheduler as the limit
	// binding while the calls offered reach this share of the calls the scheduler lets
	// through, CoupledHeadroom times the rate. The scheduler's rate is coupled to the
	// persistence rate and keeps the calls at its Headroom of it, so while the scheduler's
	// limit binds, this one denies nothing, and the controller would hold a cut as idle.
	// 0 turns it off.
	CoupledBindingShare float64 `json:",omitempty"`
}

// DefaultEnvelope derives sane bounds from the existing static MaxQPS. Note
// what is absent: no latency threshold in milliseconds.
func DefaultEnvelope(staticRPS float64) Envelope {
	return Envelope{
		Mode:            ModeOn,
		MinRPS:          0.05 * staticRPS,
		MaxRPS:          3 * staticRPS,
		Tolerance:       2.0,
		LatencyCeiling:  750 * time.Millisecond,
		LatencyFloor:    2 * time.Millisecond,
		MaxErrorRatio:   0.01,
		MaxQueuedRatio:  0.25,
		QueuedGrace:     30 * time.Second,
		MaxDecrease:     0.5,
		DeliveryBackoff: 0.9,
		DeliveryWindow:  30 * time.Second,
		IncreaseFrac:    0.05,
		ProbeRamp:       30 * time.Second,
		Cooldown:        6 * time.Second,
		BaselineWindow:  10 * time.Minute,
		BaselineAlert:   2,
		MinSamples:      50,
	}
}

// Signals is one snapshot from the host's signal plane.
type Signals struct {
	Now        time.Time
	P99        time.Duration // over the most recent signal window only
	ErrorRatio float64       // unhealthy errors / requests over the same window
	Throughput float64       // completions per second the store delivered over the same window
	Span       time.Duration // how long that window is
	Samples    int
	Binding    bool // the limiter rejected requests since the previous Step

	// FactsReported is set when the store's plugin reports facts. The controller
	// then relies on them and on timeouts, and runs the latency law only in shadow.
	FactsReported  bool
	ThrottledRatio float64 // calls the store reported as throttled / requests
	QueuedRatio    float64 // plugin connection acquires that waited / all acquires

	// Rejected counts the calls the limiter rejected over the same window. With Samples,
	// they're the calls offered.
	Rejected int `json:",omitempty"`
	// CoupledRefusals counts the tasks history's task scheduler refused over the same
	// window while its rate, coupled to the persistence rate, was below its static rate.
	// CoupledHeadroom is the share of the persistence rate the scheduler coupled at.
	CoupledRefusals int     `json:",omitempty"`
	CoupledHeadroom float64 `json:",omitempty"`
}

// Name is the controller's name in its decisions and their metric.
const Name = "persistence_capacity"

// Reason explains a decision; emit it as a metric tag, as matching does for
// poller scaling decisions.
type Reason string

const (
	ReasonOff       Reason = "off"
	ReasonNoData    Reason = "no-data"
	ReasonErrors    Reason = "errors"
	ReasonThrottled Reason = "throttled"
	ReasonQueued    Reason = "queued"
	ReasonLatency   Reason = "latency"
	ReasonCooldown  Reason = "cooldown"
	ReasonIdle      Reason = "idle" // healthy but not binding: hold (anti-windup)
	ReasonProbe     Reason = "probe"
	ReasonRecover   Reason = "recover" // back to what the store delivered after a cut
)

// Overloaded reports whether a step with this reason found the store overloaded: it
// cut the rate, or would have but for the cooldown.
func (r Reason) Overloaded() bool {
	switch r {
	case ReasonErrors, ReasonThrottled, ReasonQueued, ReasonLatency, ReasonCooldown:
		return true
	default:
		return false
	}
}

// Decision is the controller's output for one Step.
type Decision struct {
	RPS      float64 // value to apply
	Proposed float64 // what the controller wants (differs from RPS in shadow mode)
	Target   time.Duration
	Baseline time.Duration
	Reason   Reason
	// LatencyShadow is set when the store reports facts and the latency law would
	// have cut. The controller didn't; it reports it so the two can be compared.
	LatencyShadow bool
	// Delivered is the most the store delivered since the last cut, over signal
	// windows that start after it, while DeliveryWindow lasts.
	Delivered float64
	// BaselineMovedFrom is set on a step where the learned baseline has moved by
	// BaselineAlert: the baseline it moved from.
	BaselineMovedFrom time.Duration
}

// Detail is a step as its decision's detail records it: what Step took and returned,
// so a trace can replay it.
type Detail struct {
	Envelope Envelope
	Signals  Signals
	Decision Decision
	// Start is the rate the law had before the step, when it took it other than by a
	// step: the rate it started at, or the static rate it tracked while the controller was
	// off. A frozen decision has a detail only for Start.
	Start float64 `json:",omitempty"`
}

// Controller holds the learned state. It is not safe for concurrent use; the
// owner calls Step from a single ticker goroutine and publishes RPS atomically.
type Controller struct {
	rate         float64
	started      time.Time // the first step while not off, which QueuedGrace runs from
	lastDecrease time.Time
	lastBadRate  float64 // capacity estimate when overload was last detected
	baseline     windowMin

	// The last cut: when it was, the rate before it, and the most the store has
	// delivered since.
	lastCut   time.Time
	beforeCut float64
	delivered float64

	// The baseline the alert measures from, when the baseline's current run of
	// samples began, and the latest sample.
	settled  time.Duration
	learning time.Time
	lastData time.Time
}

// New returns a controller starting at initialRPS (normally the static MaxQPS).
func New(initialRPS float64) *Controller {
	return &Controller{rate: initialRPS}
}

// Rate returns the controller's current internal rate.
func (c *Controller) Rate() float64 { return c.rate }

// Step advances the controller by one tick.
func (c *Controller) Step(env Envelope, staticRPS float64, s Signals) Decision {
	if env.Mode == ModeOff {
		// Track the static value so switching Off->On is bumpless.
		c.rate = staticRPS
		return Decision{RPS: staticRPS, Proposed: staticRPS, Reason: ReasonOff}
	}
	if c.started.IsZero() {
		c.started = s.Now
	}

	d := Decision{Reason: ReasonNoData}
	if s.Samples >= env.MinSamples && s.P99 > 0 {
		c.baseline.push(s.Now, s.P99, env.BaselineWindow)
		base := max(c.baseline.min(), env.LatencyFloor)
		target := min(time.Duration(float64(base)*env.Tolerance), env.LatencyCeiling)
		d.Baseline, d.Target = base, target
		d.BaselineMovedFrom = c.baselineMoved(env, s.Now, base)
		d.Delivered = c.recordDelivered(env, s)

		errorsHigh := s.ErrorRatio > env.MaxErrorRatio
		throttledHigh := !errorsHigh && s.ThrottledRatio > 0 && s.ErrorRatio+s.ThrottledRatio > env.MaxErrorRatio
		queuedHigh := env.MaxQueuedRatio > 0 && s.QueuedRatio > env.MaxQueuedRatio &&
			s.Now.Sub(c.started) >= env.QueuedGrace
		factsHigh := errorsHigh || throttledHigh || queuedHigh
		latencyHigh := s.P99 > target
		if s.FactsReported {
			// The store says what it did, so latency runs only in shadow.
			d.LatencyShadow = latencyHigh && !factsHigh
			latencyHigh = false
		}
		binding := s.Binding || coupledBinding(env, s, c.rate)
		switch {
		case factsHigh || latencyHigh:
			if !c.lastDecrease.IsZero() && s.Now.Sub(c.lastDecrease) < env.Cooldown {
				d.Reason = ReasonCooldown
				break
			}
			switch {
			case errorsHigh:
				d.Reason = ReasonErrors
			case throttledHigh:
				d.Reason = ReasonThrottled
			case queuedHigh:
				d.Reason = ReasonQueued
			default:
				d.Reason = ReasonLatency
			}
			// Prefer the measured delivery rate; fall back to a latency gradient.
			next := c.rate * math.Max(env.MaxDecrease, float64(target)/float64(s.P99))
			if s.Throughput > 0 {
				next = s.Throughput * env.DeliveryBackoff
			} else if factsHigh {
				next = c.rate * env.MaxDecrease
			}
			c.lastBadRate = c.rate
			if s.Throughput > 0 {
				c.lastBadRate = s.Throughput // delivered rate = live capacity estimate
			}
			before := c.rate
			c.rate = math.Min(c.rate, math.Max(next, c.rate*env.MaxDecrease))
			c.lastDecrease = s.Now
			if c.rate < before {
				// What the store delivered before the cut doesn't say what it delivers now.
				c.lastCut, c.beforeCut, c.delivered = s.Now, before, 0
				d.Delivered = 0
			}
		case !binding:
			d.Reason = ReasonIdle
		default:
			step := env.IncreaseFrac
			if c.lastBadRate > 0 && c.rate >= 0.85*c.lastBadRate && env.ProbeRamp > 0 {
				ramp := float64(s.Now.Sub(c.lastDecrease)) / float64(env.ProbeRamp)
				step *= math.Min(1, math.Max(0.1, ramp))
			}
			c.rate *= 1 + step
			d.Reason = ReasonProbe
			// A cut made while timeouts took the delivered rate down can go below what
			// the store delivers once it recovers, so return to that at once instead of
			// probing back up to it, and probe slowly from there.
			if recovered := min(d.Delivered*env.DeliveryBackoff, c.beforeCut); recovered > c.rate {
				c.rate = recovered
				c.lastBadRate = d.Delivered
				d.Reason = ReasonRecover
			}
		}
	}

	c.rate = math.Min(math.Max(c.rate, env.MinRPS), env.MaxRPS)
	d.Proposed = c.rate
	d.RPS = c.rate
	if env.Mode == ModeShadow {
		d.RPS = staticRPS
	}
	return d
}

// coupledBinding reports whether the task scheduler's coupled limit binds for a controller
// at rate: the scheduler refused tasks, and the calls offered reach CoupledBindingShare of
// the calls it lets through. Calls offered well below them say that something other than
// the rate held the tasks back, such as a mix of calls the scheduler's rate hasn't caught
// up with yet.
func coupledBinding(env Envelope, s Signals, rate float64) bool {
	if env.CoupledBindingShare <= 0 || s.CoupledRefusals <= 0 || s.CoupledHeadroom <= 0 || s.Span <= 0 {
		return false
	}
	offered := float64(s.Samples+s.Rejected) / s.Span.Seconds()
	return offered >= env.CoupledBindingShare*s.CoupledHeadroom*rate
}

// recordDelivered keeps the most the store has delivered since the last cut, over
// signal windows that start after it, and returns it until DeliveryWindow has passed.
func (c *Controller) recordDelivered(env Envelope, s Signals) float64 {
	if env.DeliveryWindow <= 0 || c.lastCut.IsZero() {
		return 0
	}
	since := s.Now.Sub(c.lastCut)
	if since > env.DeliveryWindow {
		return 0
	}
	if since >= s.Span {
		c.delivered = max(c.delivered, s.Throughput)
	}
	return c.delivered
}

// baselineMoved returns the baseline that base moved from, when it has moved by
// BaselineAlert from the baseline the alert measures from, and 0 otherwise. The alert
// settles on a baseline once the baseline has a whole BaselineWindow of samples, with
// no gap long enough for the window to forget them.
func (c *Controller) baselineMoved(env Envelope, now time.Time, base time.Duration) time.Duration {
	if c.lastData.IsZero() || now.Sub(c.lastData) > env.BaselineWindow {
		c.learning = now
	}
	c.lastData = now
	if env.BaselineAlert <= 1 || now.Sub(c.learning) < env.BaselineWindow {
		return 0
	}
	from := c.settled
	if from == 0 {
		c.settled = base
		return 0
	}
	if float64(base) < float64(from)*env.BaselineAlert && float64(base)*env.BaselineAlert > float64(from) {
		return 0
	}
	c.settled = base
	return from
}

// windowMin is a sliding-window minimum (monotonic deque).
type windowMin struct {
	ts []time.Time
	vs []time.Duration
}

func (w *windowMin) push(now time.Time, v time.Duration, window time.Duration) {
	for len(w.vs) > 0 && w.vs[len(w.vs)-1] >= v {
		w.ts, w.vs = w.ts[:len(w.ts)-1], w.vs[:len(w.vs)-1]
	}
	w.ts, w.vs = append(w.ts, now), append(w.vs, v)
	for len(w.ts) > 0 && now.Sub(w.ts[0]) > window {
		w.ts, w.vs = w.ts[1:], w.vs[1:]
	}
}

func (w *windowMin) min() time.Duration {
	if len(w.vs) == 0 {
		return 0
	}
	return w.vs[0]
}
