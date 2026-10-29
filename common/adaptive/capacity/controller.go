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
//   - Probe upward only while the limit is binding (anti-windup); probe
//     slowly near the rate where overload was last seen, accelerating the
//     longer the system stays healthy.
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

	// MaxQueuedRatio of calls that waited for one of the store plugin's
	// connections that triggers a decrease: the plugin's own pool is short.
	MaxQueuedRatio float64

	// MaxDecrease is the smallest multiplicative factor applied in one
	// decision (0.5 = at most halve). DeliveryBackoff is the fraction of
	// measured throughput to cut to on overload.
	MaxDecrease     float64
	DeliveryBackoff float64

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

	// MinSamples below which the controller holds.
	MinSamples int
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
		MaxDecrease:     0.5,
		DeliveryBackoff: 0.9,
		IncreaseFrac:    0.05,
		ProbeRamp:       30 * time.Second,
		Cooldown:        6 * time.Second,
		BaselineWindow:  10 * time.Minute,
		MinSamples:      50,
	}
}

// Signals is one snapshot from the host's signal plane.
type Signals struct {
	Now        time.Time
	P99        time.Duration // over the most recent signal window only
	ErrorRatio float64       // unhealthy errors / requests over the same window
	Throughput float64       // completions per second the store delivered over the same window
	Samples    int
	Binding    bool // the limiter rejected requests since the previous Step

	// FactsReported is set when the store's plugin reports facts. The controller
	// then relies on them and on timeouts, and runs the latency law only in shadow.
	FactsReported  bool
	ThrottledRatio float64 // calls the store reported as throttled / requests
	QueuedRatio    float64 // calls that waited for a plugin connection / requests
}

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
)

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
}

// Controller holds the learned state. It is not safe for concurrent use; the
// owner calls Step from a single ticker goroutine and publishes RPS atomically.
type Controller struct {
	rate         float64
	lastDecrease time.Time
	lastBadRate  float64 // capacity estimate when overload was last detected
	baseline     windowMin
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

	d := Decision{Reason: ReasonNoData}
	if s.Samples >= env.MinSamples && s.P99 > 0 {
		c.baseline.push(s.Now, s.P99, env.BaselineWindow)
		base := max(c.baseline.min(), env.LatencyFloor)
		target := min(time.Duration(float64(base)*env.Tolerance), env.LatencyCeiling)
		d.Baseline, d.Target = base, target

		errorsHigh := s.ErrorRatio > env.MaxErrorRatio
		throttledHigh := !errorsHigh && s.ThrottledRatio > 0 && s.ErrorRatio+s.ThrottledRatio > env.MaxErrorRatio
		queuedHigh := env.MaxQueuedRatio > 0 && s.QueuedRatio > env.MaxQueuedRatio
		factsHigh := errorsHigh || throttledHigh || queuedHigh
		latencyHigh := s.P99 > target
		if s.FactsReported {
			// The store says what it did, so latency runs only in shadow.
			d.LatencyShadow = latencyHigh && !factsHigh
			latencyHigh = false
		}
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
			c.rate = math.Min(c.rate, math.Max(next, c.rate*env.MaxDecrease))
			c.lastDecrease = s.Now
		case !s.Binding:
			d.Reason = ReasonIdle
		default:
			step := env.IncreaseFrac
			if c.lastBadRate > 0 && c.rate >= 0.85*c.lastBadRate && env.ProbeRamp > 0 {
				ramp := float64(s.Now.Sub(c.lastDecrease)) / float64(env.ProbeRamp)
				step *= math.Min(1, math.Max(0.1, ramp))
			}
			c.rate *= 1 + step
			d.Reason = ReasonProbe
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
