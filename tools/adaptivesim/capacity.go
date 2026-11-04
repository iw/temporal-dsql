package adaptivesim

import (
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"golang.org/x/time/rate"
)

// capacityLimiter is the history host's persistence rate limiter with its capacity
// controller, as common/persistence/client's capacityAwareRateLimiter steps it: lazily,
// from the calls, every StepInterval. The controller is the real one; the limiter is one
// bucket for every priority, at the static rate while the controller is off or in shadow,
// and at the controller's rate while it's on. Freezing isn't simulated.
type capacityLimiter struct {
	cfg     Capacity
	hostKey []byte
	health  *health
	plane   *plane

	ctrl     *capacity.Controller
	start    float64 // the rate the law took other than by a step, until a decision reports it
	mode     adaptive.Mode
	static   *rate.Limiter
	shared   *rate.Limiter
	denied   bool
	nextStep time.Time
}

func newCapacityLimiter(cfg Capacity, hostKey []byte, h *health, pl *plane, now time.Time) *capacityLimiter {
	c := &capacityLimiter{
		cfg:     cfg,
		hostKey: hostKey,
		health:  h,
		plane:   pl,
		ctrl:    capacity.New(cfg.Static),
		start:   cfg.Static,
		mode:    adaptive.ModeOff,
	}
	c.static = rate.NewLimiter(rate.Limit(cfg.Static), c.burst(cfg.Static))
	c.shared = rate.NewLimiter(rate.Limit(cfg.Static), c.burst(cfg.Static))
	// The limiters start full at the time the host starts.
	c.static.AllowN(now, 0)
	c.shared.AllowN(now, 0)
	return c
}

// allow is Allow: a call takes a token, or the limiter rejects it.
func (c *capacityLimiter) allow(now time.Time) bool {
	c.maybeStep(now)
	limiter := c.static
	if c.mode == adaptive.ModeOn {
		limiter = c.shared
	}
	if limiter.AllowN(now, 1) {
		return true
	}
	c.denied = true
	return false
}

func (c *capacityLimiter) maybeStep(now time.Time) {
	if now.Before(c.nextStep) {
		return
	}
	p := c.cfg.Params
	interval := p.Capacity.StepInterval
	if interval <= 0 {
		interval = time.Second
	}
	c.nextStep = now.Add(interval)
	c.step(now, p)
}

func (c *capacityLimiter) step(now time.Time, p dynamicconfig.DynamicRateLimitingParams) {
	mode := adaptive.ModeOff
	if p.Enabled {
		mode = adaptive.HostMode(p.Capacity.Mode, p.Capacity.RolloutPercent, c.hostKey)
	}
	static := c.cfg.Static
	var snap persistence.HealthSnapshot
	if mode != adaptive.ModeOff {
		snap = c.health.snapshot(now, p.Capacity.SignalWindows)
	}
	env := capacityEnvelope(p.Capacity, mode, static, snap.Span)
	inputs := capacity.Signals{
		Now:            now,
		P99:            snap.P99,
		ErrorRatio:     snap.ErrorRatio,
		Throughput:     snap.Throughput,
		Span:           snap.Span,
		Samples:        snap.Samples,
		Binding:        c.denied,
		FactsReported:  snap.StoreReportsFacts,
		ThrottledRatio: ratio(snap.Throttled, snap.Samples),
		QueuedRatio:    ratio(snap.Queued, snap.Acquired),
	}
	c.denied = false
	d := c.ctrl.Step(env, static, inputs)
	if mode == adaptive.ModeOn {
		c.shared.SetLimitAt(now, rate.Limit(d.RPS))
		c.shared.SetBurstAt(now, c.burst(d.RPS))
	}
	c.mode = mode
	if mode == adaptive.ModeOff {
		c.start = static
		return
	}
	c.plane.recordDecision(adaptive.Decision{
		Controller: capacity.Name,
		Time:       now,
		Mode:       mode,
		Reason:     adaptive.Reason(d.Reason),
		Static:     static,
		Proposed:   d.Proposed,
		Applied:    d.RPS,
		Detail:     capacity.Detail{Envelope: env, Signals: inputs, Decision: d, Start: c.start},
	})
	c.start = 0
}

func (c *capacityLimiter) burst(rps float64) int {
	return max(1, int(rps*c.cfg.BurstRatio))
}

func ratio(count, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(count) / float64(total)
}

// capacityEnvelope is common/persistence/client's capacityEnvelope: dynamic config as the
// controller's envelope, with a cooldown never shorter than twice the signal span.
func capacityEnvelope(
	p dynamicconfig.CapacityControllerParams,
	mode adaptive.Mode,
	static float64,
	signalSpan time.Duration,
) capacity.Envelope {
	var m capacity.Mode
	switch mode {
	case adaptive.ModeShadow:
		m = capacity.ModeShadow
	case adaptive.ModeOn:
		m = capacity.ModeOn
	default:
		m = capacity.ModeOff
	}
	return capacity.Envelope{
		Mode:            m,
		MinRPS:          p.MinMultiplier * static,
		MaxRPS:          p.MaxMultiplier * static,
		Tolerance:       p.Tolerance,
		LatencyCeiling:  p.LatencyCeiling,
		LatencyFloor:    p.LatencyFloor,
		MaxErrorRatio:   p.MaxErrorRatio,
		MaxQueuedRatio:  p.MaxQueuedRatio,
		QueuedGrace:     p.QueuedGrace,
		MaxDecrease:     p.MaxDecrease,
		DeliveryBackoff: p.DeliveryBackoff,
		DeliveryWindow:  p.DeliveryWindow,
		IncreaseFrac:    p.IncreaseFrac,
		ProbeRamp:       p.ProbeRamp,
		Cooldown:        max(p.Cooldown, 2*signalSpan),
		BaselineWindow:  p.BaselineWindow,
		BaselineAlert:   p.BaselineAlert,
		MinSamples:      p.MinSamples,
	}
}
