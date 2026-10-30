package client

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
)

// capacityController names the persistence capacity controller in adaptive's decision
// metric.
const capacityController = "persistence_capacity"

// capacityAwareRateLimiter is the host-level dynamic persistence limiter.
//
// With DynamicRateLimitingParams.Capacity.Mode "disabled" (the default) it
// delegates every call to the existing per-priority HealthRequestRateLimiterImpl
// set, unchanged. With "enabled" it delegates to one set of priority buckets
// driven by a single per-host capacity controller, which resolves the TODO in
// newPriorityDynamicRateLimiter about making dynamic adjustment global across
// priorities. With "shadow" the controller runs and reports what it would do
// while the existing limiter stays in charge. While adaptive.frozen is set, it
// holds the controller's rate where it is.
//
// Like HealthRequestRateLimiterImpl it steps lazily from the request path, so
// it needs no goroutine or lifecycle hooks.
type capacityAwareRateLimiter struct {
	params        DynamicRateLimitingParams
	hostRateFn    quotas.RateFn
	operatorRatio OperatorRPSRatio
	burstRatio    PersistenceBurstRatio
	healthSignals persistence.HealthSignalAggregator
	metrics       metrics.Handler
	logger        log.Logger
	rolloutKey    []byte // this host's key for Capacity.RolloutPercent
	frozen        func() bool

	legacy   quotas.RequestRateLimiter
	shared   quotas.RequestRateLimiter // priority cascade over buckets
	buckets  map[int]*quotas.RateLimiterImpl
	delegate quotas.RequestRateLimiterDelegator

	denied   atomic.Bool  // any denial since the last step: the limit is binding
	nextStep atomic.Int64 // unix nanos

	mu         sync.Mutex // guards the fields below
	ctrl       *capacity.Controller
	mode       adaptive.Mode
	lastReason capacity.Reason
	lastShadow bool
}

var _ quotas.RequestRateLimiter = (*capacityAwareRateLimiter)(nil)

func newCapacityAwareRateLimiter(
	legacy quotas.RequestRateLimiter,
	hostRateFn quotas.RateFn,
	requestPriorityFn quotas.RequestPriorityFn,
	operatorRatio OperatorRPSRatio,
	burstRatio PersistenceBurstRatio,
	healthSignals persistence.HealthSignalAggregator,
	params DynamicRateLimitingParams,
	frozen AdaptiveFrozen,
	metricsHandler metrics.Handler,
	logger log.Logger,
	rolloutKey []byte,
) *capacityAwareRateLimiter {
	initial := hostRateFn()
	l := &capacityAwareRateLimiter{
		params:        params,
		hostRateFn:    hostRateFn,
		operatorRatio: operatorRatio,
		burstRatio:    burstRatio,
		healthSignals: healthSignals,
		metrics:       metricsHandler,
		logger:        logger,
		rolloutKey:    rolloutKey,
		frozen:        frozen,
		legacy:        legacy,
		buckets:       make(map[int]*quotas.RateLimiterImpl, len(RequestPrioritiesOrdered)),
		ctrl:          capacity.New(initial),
		mode:          adaptive.ModeOff,
	}
	if l.frozen == nil {
		l.frozen = func() bool { return false }
	}
	rateLimiters := make(map[int]quotas.RequestRateLimiter, len(RequestPrioritiesOrdered))
	for _, priority := range RequestPrioritiesOrdered {
		rps := initial * l.priorityFactor(priority)
		bucket := quotas.NewRateLimiter(rps, l.burst(rps))
		l.buckets[priority] = bucket
		rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(bucket)
	}
	l.shared = quotas.NewPriorityRateLimiter(requestPriorityFn, rateLimiters)
	l.delegate.SetRateLimiter(legacy)
	return l
}

func (l *capacityAwareRateLimiter) Allow(now time.Time, request quotas.Request) bool {
	l.maybeStep(now)
	ok := l.delegate.Allow(now, request)
	if !ok {
		l.denied.Store(true)
	}
	return ok
}

func (l *capacityAwareRateLimiter) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	l.maybeStep(now)
	r := l.delegate.Reserve(now, request)
	if !r.OK() || r.DelayFrom(now) > 0 {
		l.denied.Store(true)
	}
	return r
}

func (l *capacityAwareRateLimiter) Wait(ctx context.Context, request quotas.Request) error {
	l.maybeStep(time.Now())
	err := l.delegate.Wait(ctx, request)
	if err != nil {
		l.denied.Store(true)
	}
	return err
}

func (l *capacityAwareRateLimiter) maybeStep(now time.Time) {
	next := l.nextStep.Load()
	if now.UnixNano() < next {
		return
	}
	p := l.params()
	interval := p.Capacity.StepInterval
	if interval <= 0 {
		interval = time.Second
	}
	if !l.nextStep.CompareAndSwap(next, now.Add(interval).UnixNano()) {
		return // another caller is stepping
	}
	l.step(now, p)
}

func (l *capacityAwareRateLimiter) step(now time.Time, p dynamicconfig.DynamicRateLimitingParams) {
	l.mu.Lock()
	defer l.mu.Unlock()

	mode := capacityMode(p, l.rolloutKey)
	if mode != adaptive.ModeOff && l.frozen() {
		// The freeze holds the rate where it is, but a change of mode still applies, so
		// the controller can be disabled during an incident. A host that starts applying
		// it takes the controller's rate, not what the buckets kept from an earlier run.
		if mode == adaptive.ModeOn && l.mode != adaptive.ModeOn {
			l.applyRate(l.ctrl.Rate())
		}
		l.setMode(mode, p)
		l.denied.Store(false)
		adaptive.RecordDecision(l.metrics, capacityController, adaptive.ReasonFrozen)
		return
	}
	static := l.hostRateFn()

	var snap persistence.HealthSnapshot
	if mode != adaptive.ModeOff {
		snap = l.healthSignals.RecentSnapshot(p.Capacity.SignalWindows)
	}
	d := l.ctrl.Step(capacityEnvelope(p.Capacity, mode, static, snap.Span), static, capacity.Signals{
		Now:            now,
		P99:            snap.P99,
		ErrorRatio:     snap.ErrorRatio,
		Throughput:     snap.Throughput,
		Samples:        snap.Samples,
		Binding:        l.denied.Swap(false),
		FactsReported:  snap.StoreReportsFacts,
		ThrottledRatio: ratio(snap.Throttled, snap.Samples),
		QueuedRatio:    ratio(snap.Queued, snap.Acquired),
	})

	if mode == adaptive.ModeOn {
		l.applyRate(d.RPS)
	}
	l.setMode(mode, p)
	if mode == adaptive.ModeOff {
		return
	}

	if static > 0 {
		metrics.DynamicRateLimiterMultiplier.With(l.metrics).Record(d.Proposed / static)
	}
	adaptive.RecordDecision(l.metrics, capacityController, adaptive.Reason(d.Reason))
	if d.LatencyShadow {
		adaptive.RecordDecision(l.metrics, capacityController, capacityReasonLatencyShadow)
	}
	if d.Reason != l.lastReason && isDecrease(d.Reason) {
		l.logger.Info("Persistence capacity controller decreased rate.",
			tag.NewStringTag("mode", mode.String()),
			tag.NewStringTag("reason", string(d.Reason)),
			tag.Float64("proposedRPS", d.Proposed),
			tag.Float64("staticRPS", static),
			tag.NewDurationTag("p99", snap.P99),
			tag.NewDurationTag("target", d.Target),
			tag.Float64("errorRatio", snap.ErrorRatio),
			tag.NewInt("throttled", snap.Throttled),
			tag.NewInt("queued", snap.Queued),
			tag.NewInt("acquired", snap.Acquired),
		)
	}
	if d.LatencyShadow && !l.lastShadow {
		l.logger.Info("Persistence capacity controller's latency law would have decreased rate; the store reports facts, so it ran in shadow.",
			tag.NewStringTag("mode", mode.String()),
			tag.NewDurationTag("p99", snap.P99),
			tag.NewDurationTag("target", d.Target),
		)
	}
	l.lastReason = d.Reason
	l.lastShadow = d.LatencyShadow
}

// applyRate sets the shared buckets to the controller's rate, scaled for each priority.
func (l *capacityAwareRateLimiter) applyRate(rps float64) {
	for priority, bucket := range l.buckets {
		priorityRPS := rps * l.priorityFactor(priority)
		bucket.SetRPS(priorityRPS)
		bucket.SetBurst(l.burst(priorityRPS))
	}
}

// setMode switches between the shared buckets and the existing limiter when the host's
// mode changes.
func (l *capacityAwareRateLimiter) setMode(mode adaptive.Mode, p dynamicconfig.DynamicRateLimitingParams) {
	if mode == l.mode {
		return
	}
	if mode == adaptive.ModeOn {
		l.delegate.SetRateLimiter(l.shared)
	} else {
		l.delegate.SetRateLimiter(l.legacy)
	}
	l.logger.Info("Persistence capacity controller mode changed.",
		tag.NewStringTag("from", l.mode.String()), tag.NewStringTag("to", mode.String()),
		tag.NewInt("rolloutPercent", p.Capacity.RolloutPercent))
	l.mode = mode
}

// capacityReasonLatencyShadow tags a step where the latency law would have cut a
// store that reports facts, so shadow runs can compare the two.
const capacityReasonLatencyShadow adaptive.Reason = "latency-shadow"

func isDecrease(reason capacity.Reason) bool {
	switch reason {
	case capacity.ReasonErrors, capacity.ReasonThrottled, capacity.ReasonQueued, capacity.ReasonLatency:
		return true
	default:
		return false
	}
}

// ratio is count / total, both recorded over the same span, or 0 without a total.
func ratio(count, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(count) / float64(total)
}

func (l *capacityAwareRateLimiter) priorityFactor(priority int) float64 {
	if priority == CallerTypeDefaultPriority[headers.CallerTypeOperator] {
		return l.operatorRatio()
	}
	return 1
}

func (l *capacityAwareRateLimiter) burst(rps float64) int {
	return max(1, int(rps*l.burstRatio()))
}

// capacityCeilingFn is the basic host-level limiter's rate. While the capacity
// controller is enabled on this host it becomes a safety ceiling at
// Capacity.MaxMultiplier x hostMaxQPS, so the controller can use database headroom;
// otherwise it is hostMaxQPS, as before. The basic limiter refreshes its rate once
// a minute.
func capacityCeilingFn(hostRateFn quotas.RateFn, dynamicParams DynamicRateLimitingParams, rolloutKey []byte) quotas.RateFn {
	return func() float64 {
		params := dynamicParams()
		if capacityMode(params, rolloutKey) == adaptive.ModeOn && params.Capacity.MaxMultiplier > 1 {
			return params.Capacity.MaxMultiplier * hostRateFn()
		}
		return hostRateFn()
	}
}

// capacityMode is the mode this host applies: off unless Enabled is set and, outside
// Capacity.RolloutPercent's cohort, shadow for an enabled controller.
func capacityMode(p dynamicconfig.DynamicRateLimitingParams, rolloutKey []byte) adaptive.Mode {
	if !p.Enabled {
		return adaptive.ModeOff
	}
	return adaptive.HostMode(p.Capacity.Mode, p.Capacity.RolloutPercent, rolloutKey)
}

// capacityEnvelope converts dynamic config into a controller envelope. The
// effective cooldown is never shorter than twice the signal span, so the
// controller cannot act twice on the same stale data even if the latency
// windows are configured larger than recommended.
func capacityEnvelope(
	p dynamicconfig.CapacityControllerParams,
	mode adaptive.Mode,
	static float64,
	signalSpan time.Duration,
) capacity.Envelope {
	// The controller keeps its own mode type, so that it stays stdlib-only and the
	// simulator can copy it as it is.
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
		MaxDecrease:     p.MaxDecrease,
		DeliveryBackoff: p.DeliveryBackoff,
		IncreaseFrac:    p.IncreaseFrac,
		ProbeRamp:       p.ProbeRamp,
		Cooldown:        max(p.Cooldown, 2*signalSpan),
		BaselineWindow:  p.BaselineWindow,
		MinSamples:      p.MinSamples,
	}
}
