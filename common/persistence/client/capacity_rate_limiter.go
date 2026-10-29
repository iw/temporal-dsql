package client

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
)

const (
	capacityModeDisabled = "disabled"
	capacityModeShadow   = "shadow"
	capacityModeEnabled  = "enabled"
)

// capacityAwareRateLimiter is the host-level dynamic persistence limiter.
//
// With DynamicRateLimitingParams.Capacity.Mode "disabled" (the default) it
// delegates every call to the existing per-priority HealthRequestRateLimiterImpl
// set, unchanged. With "enabled" it delegates to one set of priority buckets
// driven by a single per-host capacity controller, which resolves the TODO in
// newPriorityDynamicRateLimiter about making dynamic adjustment global across
// priorities. With "shadow" the controller runs and reports what it would do
// while the existing limiter stays in charge.
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

	legacy   quotas.RequestRateLimiter
	shared   quotas.RequestRateLimiter // priority cascade over buckets
	buckets  map[int]*quotas.RateLimiterImpl
	delegate quotas.RequestRateLimiterDelegator

	denied   atomic.Bool  // any denial since the last step: the limit is binding
	nextStep atomic.Int64 // unix nanos

	mu         sync.Mutex // guards the fields below
	ctrl       *capacity.Controller
	mode       string
	lastReason capacity.Reason
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
	metricsHandler metrics.Handler,
	logger log.Logger,
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
		legacy:        legacy,
		buckets:       make(map[int]*quotas.RateLimiterImpl, len(RequestPrioritiesOrdered)),
		ctrl:          capacity.New(initial),
		mode:          capacityModeDisabled,
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

	mode := p.Capacity.Mode
	if !p.Enabled || (mode != capacityModeShadow && mode != capacityModeEnabled) {
		mode = capacityModeDisabled
	}
	static := l.hostRateFn()

	var snap persistence.HealthSnapshot
	if mode != capacityModeDisabled {
		snap = l.healthSignals.RecentSnapshot(p.Capacity.SignalWindows)
	}
	d := l.ctrl.Step(capacityEnvelope(p.Capacity, mode, static, snap.Span), static, capacity.Signals{
		Now:        now,
		P99:        snap.P99,
		ErrorRatio: snap.ErrorRatio,
		Throughput: snap.Throughput,
		Samples:    snap.Samples,
		Binding:    l.denied.Swap(false),
	})

	if mode == capacityModeEnabled {
		for priority, bucket := range l.buckets {
			rps := d.RPS * l.priorityFactor(priority)
			bucket.SetRPS(rps)
			bucket.SetBurst(l.burst(rps))
		}
	}
	if mode != l.mode {
		if mode == capacityModeEnabled {
			l.delegate.SetRateLimiter(l.shared)
		} else {
			l.delegate.SetRateLimiter(l.legacy)
		}
		l.logger.Info("Persistence capacity controller mode changed.",
			tag.NewStringTag("from", l.mode), tag.NewStringTag("to", mode))
		l.mode = mode
	}
	if mode == capacityModeDisabled {
		return
	}

	if static > 0 {
		metrics.DynamicRateLimiterMultiplier.With(l.metrics).Record(d.Proposed / static)
	}
	if d.Reason != l.lastReason && (d.Reason == capacity.ReasonLatency || d.Reason == capacity.ReasonErrors) {
		l.logger.Info("Persistence capacity controller decreased rate.",
			tag.NewStringTag("mode", mode),
			tag.NewStringTag("reason", string(d.Reason)),
			tag.Float64("proposedRPS", d.Proposed),
			tag.Float64("staticRPS", static),
			tag.NewDurationTag("p99", snap.P99),
			tag.NewDurationTag("target", d.Target),
			tag.Float64("errorRatio", snap.ErrorRatio),
		)
	}
	l.lastReason = d.Reason
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

// capacityEnvelope converts dynamic config into a controller envelope. The
// effective cooldown is never shorter than twice the signal span, so the
// controller cannot act twice on the same stale data even if the latency
// windows are configured larger than recommended.
func capacityEnvelope(
	p dynamicconfig.CapacityControllerParams,
	mode string,
	static float64,
	signalSpan time.Duration,
) capacity.Envelope {
	m := capacity.ModeOff
	switch mode {
	case capacityModeShadow:
		m = capacity.ModeShadow
	case capacityModeEnabled:
		m = capacity.ModeOn
	}
	return capacity.Envelope{
		Mode:            m,
		MinRPS:          p.MinMultiplier * static,
		MaxRPS:          p.MaxMultiplier * static,
		Tolerance:       p.Tolerance,
		LatencyCeiling:  p.LatencyCeiling,
		LatencyFloor:    p.LatencyFloor,
		MaxErrorRatio:   p.MaxErrorRatio,
		MaxDecrease:     p.MaxDecrease,
		DeliveryBackoff: p.DeliveryBackoff,
		IncreaseFrac:    p.IncreaseFrac,
		ProbeRamp:       p.ProbeRamp,
		Cooldown:        max(p.Cooldown, 2*signalSpan),
		BaselineWindow:  p.BaselineWindow,
		MinSamples:      p.MinSamples,
	}
}
