package client

import (
	"context"
	"os"
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
	rolloutKey    []byte // this host's key for Capacity.RolloutPercent

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

	mode := capacityMode(p, l.rolloutKey)
	static := l.hostRateFn()

	var snap persistence.HealthSnapshot
	if mode != capacityModeDisabled {
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
			tag.NewStringTag("from", l.mode), tag.NewStringTag("to", mode),
			tag.NewInt("rolloutPercent", p.Capacity.RolloutPercent))
		l.mode = mode
	}
	if mode == capacityModeDisabled {
		return
	}

	if static > 0 {
		metrics.DynamicRateLimiterMultiplier.With(l.metrics).Record(d.Proposed / static)
	}
	metrics.PersistenceCapacityDecisions.With(l.metrics).Record(1, metrics.StringTag("reason", string(d.Reason)))
	if d.LatencyShadow {
		metrics.PersistenceCapacityDecisions.With(l.metrics).Record(1, metrics.StringTag("reason", capacityReasonLatencyShadow))
	}
	if d.Reason != l.lastReason && isDecrease(d.Reason) {
		l.logger.Info("Persistence capacity controller decreased rate.",
			tag.NewStringTag("mode", mode),
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
			tag.NewStringTag("mode", mode),
			tag.NewDurationTag("p99", snap.P99),
			tag.NewDurationTag("target", d.Target),
		)
	}
	l.lastReason = d.Reason
	l.lastShadow = d.LatencyShadow
}

// capacityReasonLatencyShadow tags a step where the latency law would have cut a
// store that reports facts, so shadow runs can compare the two.
const capacityReasonLatencyShadow = "latency-shadow"

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
		if capacityMode(params, rolloutKey) == capacityModeEnabled && params.Capacity.MaxMultiplier > 1 {
			return params.Capacity.MaxMultiplier * hostRateFn()
		}
		return hostRateFn()
	}
}

// capacityMode is the mode this host applies. Outside Capacity.RolloutPercent's
// cohort, which rolloutKey places it in or out of, an enabled controller runs in
// shadow.
func capacityMode(p dynamicconfig.DynamicRateLimitingParams, rolloutKey []byte) string {
	mode := p.Capacity.Mode
	if !p.Enabled || (mode != capacityModeShadow && mode != capacityModeEnabled) {
		return capacityModeDisabled
	}
	if mode == capacityModeEnabled && !dynamicconfig.RolloutAccepts(rolloutKey, p.Capacity.RolloutPercent) {
		return capacityModeShadow
	}
	return mode
}

// hostRolloutKey is this host's key for Capacity.RolloutPercent: its hostname, so a
// host keeps its place in or out of the cohort across restarts.
func hostRolloutKey() []byte {
	hostname, _ := os.Hostname()
	return []byte(hostname)
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
	var m capacity.Mode
	switch mode {
	case capacityModeShadow:
		m = capacity.ModeShadow
	case capacityModeEnabled:
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
