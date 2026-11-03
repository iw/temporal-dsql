package fairshare

import (
	"context"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/quotas/calculator"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// ReasonHeld is a step whose share rose less than Deadband, so the share applied stays. A
// share that falls applies at once: a frontend never holds a share above the one Share
// gives it, so once the frontends see the same demand, the shares they hold add up to at
// most the limit.
const ReasonHeld Reason = "held"

// Exchanges counts a frontend's exchanges with the others, by outcome.
var Exchanges = metrics.NewCounterDef(
	"adaptive_fair_share_exchanges",
	metrics.WithDescription("Exchanges of namespace demand with other frontends, keyed by `outcome`"),
)

type (
	// ExchangeParams are what an Exchange needs from its frontend.
	ExchangeParams struct {
		Settings func() Settings
		Frozen   func() bool
		// Self is this frontend's address, and Members the addresses of the frontends that
		// take requests. Dial connects to one of them.
		Self       string
		Members    func() []string
		Dial       func(address string) (*grpc.ClientConn, error)
		TimeSource clock.TimeSource
		Logger     log.Logger
		Handler    metrics.Handler
		Sink       adaptive.DecisionSink
	}

	// Exchange counts the tokens each namespace's requests ask of a frontend's namespace rate
	// limits, exchanges those rates with the other frontends, and gives each limit this
	// frontend's share. It's also the gRPC service the frontends exchange their rates
	// through.
	Exchange struct {
		ExchangeParams
		classes []*class
		// counting is set while the controller runs, so requests are counted.
		counting atomic.Bool
		// lastRound is when the last round measured, or zero before the first since counting
		// started. Only the exchange's goroutine uses it.
		lastRound time.Time
		ctx       context.Context
		cancel    context.CancelFunc

		mu      sync.Mutex
		report  *structpb.Struct      // this frontend's latest report, which replies carry
		at      time.Time             // when the report was measured
		peers   map[string]peerReport // the other frontends' latest reports, by address
		conns   map[string]*grpc.ClientConn
		failing map[string]bool // the frontends whose last exchange failed

		startOnce, stopOnce sync.Once
		stopCh, doneCh      chan struct{}
	}

	// class is one of a frontend's namespace rate limits, with the counts, shares and
	// decisions the exchange keeps for it. Only the exchange's goroutine uses samples,
	// applied and last.
	class struct {
		name    string
		limit   func(namespace string) int // a namespace's limit across the cluster
		counts  counters
		samples map[string][]sample
		shares  atomic.Pointer[map[string]share] // the shares the limit takes, by namespace
		applied map[string]share
		last    map[string]adaptive.Decision // the decisions last reported, by namespace
	}

	// share is the share applied for a namespace, and the limit it was a share of.
	share struct {
		limit, share float64
	}

	peerReport struct {
		received time.Time
		demand   map[string]map[string]float64 // tokens a second, by limit and namespace
	}
)

var _ calculator.ShareProvider = (*class)(nil)

// NewExchange returns an exchange with no limits, which runs once started.
func NewExchange(p ExchangeParams) *Exchange {
	e := &Exchange{
		ExchangeParams: p,
		peers:          map[string]peerReport{},
		conns:          map[string]*grpc.ClientConn{},
		failing:        map[string]bool{},
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
	e.ctx, e.cancel = context.WithCancel(context.Background())
	return e
}

// Shares adds the named namespace rate limit, whose limit across the cluster for a
// namespace is limit, and gives its quota calculator this frontend's share of it. Limits are
// added before Start. A nil Exchange gives nil, and the calculator an equal share.
func (e *Exchange) Shares(name string, limit func(namespace string) int) calculator.ShareProvider {
	if e == nil {
		return nil
	}
	c := e.class(name)
	if c == nil {
		c = &class{name: name, limit: limit, samples: map[string][]sample{}, last: map[string]adaptive.Decision{}}
		e.classes = append(e.classes, c)
	}
	return c
}

// Count wraps limiter, the named namespace rate limit, so the exchange counts the tokens
// each namespace's requests ask of it. A nil Exchange, or one without the limit, returns
// limiter itself.
func (e *Exchange) Count(name string, limiter quotas.RequestRateLimiter) quotas.RequestRateLimiter {
	if c := e.class(name); c != nil {
		return &countingLimiter{RequestRateLimiter: limiter, exchange: e, class: c}
	}
	return limiter
}

// Active is whether the controller runs, in shadow or enabled.
func (e *Exchange) Active() bool {
	return e.counting.Load()
}

func (e *Exchange) class(name string) *class {
	if e == nil {
		return nil
	}
	for _, c := range e.classes {
		if c.name == name {
			return c
		}
	}
	return nil
}

// Share is this frontend's share of a namespace's limit, while the controller applies one.
func (c *class) Share(namespace string, limit float64) (float64, bool) {
	shares := c.shares.Load()
	if shares == nil {
		return 0, false
	}
	s, ok := (*shares)[namespace]
	if !ok || s.limit <= 0 {
		return 0, false
	}
	// The limit may have changed since the step.
	return s.share * limit / s.limit, true
}

// Start runs the exchange's rounds, every Interval, until Stop.
func (e *Exchange) Start() {
	e.startOnce.Do(func() { go e.run() })
}

// Stop ends the rounds and closes the connections to the other frontends.
func (e *Exchange) Stop() {
	e.stopOnce.Do(func() {
		e.cancel()
		close(e.stopCh)
		started := true
		e.startOnce.Do(func() { started = false })
		if started {
			<-e.doneCh
		}
		e.mu.Lock()
		defer e.mu.Unlock()
		for _, conn := range e.conns {
			_ = conn.Close()
		}
		e.conns = map[string]*grpc.ClientConn{}
	})
}

func (e *Exchange) run() {
	defer close(e.doneCh)
	for {
		interval := e.Settings().Interval
		if interval <= 0 {
			interval = DefaultSettings.Interval
		}
		fired, timer := e.TimeSource.NewTimer(interval)
		select {
		case <-e.stopCh:
			timer.Stop()
			return
		case <-fired:
			e.round(e.ctx)
		}
	}
}

// round measures this frontend's demand, exchanges it with the other frontends, and sets
// each limit's shares. Only the exchange's goroutine calls it.
func (e *Exchange) round(ctx context.Context) {
	s := e.Settings()
	if adaptive.ParseMode(s.Mode) == adaptive.ModeOff {
		if e.counting.Swap(false) {
			e.reset()
		}
		return
	}
	e.counting.Store(true)
	now := e.TimeSource.Now()
	own := e.measure(now, s.Window)
	members := e.Members()
	report := newReport(e.Self, true, own)
	e.mu.Lock()
	e.report, e.at = report, now
	e.mu.Unlock()
	e.exchange(ctx, s, members, report)
	e.decide(e.TimeSource.Now(), s, members, own)
}

// reset forgets the counts, reports and shares once the controller is disabled, so every
// limit takes an equal share again.
func (e *Exchange) reset() {
	e.lastRound = time.Time{}
	for _, c := range e.classes {
		c.counts.clear()
		c.samples = map[string][]sample{}
		c.shares.Store(nil)
		c.applied = nil
		c.last = map[string]adaptive.Decision{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.report = nil
	e.peers = map[string]peerReport{}
	e.failing = map[string]bool{}
	for _, conn := range e.conns {
		_ = conn.Close()
	}
	e.conns = map[string]*grpc.ClientConn{}
}

// exchange sends report to each other frontend and keeps the reports they reply with.
func (e *Exchange) exchange(ctx context.Context, s Settings, members []string, report *structpb.Struct) {
	var wg sync.WaitGroup
	for address, conn := range e.connect(members) {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(ctx, s.Timeout)
			defer cancel()
			reply := &structpb.Struct{}
			err := conn.Invoke(ctx, exchangeMethod, report, reply)
			e.mu.Lock()
			defer e.mu.Unlock()
			if err != nil {
				e.failed(address, err)
				return
			}
			Exchanges.With(e.Handler).Record(1, metrics.StringTag("outcome", "ok"))
			if e.failing[address] {
				delete(e.failing, address)
				e.Logger.Info("Fair share exchange with a frontend recovered", tag.Address(address))
			}
			// A frontend that reached itself under another address doesn't count itself twice.
			if host, running, age, demand := readReport(reply); running && host != e.Self {
				e.peers[address] = peerReport{received: e.TimeSource.Now().Add(-age), demand: demand}
			}
		})
	}
	wg.Wait()
}

// connect returns a connection to each other member, and closes those to frontends that
// are no longer members.
func (e *Exchange) connect(members []string) map[string]*grpc.ClientConn {
	e.mu.Lock()
	defer e.mu.Unlock()
	targets := map[string]*grpc.ClientConn{}
	for _, address := range members {
		if address == e.Self {
			continue
		}
		conn, ok := e.conns[address]
		if !ok {
			var err error
			if conn, err = e.Dial(address); err != nil {
				e.failed(address, err)
				continue
			}
			e.conns[address] = conn
		}
		targets[address] = conn
	}
	for address, conn := range e.conns {
		if _, ok := targets[address]; !ok {
			_ = conn.Close()
			delete(e.conns, address)
			delete(e.peers, address)
			delete(e.failing, address)
		}
	}
	return targets
}

// failed records a failed exchange, and logs the first of a run of them. The caller holds
// e.mu.
func (e *Exchange) failed(address string, err error) {
	Exchanges.With(e.Handler).Record(1, metrics.StringTag("outcome", "error"))
	if !e.failing[address] {
		e.failing[address] = true
		e.Logger.Warn("Fair share exchange with a frontend failed", tag.Address(address), tag.Error(err))
	}
}

// decide sets each limit's shares from the demand this frontend and the others report, and
// reports the decisions that changed.
func (e *Exchange) decide(now time.Time, s Settings, members []string, own map[string]map[string]float64) {
	n := len(members)
	if !slices.Contains(members, e.Self) {
		n++
	}
	fresh := e.fresh(now, s.StaleAfter, members)
	frozen := e.Frozen()
	for _, c := range e.classes {
		peers := map[string]float64{}
		for _, report := range fresh {
			for namespace, rate := range report[c.name] {
				peers[namespace] += rate
			}
		}
		names := map[string]struct{}{}
		next := map[string]share{}
		for _, rates := range []map[string]float64{own[c.name], peers} {
			for namespace := range rates {
				if _, ok := names[namespace]; ok {
					continue
				}
				names[namespace] = struct{}{}
				demand := Demand{Own: own[c.name][namespace], Peers: peers[namespace], Members: n, Known: len(fresh)}
				if applied, ok := e.step(c, s, now, frozen, namespace, demand); ok {
					next[namespace] = applied
				}
			}
		}
		if !frozen {
			e.forget(c, now, names)
			c.applied = next
			c.shares.Store(&next)
		}
	}
}

// step decides a namespace's share of a limit and reports the decision. It returns the share
// the limit takes, if the controller applies one.
func (e *Exchange) step(c *class, s Settings, now time.Time, frozen bool, namespace string, demand Demand) (share, bool) {
	limit := float64(c.limit(namespace))
	if limit <= 0 {
		// Without a limit across the cluster, each frontend applies its own.
		return share{}, false
	}
	proposed, reason := Share(s.Params(), limit, demand)
	mode := adaptive.HostMode(s.Mode, s.RolloutPercent, []byte(namespace))
	equal := limit / float64(max(1, demand.Members))
	applied, decision := equal, adaptive.Reason(reason)
	prev, had := c.applied[namespace]
	held := prev.share * limit / max(prev.limit, 1)
	switch {
	case mode != adaptive.ModeOn:
	case frozen:
		decision = adaptive.ReasonFrozen
		if had {
			applied = held
		}
	case had && proposed >= held && proposed-held < s.Deadband*equal:
		applied, decision = held, adaptive.Reason(ReasonHeld)
	default:
		applied = proposed
	}
	e.reportStep(c, s, namespace, adaptive.Decision{
		Controller: Name,
		Time:       now,
		Mode:       mode,
		Reason:     decision,
		Static:     equal,
		Proposed:   proposed,
		Applied:    applied,
		Detail:     Detail{Class: c.name, Namespace: namespace, Limit: limit, Params: s.Params(), Demand: demand},
	})
	return share{limit: limit, share: applied}, mode == adaptive.ModeOn && (had || reason != ReasonNoDemand)
}

// forget reports that each namespace no frontend reports demand for anymore went back to an
// equal share.
func (e *Exchange) forget(c *class, now time.Time, names map[string]struct{}) {
	for namespace, last := range c.last {
		if _, ok := names[namespace]; ok {
			continue
		}
		last.Time, last.Reason = now, adaptive.Reason(ReasonNoDemand)
		last.Proposed, last.Applied = last.Static, last.Static
		if detail, ok := last.Detail.(Detail); ok {
			detail.Demand.Own, detail.Demand.Peers = 0, 0
			last.Detail = detail
		}
		adaptive.Report(e.Handler, e.Sink, last)
		delete(c.last, namespace)
	}
}

// fresh is the demand the other frontends reported within staleAfter, by address.
func (e *Exchange) fresh(now time.Time, staleAfter time.Duration, members []string) map[string]map[string]map[string]float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	fresh := map[string]map[string]map[string]float64{}
	for _, address := range members {
		if report, ok := e.peers[address]; ok && address != e.Self && now.Sub(report.received) <= staleAfter {
			fresh[address] = report.demand
		}
	}
	return fresh
}

// reportStep reports a step's decision when it differs from the last one reported for the
// namespace: in mode, reason or the share applied, or by Deadband in the share proposed.
func (e *Exchange) reportStep(c *class, s Settings, namespace string, d adaptive.Decision) {
	last, ok := c.last[namespace]
	if ok && last.Mode == d.Mode && last.Reason == d.Reason && last.Applied == d.Applied &&
		(last.Proposed == d.Proposed || math.Abs(last.Proposed-d.Proposed) < s.Deadband*d.Static) {
		return
	}
	c.last[namespace] = d
	adaptive.Report(e.Handler, e.Sink, d)
}
