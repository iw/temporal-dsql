package adaptivesim

import (
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"net"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/fairshare"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/common/signals"
	"go.temporal.io/server/common/testing/nettest"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// frontendService is the service a frontend's trace names.
	frontendService = "frontend"
	// executionLimit is the namespace rate limit that the model's requests take, by the
	// name service/frontend gives it the fair share exchange. The model has no other.
	executionLimit = "execution"
	// startWorkflowAPI is the API each of the model's requests calls.
	startWorkflowAPI = "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"
	// limiterRefresh is how often a namespace's limiter takes its quota again, the quotas
	// package's refresh interval.
	limiterRefresh = time.Minute
	// exchangeTimeout is each exchange's timeout in the model. The exchanges run over gRPC
	// in memory, on the wall clock, so the model gives them longer than the controller's
	// Timeout: a busy machine shouldn't fail an exchange that simulated time has no part in.
	exchangeTimeout = time.Minute
)

type (
	// frontends are the frontend service's hosts. Each passes requests through its
	// namespace rate limits, and runs the fair share controller's real exchange: the
	// frontends' exchanges serve each other over gRPC in memory, and each frontend runs its
	// rounds on simulated time.
	frontends struct {
		l     *loop
		cfg   Frontends
		hosts []*frontend
		// ts is the exchanges' clock. A round sets it to the simulated time before it runs,
		// since the other frontends read it from gRPC's goroutines as they serve the round.
		ts    *clock.EventTimeSource
		pipes map[string]*nettest.Pipe
		// pick picks the frontend each workflow start goes through.
		pick *rand.Rand
		seed uint64
	}

	// frontend is one frontend host: its namespace rate limits, its exchange, and its
	// signal plane, which records the exchange's decisions.
	frontend struct {
		fs       *frontends
		name     string
		pid      int
		plane    *plane
		exchange *fairshare.Exchange
		server   *grpc.Server
		limits   *namespaceLimits
		// limiter is limits, as the exchange counts the tokens each request asks of them.
		limiter quotas.RequestRateLimiter
		counts  map[string]*NamespaceResult
		order   []string // the namespaces, in the order of their first request
	}

	// namespaceLimits are a frontend's namespace rate limits, as
	// quotas.NewNamespaceRequestRateLimiter keeps them, on simulated time: a token bucket
	// for each namespace at its quota, made at the namespace's first request. A bucket takes
	// its quota and burst again at its first request a minute or more after it last did, as
	// quotas.DynamicRateLimiterImpl does on its timer, so a share the exchange changes
	// applies within a minute.
	namespaceLimits struct {
		// RequestRateLimiter answers Reserve and Wait, which the model doesn't call.
		quotas.RequestRateLimiter
		quota      func(namespace string) float64
		burstRatio func(namespace string) float64
		buckets    map[string]*namespaceBucket
	}

	namespaceBucket struct {
		limiter   *rate.Limiter
		burst     int
		refreshed time.Time
	}

	// FrontendResult is what one frontend did.
	FrontendResult struct {
		Name string
		PID  int
		// Namespaces are the requests the frontend took, by namespace, in the order of each
		// namespace's first request.
		Namespaces []NamespaceResult
		// Decisions are the fair share controller's decisions, by time, then by limit and
		// namespace.
		Decisions Decisions
	}

	// NamespaceResult is the requests a frontend took for one namespace.
	NamespaceResult struct {
		Namespace           string
		Admitted, Throttled int
		// PerSecond is the requests in each second after the run's start.
		PerSecond []RequestCounts
		// Limit is the namespace's rate limit at the frontend at the end, as its limiter
		// last took it.
		Limit float64
	}

	// RequestCounts are requests a frontend admitted and throttled.
	RequestCounts struct {
		Admitted, Throttled int
	}
)

var _ calculator.MemberCounter = (*frontends)(nil)

// newFrontends returns the scenario's frontends, numbered from firstPID, each serving its
// exchange.
func newFrontends(w *world, firstPID int) *frontends {
	cfg := w.sc.Frontends
	fs := &frontends{
		l:     w.l,
		cfg:   cfg,
		ts:    clock.NewEventTimeSource().Update(w.l.now),
		pipes: map[string]*nettest.Pipe{},
		pick:  randoms(w.sc.Seed, "frontends/starts"),
		seed:  w.sc.Seed,
	}
	collection := newFrontendCollection(cfg)
	settings := fairshare.Config.Get(collection)
	exchangeSettings := func() fairshare.Settings {
		s := settings()
		s.Timeout = max(s.Timeout, exchangeTimeout)
		return s
	}
	frozen := adaptive.Frozen.Get(collection)
	global := dynamicconfig.FrontendGlobalNamespaceRPS.Get(collection)
	perInstance := dynamicconfig.FrontendMaxNamespaceRPSPerInstance.Get(collection)
	burstRatio := dynamicconfig.FrontendMaxNamespaceBurstRatioPerInstance.Get(collection)
	members := make([]string, cfg.Count)
	for i := range members {
		members[i] = fmt.Sprintf("frontend-%d", i+1)
		fs.pipes[members[i]] = nettest.NewPipe()
	}
	for i, name := range members {
		f := &frontend{fs: fs, name: name, pid: firstPID + i, counts: map[string]*NamespaceResult{}}
		header := signals.Header{Service: frontendService, Host: name, PID: f.pid}
		f.plane = newPlane(w.l, w.sc.Signals, emptySource{}, header, w.opts.TraceDir, w.log)
		f.exchange = fairshare.NewExchange(fairshare.ExchangeParams{
			Settings:   exchangeSettings,
			Frozen:     frozen,
			Self:       name,
			Members:    func() []string { return members },
			Dial:       fs.dial,
			TimeSource: fs.ts,
			Logger:     w.log,
			Handler:    metrics.NoopMetricsHandler,
			Sink:       f.plane.signals,
		})
		f.limits = &namespaceLimits{
			RequestRateLimiter: quotas.NoopRequestRateLimiter,
			quota: calculator.ClusterAwareNamespaceQuotaCalculator{
				MemberCounter:    fs,
				ShareProvider:    f.exchange.Shares(executionLimit, global),
				PerInstanceQuota: perInstance,
				GlobalQuota:      global,
			}.GetQuota,
			burstRatio: burstRatio,
			buckets:    map[string]*namespaceBucket{},
		}
		f.limiter = f.exchange.Count(executionLimit, f.limits)
		f.server = grpc.NewServer()
		f.exchange.Register(f.server)
		listener := nettest.NewListener(fs.pipes[name])
		go func() { _ = f.server.Serve(listener) }()
		fs.hosts = append(fs.hosts, f)
	}
	return fs
}

// newFrontendCollection is the frontends' dynamic config: the namespaces' limits across the
// cluster, the limits each frontend applies without one, and the fair share controller's
// settings, each its default where the scenario leaves it zero.
func newFrontendCollection(cfg Frontends) *dynamicconfig.Collection {
	values := dynamicconfig.StaticClient{}
	if len(cfg.Limits) > 0 {
		var limits []dynamicconfig.ConstrainedValue
		for _, namespace := range slices.Sorted(maps.Keys(cfg.Limits)) {
			limits = append(limits, dynamicconfig.ConstrainedValue{
				Constraints: dynamicconfig.Constraints{Namespace: namespace},
				Value:       cfg.Limits[namespace],
			})
		}
		values[dynamicconfig.FrontendGlobalNamespaceRPS.Key()] = limits
	}
	if cfg.PerInstance > 0 {
		values[dynamicconfig.FrontendMaxNamespaceRPSPerInstance.Key()] = cfg.PerInstance
	}
	if cfg.BurstRatio > 0 {
		values[dynamicconfig.FrontendMaxNamespaceBurstRatioPerInstance.Key()] = cfg.BurstRatio
	}
	if cfg.FairShare != (fairshare.Settings{}) {
		values[fairshare.Config.Key()] = cfg.FairShare
	}
	return dynamicconfig.NewCollection(values, log.NewNoopLogger())
}

// AvailableMemberCount is how many frontends there are, which the quota calculator divides
// a limit across the cluster by, without a share from the exchange.
func (fs *frontends) AvailableMemberCount() int {
	return fs.cfg.Count
}

// dial connects to another frontend's exchange through its in-memory pipe.
func (fs *frontends) dial(address string) (*grpc.ClientConn, error) {
	pipe, ok := fs.pipes[address]
	if !ok {
		return nil, fmt.Errorf("no frontend %s", address)
	}
	return grpc.NewClient("passthrough:///"+address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return pipe.Connect(ctx.Done())
		}),
	)
}

// start starts each frontend's plane and rounds, and the scenario's requests. A frontend's
// first round comes at an offset of its own within the first Interval, as its process
// would have started at a time of its own.
func (fs *frontends) start() {
	offsets := randoms(fs.seed, "frontends/rounds")
	for _, f := range fs.hosts {
		f.plane.start()
		fs.l.after(between(offsets, 0, f.interval()), f.round)
	}
	for _, r := range fs.cfg.Requests {
		for i, perSecond := range r.Rates {
			f := fs.hosts[i]
			arrivals(fs.l, Phase{At: r.At, Duration: r.Duration, Rate: perSecond}, func() { f.request(r.Namespace) })
		}
	}
}

// startWorkflow passes a workflow's start through a frontend, picked by the starts'
// weights, then starts the workflow. A throttled start is tried again after RetryDelay,
// through a frontend picked again.
func (fs *frontends) startWorkflow(start func()) {
	if fs.hosts[fs.pickHost()].request(workflowNamespace) {
		start()
		return
	}
	fs.l.after(fs.cfg.RetryDelay, func() { fs.startWorkflow(start) })
}

func (fs *frontends) pickHost() int {
	weights := fs.cfg.Starts
	if len(weights) == 0 {
		return fs.pick.IntN(len(fs.hosts))
	}
	total := 0.0
	for _, w := range weights {
		total += w
	}
	x := fs.pick.Float64() * total
	for i, w := range weights {
		if x < w {
			return i
		}
		x -= w
	}
	return len(weights) - 1
}

// stop stops the frontends' servers and exchanges, and closes their traces.
func (fs *frontends) stop() {
	for _, f := range fs.hosts {
		f.server.Stop()
		f.exchange.Stop()
		f.plane.stop()
	}
}

func (fs *frontends) result() []FrontendResult {
	results := make([]FrontendResult, 0, len(fs.hosts))
	for _, f := range fs.hosts {
		r := FrontendResult{Name: f.name, PID: f.pid, Decisions: slices.Clone(f.plane.decisions)}
		// The exchange decides for its namespaces in a map's order; a result shouldn't.
		slices.SortStableFunc(r.Decisions, func(a, b adaptive.Decision) int {
			if c := a.Time.Compare(b.Time); c != 0 {
				return c
			}
			return strings.Compare(decisionKey(a), decisionKey(b))
		})
		for _, namespace := range f.order {
			n := *f.counts[namespace]
			if b, ok := f.limits.buckets[namespace]; ok {
				n.Limit = float64(b.limiter.Limit())
			}
			r.Namespaces = append(r.Namespaces, n)
		}
		results = append(results, r)
	}
	return results
}

// interval is the time between the frontend's rounds, as the exchange's own loop takes it.
func (f *frontend) interval() time.Duration {
	if interval := f.exchange.Settings().Interval; interval > 0 {
		return interval
	}
	return fairshare.DefaultSettings.Interval
}

// round runs one of the exchange's rounds at the simulated time, then schedules the next.
func (f *frontend) round() {
	f.fs.ts.Update(f.fs.l.now)
	f.exchange.Round(context.Background())
	f.fs.l.after(f.interval(), f.round)
}

// request passes one of a namespace's requests through the frontend's limits, and counts
// it. It reports whether the frontend admitted it.
func (f *frontend) request(namespace string) bool {
	now := f.fs.l.now
	admitted := f.limiter.Allow(now, quotas.NewRequest(startWorkflowAPI, 1, namespace, headers.CallerTypeAPI, 0, ""))
	n, ok := f.counts[namespace]
	if !ok {
		n = &NamespaceResult{Namespace: namespace}
		f.counts[namespace] = n
		f.order = append(f.order, namespace)
	}
	second := int(now.Sub(epoch) / time.Second)
	for len(n.PerSecond) <= second {
		n.PerSecond = append(n.PerSecond, RequestCounts{})
	}
	if admitted {
		n.Admitted++
		n.PerSecond[second].Admitted++
	} else {
		n.Throttled++
		n.PerSecond[second].Throttled++
	}
	return admitted
}

// Allow takes a request's tokens from its namespace's bucket, if it has them.
func (n *namespaceLimits) Allow(now time.Time, request quotas.Request) bool {
	return n.bucket(now, request.Caller).limiter.AllowN(now, request.Token)
}

func (n *namespaceLimits) bucket(now time.Time, namespace string) *namespaceBucket {
	b, ok := n.buckets[namespace]
	if !ok {
		rps, burst := n.rateBurst(namespace)
		b = &namespaceBucket{limiter: rate.NewLimiter(rate.Limit(rps), burst), burst: burst, refreshed: now}
		n.buckets[namespace] = b
		return b
	}
	if now.Sub(b.refreshed) >= limiterRefresh {
		b.refreshed = now
		// As quotas.RateLimiterImpl, which sets them only when either changes.
		if rps, burst := n.rateBurst(namespace); rate.Limit(rps) != b.limiter.Limit() || burst != b.burst {
			b.limiter.SetLimitAt(now, rate.Limit(rps))
			b.limiter.SetBurstAt(now, burst)
			b.burst = burst
		}
	}
	return b
}

// rateBurst is a namespace's quota, and its burst as quotas.NewNamespaceRateBurst sets it.
func (n *namespaceLimits) rateBurst(namespace string) (float64, int) {
	rps := n.quota(namespace)
	return rps, max(1, int(math.Ceil(rps*n.burstRatio(namespace))))
}

// Rates are the requests admitted and throttled a second, over the seconds from the
// namespace's first request to its last.
func (n NamespaceResult) Rates() (admitted, throttled float64) {
	first, last := -1, -1
	for i, c := range n.PerSecond {
		if c.Admitted+c.Throttled > 0 {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 {
		return 0, 0
	}
	seconds := float64(last - first + 1)
	return float64(n.Admitted) / seconds, float64(n.Throttled) / seconds
}

// Namespace is the frontend's requests for a namespace.
func (r FrontendResult) Namespace(namespace string) (NamespaceResult, bool) {
	for _, n := range r.Namespaces {
		if n.Namespace == namespace {
			return n, true
		}
	}
	return NamespaceResult{}, false
}
