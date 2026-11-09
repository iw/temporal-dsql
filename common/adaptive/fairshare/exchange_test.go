package fairshare

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/nettest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const testLimit = "execution"

// testCluster is frontends whose exchanges serve each other over in-memory gRPC. The test
// drives their rounds.
type testCluster struct {
	t         *testing.T
	ts        *clock.EventTimeSource
	settings  Settings
	frozen    bool
	members   []string
	limits    map[string]int
	exchanges map[string]*Exchange
	limiters  map[string]quotas.RequestRateLimiter
	decisions *decisionRecorder
	pipes     map[string]*nettest.Pipe
}

type decisionRecorder struct {
	mu        sync.Mutex
	decisions []adaptive.Decision
	hosts     []string // the frontend that made each decision
}

// frontendSink records one frontend's decisions.
type frontendSink struct {
	host     string
	recorder *decisionRecorder
}

func (s frontendSink) RecordDecision(d adaptive.Decision) {
	s.recorder.mu.Lock()
	defer s.recorder.mu.Unlock()
	s.recorder.decisions = append(s.recorder.decisions, d)
	s.recorder.hosts = append(s.recorder.hosts, s.host)
}

// last is the latest decision for a namespace, from any frontend in the cluster.
func (r *decisionRecorder) last(namespace string) (adaptive.Decision, bool) {
	return r.lastFrom("", namespace)
}

// lastFrom is a frontend's latest decision for a namespace, or any frontend's when host is
// empty.
func (r *decisionRecorder) lastFrom(host, namespace string) (adaptive.Decision, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, d := range slices.Backward(r.decisions) {
		if (host == "" || r.hosts[i] == host) && d.Detail.(Detail).Namespace == namespace {
			return d, true
		}
	}
	return adaptive.Decision{}, false
}

func newTestCluster(t *testing.T, mode string, hosts ...string) *testCluster {
	settings := DefaultSettings
	settings.Mode = mode
	c := &testCluster{
		t:         t,
		ts:        clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0)),
		settings:  settings,
		members:   hosts,
		limits:    map[string]int{},
		exchanges: map[string]*Exchange{},
		limiters:  map[string]quotas.RequestRateLimiter{},
		decisions: &decisionRecorder{},
	}
	pipes := map[string]*nettest.Pipe{}
	for _, host := range hosts {
		pipes[host] = nettest.NewPipe()
	}
	c.pipes = pipes
	for _, host := range hosts {
		e := NewExchange(ExchangeParams{
			Settings: func() Settings { return c.settings },
			Frozen:   func() bool { return c.frozen },
			Self:     host,
			Members:  func() []string { return c.members },
			Dial: func(address string) (*grpc.ClientConn, error) {
				pipe, ok := pipes[address]
				if !ok {
					return nil, errors.New("no such frontend")
				}
				return grpc.NewClient("passthrough:///"+address,
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
						return pipe.Connect(ctx.Done())
					}),
				)
			},
			TimeSource: c.ts,
			Logger:     log.NewNoopLogger(),
			Handler:    metrics.NoopMetricsHandler,
			Sink:       frontendSink{host: host, recorder: c.decisions},
		})
		e.Shares(testLimit, func(namespace string) int { return c.limits[namespace] })
		c.limiters[host] = e.Count(testLimit, quotas.NoopRequestRateLimiter)
		server := grpc.NewServer()
		e.Register(server)
		listener := nettest.NewListener(pipes[host])
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(func() {
			server.Stop()
			e.Stop()
		})
		c.exchanges[host] = e
	}
	return c
}

// round runs a round on each frontend, in turn.
func (c *testCluster) round() {
	for _, host := range c.members {
		if e, ok := c.exchanges[host]; ok {
			e.Round(c.t.Context())
		}
	}
}

// load sends the requests a frontend receives for a namespace in an interval, then runs
// a round on each frontend.
func (c *testCluster) load(perHost map[string]int, namespace string) {
	for host, requests := range perHost {
		for range requests {
			c.limiters[host].Allow(c.ts.Now(), quotas.NewRequest("api", 1, namespace, "", 0, ""))
		}
	}
	c.ts.Advance(c.settings.Interval)
	c.round()
}

// fill sends a namespace's requests, as load does, until the frontends' windows for it have
// filled and their shares have left the equal split: a round more than the window for the
// round before the first requests to leave it, one for the other frontends to report it, and
// one for a share that rises to wait for those that fall.
func (c *testCluster) fill(perHost map[string]int, namespace string) {
	for range int(c.settings.Window/c.settings.Interval) + 3 {
		c.load(perHost, namespace)
	}
}

// share is a frontend's share of a namespace's limit, as its quota calculator gets it.
func (c *testCluster) share(host, namespace string) (float64, bool) {
	return c.exchanges[host].Shares(testLimit, nil).Share(namespace, float64(c.limits[namespace]))
}

func TestDemandIsTheRateOverTheWindow(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameShadow, "a")
	c.round() // starts counting
	e := c.exchanges["a"]
	measure := func() (map[string]float64, map[string]bool) {
		own, filling := e.measure(c.ts.Now(), c.settings.Window)
		return own[testLimit], filling[testLimit]
	}
	c.load(map[string]int{"a": 50}, "ns") // 10 a second
	// A namespace's first count started after the last round, so its window fills until
	// that round leaves it.
	rates, filling := measure()
	require.InDelta(t, 10.0, rates["ns"], 1e-9)
	require.True(t, filling["ns"])
	for range 5 {
		c.load(map[string]int{"a": 50}, "ns")
	}
	rates, filling = measure()
	require.InDelta(t, 10.0, rates["ns"], 1e-9)
	require.True(t, filling["ns"])

	// The window covers the last 30 seconds: 20 a second for the last 15 of them.
	for range 3 {
		c.load(map[string]int{"a": 100}, "ns")
	}
	rates, filling = measure()
	require.InDelta(t, (3*50+3*100)/30.0, rates["ns"], 1e-9)
	require.Empty(t, filling)

	// A namespace with no requests in the window has no demand.
	for range 7 {
		c.load(nil, "ns")
	}
	rates, _ = measure()
	require.NotContains(t, rates, "ns")
}

func TestSharesFollowEachFrontendsDemand(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameEnabled, "a", "b")
	c.settings.Deadband = 0
	c.limits["ns"] = 100
	c.round()
	c.fill(map[string]int{"a": 750, "b": 50}, "ns") // 150 and 10 a second
	a, ok := c.share("a", "ns")
	require.True(t, ok)
	b, ok := c.share("b", "ns")
	require.True(t, ok)
	require.InDelta(t, 85.0, a, 1e-6)
	require.InDelta(t, 15.0, b, 1e-6)

	d, ok := c.decisions.last("ns")
	require.True(t, ok)
	require.Equal(t, adaptive.Reason(ReasonOver), d.Reason)
	require.Equal(t, adaptive.ModeOn, d.Mode)
	require.InDelta(t, 50.0, d.Static, 1e-9)
	require.InDelta(t, d.Proposed, d.Applied, 1e-9)

	// A namespace without demand, or without a limit across the cluster, gets an equal share.
	_, ok = c.share("a", "other")
	require.False(t, ok)
}

func TestShadowReportsSharesWithoutApplyingThem(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameShadow, "a", "b")
	c.limits["ns"] = 100
	c.round()
	for range 3 {
		c.load(map[string]int{"a": 750, "b": 50}, "ns")
	}
	_, ok := c.share("a", "ns")
	require.False(t, ok)

	d, ok := c.decisions.last("ns")
	require.True(t, ok)
	require.Equal(t, adaptive.ModeShadow, d.Mode)
	require.InDelta(t, d.Static, d.Applied, 1e-9)
	require.NotEqual(t, d.Static, d.Proposed)
}

func TestRolloutPicksNamespacesByName(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameEnabled, "a", "b")
	c.settings.RolloutPercent = 50
	namespaces := []string{"ns-1", "ns-2", "ns-3", "ns-4", "ns-5", "ns-6", "ns-7", "ns-8"}
	c.round()
	for range 3 {
		for _, ns := range namespaces {
			c.limits[ns] = 100
			for _, l := range []string{"a", "b"} {
				requests := 50
				if l == "a" {
					requests = 750
				}
				for range requests {
					c.limiters[l].Allow(c.ts.Now(), quotas.NewRequest("api", 1, ns, "", 0, ""))
				}
			}
		}
		c.ts.Advance(c.settings.Interval)
		c.round()
	}
	var in, out int
	for _, ns := range namespaces {
		_, applied := c.share("a", ns)
		require.Equal(t, dynamicconfig.RolloutAccepts([]byte(ns), 50), applied, ns)
		if applied {
			in++
		} else {
			out++
		}
	}
	require.Positive(t, in)
	require.Positive(t, out)
}

func TestFrozenHoldsTheSharesApplied(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameEnabled, "a", "b")
	c.limits["ns"] = 100
	c.round()
	for range 3 {
		c.load(map[string]int{"a": 750, "b": 50}, "ns")
	}
	before, _ := c.share("a", "ns")

	c.frozen = true
	for range 7 {
		c.load(map[string]int{"a": 50, "b": 750}, "ns")
	}
	held, ok := c.share("a", "ns")
	require.True(t, ok)
	require.InDelta(t, before, held, 1e-9)
	d, _ := c.decisions.last("ns")
	require.Equal(t, adaptive.ReasonFrozen, d.Reason)

	c.frozen = false
	c.load(map[string]int{"a": 50, "b": 750}, "ns")
	after, _ := c.share("a", "ns")
	require.InDelta(t, 15.0, after, 1e-6)
}

func TestSharesRiseByDeadbandAndFallAtOnce(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameEnabled, "a", "b")
	c.limits["ns"] = 100
	c.round()
	for range 7 {
		c.load(map[string]int{"a": 750, "b": 50}, "ns")
	}
	before, _ := c.share("b", "ns")

	// b's part of the demand moves from 10/160 to 20/170 over the window: its share rises
	// by under 5, Deadband of an equal share, so it holds.
	for range 7 {
		c.load(map[string]int{"a": 750, "b": 100}, "ns")
	}
	held, _ := c.share("b", "ns")
	require.InDelta(t, before, held, 1e-9)
	d, _ := c.decisions.lastFrom("b", "ns")
	require.Equal(t, adaptive.Reason(ReasonHeld), d.Reason)
	// a's falls by as much, and applies at once, so the shares fit the limit.
	a, _ := c.share("a", "ns")
	require.InDelta(t, 10+80*150/170.0, a, 1e-6)
	d, _ = c.decisions.lastFrom("a", "ns")
	require.Equal(t, adaptive.Reason(ReasonOver), d.Reason)

	// An even demand raises b's to within Deadband of an equal share, and lowers a's to an
	// equal share.
	for range 8 {
		c.load(map[string]int{"a": 750, "b": 750}, "ns")
	}
	after, _ := c.share("b", "ns")
	require.InDelta(t, 50.0, after, 5)
	require.Greater(t, after, before+5)
	a, _ = c.share("a", "ns")
	require.InDelta(t, 50.0, a, 1e-6)
}

func TestSharesFitTheLimitWhileStaggeredWindowsFill(t *testing.T) {
	// The system appreciation's uneven frontends: a limit of 60 a second, with 72 requests a
	// second reaching a and 18 reaching b.
	for _, first := range []string{"a", "b"} {
		t.Run(first+" first", func(t *testing.T) {
			c := newTestCluster(t, adaptive.ModeNameEnabled, "a", "b")
			if first == "b" {
				c.members = []string{"b", "a"}
			}
			c.limits["ns"] = 60
			want := map[string]float64{}
			for host, own := range map[string]float64{"a": 72, "b": 18} {
				want[host], _ = Share(c.settings.Params(), 60, Demand{Own: own, Peers: 90 - own, Members: 2, Known: 1})
			}
			band := c.settings.Deadband * 60 / 2 // Deadband of an equal share
			c.round()

			// The requests start 2 s before the first round, so the windows count from the
			// round before them until the seventh. The frontend whose round comes first would
			// set its share from its own window and the other's a round behind it, so until
			// the windows have filled, each keeps an equal share. Then b's share falls at once,
			// and a's rises a round later, once b's has fallen.
			for round := 1; round <= 20; round++ {
				seconds := 5
				if round == 1 {
					seconds = 2
				}
				c.load(map[string]int{"a": seconds * 72, "b": seconds * 18}, "ns")
				shares := map[string]float64{}
				for _, host := range []string{"a", "b"} {
					share, ok := c.share(host, "ns")
					require.True(t, ok)
					shares[host] = share
				}
				require.LessOrEqual(t, shares["a"]+shares["b"], 60+1e-9, "round %d", round)
				switch {
				case round < 7:
					for host, share := range shares {
						require.InDelta(t, 30.0, share, 1e-9, "%s in round %d", host, round)
						d, _ := c.decisions.lastFrom(host, "ns")
						require.Equal(t, adaptive.Reason(ReasonFilling), d.Reason)
					}
				case round >= 9:
					// Each frontend holds a share no higher than the demand gives it, and less
					// than Deadband below it.
					for host, share := range shares {
						require.LessOrEqual(t, share, want[host]+1e-9, "%s in round %d", host, round)
						require.Greater(t, share, want[host]-band, "%s in round %d", host, round)
					}
				default:
					// In rounds 7 and 8 the windows have filled, and the shares leave the equal
					// split, b's first.
				}
			}

			// Each decision replays as tools/adaptivereplay reads it: its detail gives the
			// share it proposed and, unless it held, its reason. None held a share above the
			// one proposed, or kept other than an equal share while the windows filled.
			c.decisions.mu.Lock()
			defer c.decisions.mu.Unlock()
			for _, d := range c.decisions.decisions {
				detail := d.Detail.(Detail)
				share, reason := Share(detail.Params, detail.Limit, detail.Demand)
				require.InDelta(t, share, d.Proposed, 1e-9)
				switch d.Reason {
				case adaptive.Reason(ReasonHeld):
					require.LessOrEqual(t, d.Applied, d.Proposed)
				case adaptive.Reason(ReasonFilling):
					require.InDelta(t, d.Static, d.Applied, 1e-9)
				default:
					require.Equal(t, adaptive.Reason(reason), d.Reason)
				}
			}
		})
	}
}

func TestEstablishedSharesDoNotWaitForAnotherFrontendsWindow(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameEnabled, "a", "b")
	c.limits["ns"] = 100
	c.round()
	c.fill(map[string]int{"a": 750}, "ns")
	a, _ := c.share("a", "ns")
	require.InDelta(t, 90.0, a, 1e-6, "a's demand passes the limit, and b has none")

	// b's requests start, so its window fills. The frontends already apply shares that
	// follow the demand, so they keep doing so rather than go back to equal shares.
	for range 3 {
		c.load(map[string]int{"a": 750, "b": 50}, "ns")
		for _, host := range []string{"a", "b"} {
			d, _ := c.decisions.lastFrom(host, "ns")
			require.NotEqual(t, adaptive.Reason(ReasonFilling), d.Reason, host)
		}
	}
	a, _ = c.share("a", "ns")
	require.InDelta(t, 10+80*150/160.0, a, 1e-6)
	_, filling := c.exchanges["b"].measure(c.ts.Now(), c.settings.Window)
	require.True(t, filling[testLimit]["ns"])
}

func TestFrontendsWithoutFreshReportsKeepAnEqualShare(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameEnabled, "a", "b", "c")
	c.settings.Deadband = 0
	c.limits["ns"] = 90

	// c's rounds never run, so a and b each keep it an equal share of 30, and split the
	// other 60 by their demand: floors of 6, and 48 above them.
	delete(c.exchanges, "c")
	c.round()
	c.fill(map[string]int{"a": 750, "b": 50}, "ns")
	a, _ := c.share("a", "ns")
	b, _ := c.share("b", "ns")
	require.InDelta(t, 6+48*150/160.0, a, 1e-6)
	require.InDelta(t, 6+48*10/160.0, b, 1e-6)

	// Once b's rounds stop and its report is older than StaleAfter, a keeps b an equal
	// share too, and a's own share is an equal one.
	delete(c.exchanges, "b")
	for range 4 {
		c.load(map[string]int{"a": 750}, "ns")
	}
	a, _ = c.share("a", "ns")
	require.InDelta(t, 30.0, a, 1e-6)
}

func TestNamespaceWithoutDemandGoesBackToAnEqualShare(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameEnabled, "a", "b")
	c.limits["ns"] = 100
	c.round()
	for range 3 {
		c.load(map[string]int{"a": 750, "b": 50}, "ns")
	}
	_, ok := c.share("a", "ns")
	require.True(t, ok)

	for range 8 {
		c.load(nil, "ns")
	}
	_, ok = c.share("a", "ns")
	require.False(t, ok)
	d, _ := c.decisions.last("ns")
	require.Equal(t, adaptive.Reason(ReasonNoDemand), d.Reason)
	require.InDelta(t, d.Static, d.Applied, 1e-9)
	// Its detail gives the share it reports, as a replay reads it.
	detail := d.Detail.(Detail)
	share, reason := Share(detail.Params, detail.Limit, detail.Demand)
	require.Equal(t, ReasonNoDemand, reason)
	require.InDelta(t, d.Proposed, share, 1e-9)
}

func TestDisabledControllerCountsNothingAndAppliesNothing(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameEnabled, "a", "b")
	c.limits["ns"] = 100
	c.round()
	for range 3 {
		c.load(map[string]int{"a": 750, "b": 50}, "ns")
	}
	_, ok := c.share("a", "ns")
	require.True(t, ok)

	c.settings.Mode = adaptive.ModeNameDisabled
	c.load(map[string]int{"a": 750, "b": 50}, "ns")
	_, ok = c.share("a", "ns")
	require.False(t, ok)
	require.False(t, c.exchanges["a"].Active())
	c.load(map[string]int{"a": 750}, "ns")
	_, counted := c.exchanges["a"].class(testLimit).counts.count("ns")
	require.False(t, counted)

	// A disabled frontend's reply doesn't count as a report.
	c.settings.Mode = adaptive.ModeNameEnabled
	c.exchanges["a"].round(t.Context())
	require.Empty(t, c.exchanges["a"].fresh(c.ts.Now(), c.settings.StaleAfter, c.members))
}

func TestFrontendDoesNotCountItselfAsAnother(t *testing.T) {
	c := newTestCluster(t, adaptive.ModeNameEnabled, "a", "b")
	c.settings.Deadband = 0
	c.limits["ns"] = 100
	// a also answers at another address, which membership lists as a third frontend.
	c.pipes["a-alias"] = c.pipes["a"]
	c.members = []string{"a", "b", "a-alias"}
	c.round()
	for range 3 {
		c.load(map[string]int{"a": 750, "b": 50}, "ns")
	}
	require.NotContains(t, c.exchanges["a"].fresh(c.ts.Now(), c.settings.StaleAfter, c.members), "a-alias")
}

func TestNilExchangeLeavesTheLimitsAsTheyAre(t *testing.T) {
	var e *Exchange
	require.Nil(t, e.Shares(testLimit, nil))
	require.Equal(t, quotas.NoopRequestRateLimiter, e.Count(testLimit, quotas.NoopRequestRateLimiter))
}

func TestExchangeNeedsClusterAdminAccess(t *testing.T) {
	require.Equal(t, api.MethodMetadata{Scope: api.ScopeCluster, Access: api.AccessAdmin}, api.GetMethodMetadata(exchangeMethod))
}

func TestReportsRoundTrip(t *testing.T) {
	demand := map[string]map[string]float64{testLimit: {"ns": 12.5, "other": 3}, "visibility": {}}
	filling := map[string]map[string]bool{testLimit: {"other": true}, "visibility": {}}
	host, running, age, got := readReport(newReport("a:7233", true, demand, filling))
	require.Equal(t, "a:7233", host)
	require.True(t, running)
	require.Zero(t, age)
	require.Equal(t, demand, got.demand)
	require.Equal(t, filling, got.filling)

	// A report from a frontend that doesn't send the namespaces whose window fills has none.
	report := newReport("a:7233", true, demand, nil)
	delete(report.Fields, "filling")
	_, _, _, got = readReport(report)
	require.Equal(t, demand, got.demand)
	require.Empty(t, got.filling)
}
