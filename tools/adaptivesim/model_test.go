package adaptivesim

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence"
)

func TestLoopRunsEventsByTimeThenByScheduling(t *testing.T) {
	l := newLoop(epoch)
	var order []string
	l.after(2*time.Second, func() { order = append(order, "late") })
	l.after(time.Second, func() {
		order = append(order, "first")
		l.soon(func() { order = append(order, "soon") })
	})
	l.after(time.Second, func() { order = append(order, "second") })
	canceled := l.after(time.Second, func() { order = append(order, "canceled") })
	canceled.cancel()

	l.run(epoch.Add(1500 * time.Millisecond))
	require.Equal(t, []string{"first", "second", "soon"}, order)
	require.Equal(t, epoch.Add(1500*time.Millisecond), l.now)
	l.run(epoch.Add(time.Minute))
	require.Equal(t, []string{"first", "second", "soon", "late"}, order)
}

func TestSimClockTimers(t *testing.T) {
	l := newLoop(epoch)
	c := simClock{l: l}
	fired := 0
	timer := c.AfterFunc(time.Second, func() { fired++ })
	ch, stopped := c.NewTimer(time.Second)
	require.True(t, stopped.Stop())
	l.run(epoch.Add(2 * time.Second))
	require.Equal(t, 1, fired)
	require.False(t, timer.Stop())
	require.Empty(t, ch)
	require.Equal(t, time.Second, c.Since(epoch.Add(time.Second)))
}

func TestFifoKeepsOrderAcrossCompaction(t *testing.T) {
	var q fifo[int]
	for i := range 5000 {
		q.push(i)
	}
	for i := range 3000 {
		v, ok := q.pop()
		require.True(t, ok)
		require.Equal(t, i, v)
	}
	q.push(5000)
	require.Equal(t, 2001, q.len())
	for i := 3000; i <= 5000; i++ {
		v, ok := q.pop()
		require.True(t, ok)
		require.Equal(t, i, v)
	}
	_, ok := q.pop()
	require.False(t, ok)
}

func TestTokenBucket(t *testing.T) {
	b := newTokenBucket(10, 2, epoch)
	require.True(t, b.take(epoch))
	require.True(t, b.take(epoch))
	require.False(t, b.take(epoch))
	require.True(t, b.take(epoch.Add(100*time.Millisecond)))

	// A reservation goes into debt, and waits for it.
	b = newTokenBucket(10, 1, epoch)
	require.Zero(t, b.reserve(epoch))
	require.Equal(t, 100*time.Millisecond, b.reserve(epoch))
	require.Equal(t, 200*time.Millisecond, b.reserve(epoch))
}

func TestBackoffDelay(t *testing.T) {
	b := Backoff{Initial: 3 * time.Second, Coefficient: 1.5, Max: 5 * time.Second}
	require.Equal(t, 3*time.Second, b.delay(0))
	require.Equal(t, 4500*time.Millisecond, b.delay(1))
	require.Equal(t, 5*time.Second, b.delay(2))
}

// testPool is a pool on its own cluster, with no pacing to speak of, connects that take
// 100 ms, and 10 ms round trips.
func testPool(t *testing.T, cfg Pool) (*loop, *dsql, *pool) {
	t.Helper()
	l := newLoop(epoch)
	net := Network{RoundTrip: 10 * time.Millisecond, Connect: Range{Min: 100 * time.Millisecond, Max: 100 * time.Millisecond}, Cancel: 10 * time.Millisecond}
	d := newDSQL(l, dsqlCluster(), net, 1)
	pacer := newPacer(l, d, Pacing{AttemptSlots: 8, Rate: 1000, Burst: 1000}, nil, 1, "test")
	if cfg.HealthCheck == 0 {
		cfg.HealthCheck = time.Minute
	}
	p := newPool(l, cfg, net, pacer, d, 1, "test")
	p.start()
	return l, d, p
}

type acquireOutcome struct {
	c      *conn
	result acquireResult
	at     time.Time
}

func acquireInto(l *loop, p *pool, deadline time.Time, out *[]acquireOutcome) {
	p.acquire(deadline, func(c *conn, result acquireResult) {
		*out = append(*out, acquireOutcome{c: c, result: result, at: l.now})
	})
}

func TestPoolConstructsOnDemandAndReusesTheLastReleased(t *testing.T) {
	l, _, p := testPool(t, Pool{MaxConns: 2})
	var got []acquireOutcome
	acquireInto(l, p, time.Time{}, &got)
	acquireInto(l, p, time.Time{}, &got)
	l.run(epoch.Add(time.Second))
	require.Len(t, got, 2)
	require.Equal(t, epoch.Add(100*time.Millisecond), got[0].at)
	require.Equal(t, 2, p.opened)

	p.release(got[0].c)
	p.release(got[1].c)
	acquireInto(l, p, time.Time{}, &got)
	l.run(epoch.Add(1100 * time.Millisecond))
	require.Same(t, got[1].c, got[2].c, "idle connections are reused last in, first out")
	require.Equal(t, int64(3), p.acquires)
	require.Equal(t, int64(2), p.emptyAcquires, "only the constructions waited")
}

func TestPoolWaitersAreServedInTurnAndCountAsEmpty(t *testing.T) {
	l, _, p := testPool(t, Pool{MaxConns: 1})
	var got []acquireOutcome
	acquireInto(l, p, time.Time{}, &got)
	l.run(epoch.Add(time.Second))
	acquireInto(l, p, time.Time{}, &got)
	acquireInto(l, p, epoch.Add(1500*time.Millisecond), &got)
	acquireInto(l, p, time.Time{}, &got)
	l.run(epoch.Add(2 * time.Second))
	require.Len(t, got, 2, "the one that timed out")
	require.Equal(t, acquireTimedOut, got[1].result)

	p.release(got[0].c)
	l.run(epoch.Add(3 * time.Second))
	require.Len(t, got, 3)
	require.Same(t, got[0].c, got[2].c)
	require.Equal(t, int64(2), p.acquires)
	require.Equal(t, int64(2), p.emptyAcquires)
	require.Equal(t, int64(1), p.canceledAcquires)
	require.Equal(t, 1, p.live, "the last waiter still waits")
}

func TestPoolConstructionOutlivesItsCaller(t *testing.T) {
	l, _, p := testPool(t, Pool{MaxConns: 1})
	var got []acquireOutcome
	acquireInto(l, p, epoch.Add(50*time.Millisecond), &got)
	l.run(epoch.Add(time.Second))
	require.Len(t, got, 1)
	require.Equal(t, acquireTimedOut, got[0].result)
	require.Len(t, p.idle, 1, "the connection went idle")
	require.Zero(t, p.held)

	acquireInto(l, p, time.Time{}, &got)
	l.run(epoch.Add(2 * time.Second))
	require.Equal(t, acquired, got[1].result)
	require.Equal(t, int64(1), p.acquires)
	require.Zero(t, p.emptyAcquires)
}

func TestPoolReplacesAConnectionACutOffQueryClosed(t *testing.T) {
	l, d, p := testPool(t, Pool{MaxConns: 1, MinConns: 1})
	l.run(epoch.Add(time.Second))
	require.Equal(t, 1, p.opened)
	var got []acquireOutcome
	acquireInto(l, p, time.Time{}, &got)
	l.run(epoch.Add(2 * time.Second))
	require.Len(t, got, 1)

	d.cancelQuery(got[0].c)
	p.release(got[0].c)
	require.Zero(t, p.total)
	l.run(epoch.Add(3 * time.Second))
	require.Equal(t, 2, p.opened, "the health check after the destroy refills the pool")
	require.Len(t, p.idle, 1)
	total := ConnCounts{}
	for _, c := range d.counts.secs {
		total.add(c)
	}
	require.Equal(t, ConnCounts{Dials: 2, Cancels: 1, Opens: 2}, total)
	require.Equal(t, 1, d.open)
}

func TestPoolRetiresConnectionsAtTheirLifetime(t *testing.T) {
	l, d, p := testPool(t, Pool{MaxConns: 2, MinConns: 2, Lifetime: 10 * time.Minute, HealthCheck: time.Minute})
	l.run(epoch.Add(9 * time.Minute))
	require.Equal(t, 2, p.opened)
	l.run(epoch.Add(12 * time.Minute))
	require.Equal(t, 2, p.lifetimeDestroys)
	require.Equal(t, 4, p.opened)
	require.Len(t, p.idle, 2)
	require.Equal(t, 2, d.open)
}

func TestDSQLRejectsPastItsRateAndCutsOldConnections(t *testing.T) {
	l := newLoop(epoch)
	net := Network{Connect: Range{Min: 10 * time.Millisecond, Max: 10 * time.Millisecond}}
	d := newDSQL(l, Cluster{Rate: 1, Burst: 2, MaxConnections: 10, MaxAge: time.Hour}, net, 1)
	var conns []*conn
	var rejections []rejection
	for range 3 {
		d.dial(poolDial, func(c *conn, rej rejection) {
			rejections = append(rejections, rej)
			if c != nil {
				conns = append(conns, c)
			}
		})
	}
	l.run(epoch.Add(time.Second))
	require.Equal(t, []rejection{admitted, admitted, rejectedRate}, rejections)
	require.Equal(t, 2, d.open)

	cut := false
	conns[0].onCut = func() { cut = true }
	l.run(epoch.Add(2 * time.Hour))
	require.True(t, cut)
	require.True(t, conns[0].broken)
	require.Zero(t, d.open)
}

func TestHealthSnapshot(t *testing.T) {
	h := newHealth(Signals{Window: time.Second, Windows: 10})
	at := epoch.Add(10 * time.Second)
	for i := range 100 {
		now := at.Add(time.Duration(i) * 20 * time.Millisecond)
		f, passed := ok, false
		if i%10 == 0 {
			f, passed = unavailable, true
		}
		h.recordCall(now, time.Duration(i)*time.Millisecond, f, passed)
	}
	h.recordCall(at, 0, limited, false)
	h.reportFact(at, persistence.StoreFactThrottled, 5)
	h.reportFact(at, persistence.StoreFactQueued, 3)
	h.reportFact(at, persistence.StoreFactAcquired, 12)
	h.factsDeclared = true

	snap := h.snapshot(at.Add(2500*time.Millisecond), 3)
	require.Equal(t, 100, snap.Samples)
	require.Equal(t, 1, snap.Rejected)
	require.InDelta(t, 0.1, snap.ErrorRatio, 1e-9)
	require.Equal(t, 2500*time.Millisecond, snap.Span)
	require.InDelta(t, (100-10-5)/2.5, snap.Throughput, 1e-9)
	require.Equal(t, 98*time.Millisecond, snap.P99, "the 99th of 100 samples, 0 to 99 ms")
	require.True(t, snap.StoreReportsFacts)
	require.Equal(t, 5, snap.Throttled)
	require.Equal(t, 3, snap.Queued)
	require.Equal(t, 12, snap.Acquired)

	// Windows older than the snapshot's span are left out.
	require.Zero(t, h.snapshot(at.Add(time.Minute), 3).Samples)
}
