package adaptivesim

import (
	"math/rand/v2"
	"time"
)

// dialKind is what a connection attempt is for.
type dialKind int

const (
	// poolDial is a pool opening a connection, after the plugin's pacing.
	poolDial dialKind = iota
	// cancelDial is pgx sending a cancel request, on a connection of its own, for a query
	// it cut off. It goes through the connection config's DialFunc, so the plugin's pacing
	// doesn't see it.
	cancelDial
)

// rejection is how DSQL answered a connection attempt.
type rejection int

const (
	admitted rejection = iota
	// rejectedRate is SQLSTATE 53400: the cluster's connection rate.
	rejectedRate
	// rejectedCap is SQLSTATE 53300: the cluster's connection limit.
	rejectedCap
)

type (
	// tokenBucket is a token bucket on simulated time.
	tokenBucket struct {
		rate, burst, tokens float64
		last                time.Time
	}

	// dsql is the cluster's side of connections: its admission of new connections, its
	// connection limit and its cutoff of old connections.
	dsql struct {
		l         *loop
		cfg       Cluster
		net       Network
		rng       *rand.Rand
		admission *tokenBucket
		open      int
		peakOpen  int
		counts    perSecond[ConnCounts]
	}

	// conn is one connection, from DSQL's side and the pool's.
	conn struct {
		opened, expires time.Time
		idleSince       time.Time
		// broken is set once pgx has closed the connection on its side, after a query it
		// cut off or DSQL's cutoff. The pool destroys a broken connection on its release.
		broken bool
		// serverClosed is set once DSQL no longer counts the connection.
		serverClosed bool
		cutoff       *event
		// onCut is called when DSQL cuts the connection off while it's open: the pool sets
		// it while the connection is idle, and a statement while it runs.
		onCut func()
	}

	// ConnCounts are connection attempts and their outcomes: pools' dials and cancel
	// requests, the connections opened, DSQL's rejections for its connection rate (53400)
	// and its connection limit (53300), and connections it closed at their maximum age.
	ConnCounts struct {
		Dials, Cancels, Opens, RejectedRate, RejectedCap, Cuts int
	}
)

func newTokenBucket(rate, burst float64, now time.Time) *tokenBucket {
	return &tokenBucket{rate: rate, burst: burst, tokens: burst, last: now}
}

func (b *tokenBucket) refill(now time.Time) {
	if now.After(b.last) {
		b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
		b.last = now
	}
}

// take takes a token if there is one.
func (b *tokenBucket) take(now time.Time) bool {
	b.refill(now)
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// reserve takes a token, going into debt for it as golang.org/x/time/rate does, and
// returns how long the caller waits until the token is due.
func (b *tokenBucket) reserve(now time.Time) time.Duration {
	b.refill(now)
	b.tokens--
	if b.tokens >= 0 {
		return 0
	}
	return time.Duration(-b.tokens / b.rate * float64(time.Second))
}

func newDSQL(l *loop, cfg Cluster, net Network, seed uint64) *dsql {
	return &dsql{
		l:         l,
		cfg:       cfg,
		net:       net,
		rng:       randoms(seed, "dsql"),
		admission: newTokenBucket(cfg.Rate, cfg.Burst, l.now),
		counts:    perSecond[ConnCounts]{start: l.now},
	}
}

// dial opens a connection. DSQL admits or rejects the attempt as it arrives, and the
// dialer learns which after the connect cost, which a rejection costs too. done gets the
// connection when it's admitted; a cancel request gets none, and is done once DSQL has
// closed its connection.
func (d *dsql) dial(kind dialKind, done func(*conn, rejection)) {
	now := d.l.now
	counts := d.counts.at(now)
	if kind == cancelDial {
		counts.Cancels++
	} else {
		counts.Dials++
	}
	cost := between(d.rng, d.net.Connect.Min, d.net.Connect.Max)
	switch {
	case d.open >= d.cfg.MaxConnections:
		counts.RejectedCap++
		d.l.after(cost, func() { done(nil, rejectedCap) })
	case !d.admission.take(now):
		counts.RejectedRate++
		d.l.after(cost, func() { done(nil, rejectedRate) })
	default:
		d.admit(kind, cost, done)
	}
}

// admit opens a connection DSQL admitted, once its connect cost has passed. A cancel
// request holds its connection until DSQL has canceled the query.
func (d *dsql) admit(kind dialKind, cost time.Duration, done func(*conn, rejection)) {
	d.open++
	d.peakOpen = max(d.peakOpen, d.open)
	if kind == cancelDial {
		d.l.after(cost+d.net.Cancel, func() {
			d.open--
			done(nil, admitted)
		})
		return
	}
	d.l.after(cost, func() {
		c := &conn{opened: d.l.now}
		c.cutoff = d.l.after(d.cfg.MaxAge, func() { d.cut(c) })
		d.counts.at(d.l.now).Opens++
		done(c, admitted)
	})
}

// close closes a connection on DSQL's side.
func (d *dsql) close(c *conn) {
	if c.serverClosed {
		return
	}
	c.serverClosed = true
	c.cutoff.cancel()
	d.open--
}

// cut is DSQL closing a connection at its maximum age.
func (d *dsql) cut(c *conn) {
	if c.serverClosed {
		return
	}
	c.serverClosed = true
	c.broken = true
	d.open--
	d.counts.at(d.l.now).Cuts++
	if c.onCut != nil {
		c.onCut()
	}
}

// cancelQuery is pgx's asyncClose for a query cut off at its deadline: it sends a cancel
// request on a new connection, then closes the query's connection.
func (d *dsql) cancelQuery(c *conn) {
	c.broken = true
	d.dial(cancelDial, func(*conn, rejection) { d.close(c) })
}

// perSecond keeps counts for each second of a run.
type perSecond[T any] struct {
	start time.Time
	secs  []T
}

func (p *perSecond[T]) at(now time.Time) *T {
	i := int(now.Sub(p.start) / time.Second)
	for len(p.secs) <= i {
		var zero T
		p.secs = append(p.secs, zero)
	}
	return &p.secs[i]
}
