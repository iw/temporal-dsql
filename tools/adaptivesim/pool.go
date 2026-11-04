package adaptivesim

import (
	"math/rand/v2"
	"time"
)

// pacer is the plugin's BeforeConnect: each of a process's connects takes one of the
// process's attempt slots while it waits for the connection limiter, and gives it back
// before it dials.
type pacer struct {
	l       *loop
	dsql    *dsql
	slots   int
	busy    int
	waiting fifo[func()]
	// bucket is the process's own limiter, or, with a shared one, the cluster-wide bucket
	// that every process reserves from, each reservation taking latency.
	bucket  *tokenBucket
	latency Range
	rng     *rand.Rand
}

func newPacer(l *loop, d *dsql, cfg Pacing, shared *tokenBucket, seed uint64, name string) *pacer {
	p := &pacer{l: l, dsql: d, slots: cfg.AttemptSlots, rng: randoms(seed, name+"/pacer")}
	if shared != nil {
		p.bucket, p.latency = shared, cfg.Latency
	} else {
		p.bucket = newTokenBucket(cfg.Rate, cfg.Burst, l.now)
	}
	return p
}

// connect paces a connection attempt, then dials it.
func (p *pacer) connect(done func(*conn, rejection)) {
	if p.busy < p.slots {
		p.busy++
		p.reserve(done)
		return
	}
	p.waiting.push(func() { p.reserve(done) })
}

func (p *pacer) reserve(done func(*conn, rejection)) {
	p.l.after(between(p.rng, p.latency.Min, p.latency.Max), func() {
		p.l.after(p.bucket.reserve(p.l.now), func() {
			if next, ok := p.waiting.pop(); ok {
				next()
			} else {
				p.busy--
			}
			p.dsql.dial(poolDial, done)
		})
	})
}

// acquireResult is how a pool acquire ended.
type acquireResult int

const (
	acquired acquireResult = iota
	// acquireTimedOut is the caller's deadline passing before it had a connection.
	acquireTimedOut
	// acquireRejected is DSQL rejecting the connection the caller waited for.
	acquireRejected
)

// healthCheckDelay is how long after a destroyed connection pgxpool checks the pool's
// health, as triggerHealthCheck does.
const healthCheckDelay = 500 * time.Millisecond

type (
	// pool is the plugin's pgxpool as puddle runs it: a semaphore of MaxConns units, which
	// acquired connections and constructions hold, waited for first in, first out; idle
	// connections reused last in, first out; construction on demand when no connection is
	// idle; and pgxpool's minimum fill, lifetimes and health checks.
	pool struct {
		l     *loop
		cfg   Pool
		net   Network
		pacer *pacer
		dsql  *dsql
		rng   *rand.Rand

		held    int // semaphore units held
		waiters fifo[*acquirer]
		live    int // waiters that haven't given up
		idle    []*conn
		total   int // connections, idle, acquired or being constructed
		closed  bool

		// What pgxpool's Stat reports, and the connections opened.
		acquires, emptyAcquires, canceledAcquires int64
		opened, lifetimeDestroys                  int

		healthTimer       *event
		checking, pending bool
	}

	acquirer struct {
		waited   bool
		state    acquirerState
		expire   *event
		done     func(*conn, acquireResult)
		deadline time.Time
	}
)

type acquirerState int

const (
	waitingForUnit acquirerState = iota
	constructing
	finished
)

func newPool(l *loop, cfg Pool, net Network, pacer *pacer, d *dsql, seed uint64, name string) *pool {
	return &pool{l: l, cfg: cfg, net: net, pacer: pacer, dsql: d, rng: randoms(seed, name+"/pool")}
}

// start opens MinConns connections in the background, then starts the health checks, as
// pgxpool does when it's created.
func (p *pool) start() {
	p.createIdle(p.cfg.MinConns, func(bool) {
		p.l.after(p.cfg.HealthCheck, p.tick)
	})
}

// acquire gives done a connection, or how it failed. A zero deadline waits for as long as
// it takes.
func (p *pool) acquire(deadline time.Time, done func(*conn, acquireResult)) {
	a := &acquirer{done: done, deadline: deadline}
	if !deadline.IsZero() {
		a.expire = p.l.at(deadline, func() { p.expire(a) })
	}
	// The semaphore's TryAcquire fails while others wait.
	if p.held < p.cfg.MaxConns && p.live == 0 {
		p.held++
		p.take(a)
		return
	}
	a.waited = true
	p.live++
	p.waiters.push(a)
}

func (p *pool) expire(a *acquirer) {
	if a.state == finished {
		return
	}
	// A construction goes on after its caller has gone, and its connection goes idle.
	if a.state == waitingForUnit {
		p.live--
	}
	a.state = finished
	p.canceledAcquires++
	p.l.soon(func() { a.done(nil, acquireTimedOut) })
}

// take gives an acquirer that holds a semaphore unit an idle connection, or constructs one.
func (p *pool) take(a *acquirer) {
	for len(p.idle) > 0 {
		c := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		c.onCut = nil
		if c.broken {
			p.destroy(c)
			continue
		}
		p.grant(a, c, a.waited, p.l.now.Sub(c.idleSince) > time.Second)
		return
	}
	a.state = constructing
	p.total++
	p.construct(func(c *conn, rej rejection) {
		if rej != admitted {
			p.total--
			p.releaseUnit()
			if a.state != finished {
				a.state = finished
				a.expire.cancel()
				a.done(nil, acquireRejected)
			}
			return
		}
		if a.state == finished {
			p.toIdle(c)
			return
		}
		p.grant(a, c, true, false)
	})
}

// grant hands an acquirer its connection. An acquire that waited for a semaphore unit or
// for a construction is an empty acquire. pgxpool pings a connection idle for more than a
// second first.
func (p *pool) grant(a *acquirer, c *conn, empty, ping bool) {
	a.state = finished
	a.expire.cancel()
	p.acquires++
	if empty {
		p.emptyAcquires++
	}
	if !ping {
		p.l.soon(func() { a.done(c, acquired) })
		return
	}
	p.l.after(p.net.RoundTrip, func() { a.done(c, acquired) })
}

// construct opens a connection through the plugin's pacing.
func (p *pool) construct(done func(*conn, rejection)) {
	p.pacer.connect(func(c *conn, rej rejection) {
		if c != nil {
			p.opened++
			jitter := time.Duration(p.rng.Float64()*p.cfg.LifetimeJitter.Seconds()) * time.Second
			c.expires = c.opened.Add(p.cfg.Lifetime + jitter)
		}
		done(c, rej)
	})
}

// release returns an acquired connection, as pgxpool's Conn.Release does.
func (p *pool) release(c *conn) {
	c.onCut = nil
	switch {
	case p.closed:
		p.destroy(c)
	case c.broken:
		p.destroy(c)
		p.triggerHealthCheck()
	case p.expired(c):
		p.lifetimeDestroys++
		p.destroy(c)
		p.triggerHealthCheck()
	default:
		p.idle = append(p.idle, c)
		p.setIdle(c)
	}
	p.releaseUnit()
}

func (p *pool) expired(c *conn) bool {
	return p.cfg.Lifetime > 0 && p.l.now.After(c.expires)
}

func (p *pool) setIdle(c *conn) {
	c.idleSince = p.l.now
	c.onCut = func() { p.removeIdle(c) }
}

func (p *pool) toIdle(c *conn) {
	if p.closed {
		p.destroy(c)
	} else {
		p.idle = append(p.idle, c)
		p.setIdle(c)
	}
	p.releaseUnit()
}

func (p *pool) removeIdle(c *conn) {
	for i, idle := range p.idle {
		if idle == c {
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			p.destroy(c)
			return
		}
	}
}

func (p *pool) destroy(c *conn) {
	p.total--
	if !c.broken {
		p.dsql.close(c)
	}
}

// releaseUnit gives back a semaphore unit, to the longest waiter.
func (p *pool) releaseUnit() {
	p.held--
	for p.held < p.cfg.MaxConns {
		a, ok := p.waiters.pop()
		if !ok {
			return
		}
		if a.state != waitingForUnit {
			continue
		}
		p.live--
		p.held++
		p.take(a)
	}
}

// close closes the pool: idle connections now, acquired ones as they're released.
func (p *pool) close() {
	p.closed = true
	for _, c := range p.idle {
		c.onCut = nil
		p.destroy(c)
	}
	p.idle = nil
}

// createIdle is pgxpool's createIdleResources: n background constructions, each holding a
// semaphore unit, which give up at once when the semaphore has no unit free or has
// waiters. done learns whether any construction failed.
func (p *pool) createIdle(n int, done func(failed bool)) {
	if n <= 0 {
		done(false)
		return
	}
	pending, anyFailed := n, false
	finish := func(failed bool) {
		anyFailed = anyFailed || failed
		pending--
		if pending == 0 {
			done(anyFailed)
		}
	}
	for range n {
		if p.closed || p.held >= p.cfg.MaxConns || p.live > 0 || p.total >= p.cfg.MaxConns {
			// puddle's ErrNotAvailable, which pgxpool ignores.
			finish(false)
			continue
		}
		p.held++
		p.total++
		p.construct(func(c *conn, rej rejection) {
			if rej != admitted {
				p.total--
				p.releaseUnit()
				finish(true)
				return
			}
			p.toIdle(c)
			finish(false)
		})
	}
}

func (p *pool) tick() {
	if p.closed {
		return
	}
	p.requestCheck()
	p.l.after(p.cfg.HealthCheck, p.tick)
}

// triggerHealthCheck checks the pool's health healthCheckDelay after the last destroyed
// connection.
func (p *pool) triggerHealthCheck() {
	p.healthTimer.cancel()
	p.healthTimer = p.l.after(healthCheckDelay, p.requestCheck)
}

// requestCheck runs a health check, or one more after the one running.
func (p *pool) requestCheck() {
	if p.closed {
		return
	}
	if p.checking {
		p.pending = true
		return
	}
	p.checking = true
	p.checkHealth()
}

// checkHealth is pgxpool's checkHealth: fill to MinConns, then destroy expired idle
// connections, and again 500 ms later while it destroys any.
func (p *pool) checkHealth() {
	p.createIdle(p.cfg.MinConns-p.total, func(failed bool) {
		if failed || !p.checkConns() || p.closed {
			p.checking = false
			if p.pending {
				p.pending = false
				p.requestCheck()
			}
			return
		}
		p.l.after(healthCheckDelay, p.checkHealth)
	})
}

// checkConns is pgxpool's checkConnsHealth: it destroys idle connections past their
// lifetime while the pool has at least MinConns, and reports whether it destroyed any.
func (p *pool) checkConns() bool {
	total := p.total
	kept := p.idle[:0]
	destroyed := false
	for _, c := range p.idle {
		if p.expired(c) && total >= p.cfg.MinConns {
			c.onCut = nil
			p.lifetimeDestroys++
			p.destroy(c)
			total--
			destroyed = true
			continue
		}
		kept = append(kept, c)
	}
	clear(p.idle[len(kept):])
	p.idle = kept
	return destroyed
}
