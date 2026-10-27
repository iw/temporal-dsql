package main

import (
	"math/rand"
	"time"
)

type modelPool struct {
	pendingAcquires                  map[*modelRequest]struct{}
	healthStarted                    bool
	finalSampled                     bool
	onFirstAcquire                   func()
	instance                         *instance
	name, model                      string
	cfg                              ServiceConfig
	rng                              *rand.Rand
	stats                            *poolStats
	closed                           bool
	ephemeral                        bool
	ready, idle                      []*physicalConn
	owned                            map[int64]*physicalConn
	constructing, physical, borrowed int
	waiters                          []*modelRequest
	healthRunning, healthPending     bool
	lifetimeDebt                     int
}

func (p *modelPool) run() *comparisonRun { return p.instance.run }
func (p *modelPool) now() time.Time      { return p.run().sim.Now() }
func (p *modelPool) isPGX() bool         { return p.model == "pgxpool" }
func (p *modelPool) leak() bool {
	return p.model == "reservoir-sql" && boolDefault(p.cfg.LeakOnClose, true)
}
func (p *modelPool) reconnect() bool {
	return p.model == "reservoir-sql" && boolDefault(p.cfg.ReconnectOnConnError, true)
}
func (p *modelPool) total() int { return len(p.owned) + p.constructing }
func (p *modelPool) limit() int {
	if p.isPGX() {
		return p.cfg.PGX.MaxConns
	}
	return p.cfg.MaxOpen
}
func (p *modelPool) target() int {
	if p.isPGX() {
		return p.cfg.PGX.MinConns
	}
	return p.cfg.TargetReady
}

// pgxpool.NewWithConfig's constructor adds nonnegative jitter, truncated to
// whole seconds, to MaxConnLifetime after ConnectConfig completes.
func (p *modelPool) lifetime() time.Duration {
	if p.isPGX() {
		jitter := time.Duration(p.rng.Float64()*p.cfg.PGX.MaxConnLifetimeJitter.Seconds()) * time.Second
		return p.cfg.PGX.MaxConnLifetime + jitter
	}
	if p.cfg.Jitter <= 0 {
		return p.cfg.BaseLifetime
	}
	return p.cfg.BaseLifetime + time.Duration(p.rng.Int63n(int64(p.cfg.Jitter)))
}

func (p *modelPool) guarded(c *physicalConn) bool {
	return !p.now().Before(c.expires.Add(-p.cfg.GuardWindow))
}

func (p *modelPool) start() {
	if p.isPGX() {
		// pgxpool.NewWithConfig creates MinConns asynchronously before the first
		// backgroundHealthCheck. Constructors count against puddle MaxSize.
		p.health()
	} else {
		p.refill()
		p.run().sim.After(time.Second, "reservoir-scan", p.scan)
	}
}

func (p *modelPool) create(done func(*physicalConn, string)) {
	p.createWithCancel(nil, done)
}

func (p *modelPool) createWithCancel(cancelled func() bool, done func(*physicalConn, string)) {
	p.constructing++
	p.instance.beforeConnect(p, cancelled, func(c *physicalConn, code string) {
		p.constructing--
		if c != nil {
			if p.closed {
				// Late constructor completions belong to the old pool, never its replacement.
				if !p.leak() {
					p.run().cluster.close(c)
				}
				done(nil, "closed")
				return
			}
			p.owned[c.id] = c
			if p.lifetimeDebt > 0 {
				p.lifetimeDebt--
				if p.now().Sub(p.run().start) >= p.run().cfg.SteadyAfter {
					p.run().stats.churnOpens++
					p.stats.churnOpens++
				}
			}
		}
		done(c, code)
	})
}

// Reservoir design: a sequential continuous refiller, 250ms failure backoff,
// FIFO ready channel and one-second scanner. Checkout does not wait on opens.
func (p *modelPool) refill() {
	if p.closed || p.constructing > 0 || len(p.ready) >= p.cfg.TargetReady {
		return
	}
	p.create(func(c *physicalConn, code string) {
		if p.closed {
			return
		}
		if code != "" {
			p.run().sim.After(FailureBackoff, "reservoir-retry", p.refill)
			return
		}
		p.ready = append(p.ready, c)
		p.refill()
	})
}

func (p *modelPool) scan() {
	if p.closed {
		return
	}
	keep := p.ready[:0]
	for _, c := range p.ready {
		if !c.alive || p.guarded(c) {
			p.destroy(c, c.alive)
		} else {
			keep = append(keep, c)
		}
	}
	p.ready = keep
	p.refill()
	p.run().sim.After(time.Second, "reservoir-scan", p.scan)
}

func (p *modelPool) destroy(c *physicalConn, lifetime bool) {
	delete(p.owned, c.id)
	if c.borrowed {
		p.borrowed--
		c.borrowed = false
	}
	p.run().cluster.close(c)
	if lifetime {
		p.lifetimeDebt++
	}
}

func (p *modelPool) acquire(r *modelRequest) {
	if r.done || r.acquired {
		return
	}
	if p.closed {
		r.finish("closed")
		return
	}
	if p.isPGX() {
		p.acquirePGX(r)
		return
	}
	if p.model == "reservoir-sql" {
		for len(p.idle) > 0 {
			n := len(p.idle) - 1
			c := p.idle[n]
			p.idle = p.idle[:n]
			if p.cfg.EnforceLifetimeInUse && p.guarded(c) {
				p.destroy(c, true)
				continue
			}
			r.use(c)
			return
		}
		if p.borrowed >= p.cfg.MaxOpen {
			p.wait(r)
			return
		}
	}
	for len(p.ready) > 0 {
		c := p.ready[0]
		p.ready[0] = nil
		p.ready = p.ready[1:]
		if !c.alive || p.guarded(c) {
			p.destroy(c, c.alive)
			continue
		}
		if p.model == "reservoir-sql" {
			c.borrowed = true
			p.borrowed++
		}
		r.use(c)
		p.refill()
		return
	}
	p.refill()
	r.finish("empty")
}

// puddle.Pool.acquire / tryAcquireIdleResource use GenStack.Pop: LIFO between
// health sweeps. pgxpool.Pool.Acquire pings after >1s idle, then runs BeforeAcquire.
func (p *modelPool) acquirePGX(r *modelRequest) {
	for len(p.idle) > 0 {
		n := len(p.idle) - 1
		c := p.idle[n]
		p.idle = p.idle[:n]
		if !c.alive && p.now().Sub(c.lastUsed) > time.Second {
			p.destroy(c, false)
			continue
		}
		// Proposed BeforeAcquire hook: reject the guard window and try again.
		if p.guarded(c) {
			p.destroy(c, true)
			continue
		}
		r.use(c)
		return
	}
	if p.total() >= p.limit() {
		p.wait(r)
		return
	}
	r.waitForNew()
	r.armTimeout()
	if p.pendingAcquires == nil {
		p.pendingAcquires = make(map[*modelRequest]struct{})
	}
	p.pendingAcquires[r] = struct{}{}
	p.create(func(c *physicalConn, code string) {
		delete(p.pendingAcquires, r)
		if code != "" {
			if !r.done {
				r.finish(code)
			}
			p.dispatch()
			return
		}
		// puddle.initResourceValue continues construction after caller cancellation.
		if r.done {
			p.idle = append(p.idle, c)
			p.dispatch()
			return
		}
		if p.guarded(c) {
			p.destroy(c, true)
			p.acquirePGX(r)
			return
		}
		r.use(c)
	})
}

func (p *modelPool) wait(r *modelRequest) {
	if p.isPGX() && p.constructing > 0 {
		r.waitForNew()
	}
	p.waiters = append(p.waiters, r)
	r.armTimeout()
}

func (p *modelPool) dispatch() {
	for !p.closed && len(p.waiters) > 0 {
		if p.isPGX() && len(p.idle) == 0 && p.total() >= p.limit() {
			return
		}
		if !p.isPGX() && len(p.idle) == 0 && p.borrowed >= p.limit() {
			return
		}
		r := p.waiters[0]
		p.waiters[0] = nil
		p.waiters = p.waiters[1:]
		p.acquire(r)
	}
}

// pgxpool.Conn.Release destroys expired and closed connections and triggers a
// health check after 500ms. It never terminates a query at the client lifetime.
func (p *modelPool) release(c *physicalConn) {
	if p.closed {
		delete(p.owned, c.id)
		if !p.leak() {
			p.run().cluster.close(c)
		}
		return
	}
	if p.isPGX() {
		expired := p.now().After(c.expires)
		if !c.alive || expired {
			p.destroy(c, expired)
			p.triggerHealth()
		} else {
			c.lastUsed = p.now()
			p.idle = append(p.idle, c)
		}
	} else if p.model == "reservoir-sql" {
		expired := p.cfg.EnforceLifetimeInUse && p.guarded(c)
		if !c.alive || expired {
			p.destroy(c, expired)
		} else {
			c.lastUsed = p.now()
			p.idle = append(p.idle, c)
		}
	} else {
		expired := p.guarded(c)
		if !c.alive || expired || len(p.ready) >= p.cfg.TargetReady {
			p.destroy(c, expired)
		} else {
			c.lastUsed = p.now()
			p.ready = append(p.ready, c)
		}
		p.refill()
	}
	p.dispatch()
}

func (p *modelPool) healthTick() {
	if p.closed {
		return
	}
	p.health()
	p.run().sim.After(p.cfg.PGX.HealthCheckPeriod, "pgx-health-ticker", p.healthTick)
}

func (p *modelPool) triggerHealth() {
	if p.closed || p.healthPending {
		return
	}
	p.healthPending = true
	p.run().sim.After(500*time.Millisecond, "pgx-trigger-health", func() { p.healthPending = false; p.health() })
}

// pgxpool.checkHealth calls checkMinConns before checkConnsHealth, awaits all
// createIdleResources constructors, and skips destruction on a creation error.
func (p *modelPool) health() {
	if p.closed || p.healthRunning {
		return
	}
	p.healthRunning = true
	need := p.cfg.PGX.MinConns - p.total()
	if need <= 0 {
		p.sweep()
		return
	}
	pending := need
	failed := false
	for range need {
		p.createWithCancel(func() bool { return failed }, func(c *physicalConn, code string) {
			if code != "" {
				failed = true
			} else {
				p.idle = append(p.idle, c)
			}
			pending--
			if pending == 0 {
				if failed || p.closed {
					p.finishHealth(false)
				} else {
					p.sweep()
				}
			}
			// puddle.CreateResource releases its semaphore slot per constructor,
			// so an acquire need not wait for the entire MinConns batch to finish.
			p.dispatch()
		})
	}
}

// pgxpool.checkConnsHealth + puddle.AcquireAllIdle / ReleaseUnused: sweep idle
// connections in pop order, preserve last-used times, reverse stack order on
// return. Single-threaded events mean no concurrently acquired old generation.
func (p *modelPool) sweep() {
	total := p.total()
	idle := p.idle
	p.idle = nil
	destroyed := false
	for j := len(idle) - 1; j >= 0; j-- {
		c := idle[j]
		expired := p.now().After(c.expires) && total >= p.cfg.PGX.MinConns
		tooIdle := p.cfg.PGX.MaxConnIdleTime > 0 && p.now().Sub(c.lastUsed) > p.cfg.PGX.MaxConnIdleTime && total > p.cfg.PGX.MinConns
		if expired || tooIdle {
			p.destroy(c, expired)
			total--
			destroyed = true
		} else {
			p.idle = append(p.idle, c)
		}
	}
	p.finishHealth(destroyed)
}

func (p *modelPool) finishHealth(destroyed bool) {
	p.healthRunning = false
	if p.closed {
		return
	}
	if !p.healthStarted {
		p.healthStarted = true
		p.run().sim.After(p.cfg.PGX.HealthCheckPeriod, "pgx-health-ticker", p.healthTick)
	}
	if destroyed {
		p.triggerHealth()
	}
}

// puddle.Close destroys idle connections immediately and acquired ones when
// returned. Reservoir-sql's leak toggle deliberately keeps server slots alive.
func (p *modelPool) close() {
	if p.closed {
		return
	}
	p.closed = true
	for _, c := range p.owned {
		if c.request == nil && !p.leak() {
			p.run().cluster.close(c)
			delete(p.owned, c.id)
		}
	}
	p.ready = nil
	p.idle = nil
	for _, r := range p.waiters {
		if !r.done {
			r.finish("closed")
		}
	}
	p.waiters = nil
	// puddle.Close cancels the constructor context. Acquired queries drain, but
	// callers waiting in initResourceValue return as soon as the pool closes.
	for r := range p.pendingAcquires {
		if !r.done {
			r.finish("closed")
		}
	}
}

type modelRequest struct {
	pool                             *modelPool
	start                            time.Time
	hold, maxWait                    time.Duration
	conn                             *physicalConn
	done, acquired, timed, waitedNew bool
	completed                        func()
}

func (p *modelPool) request(hold, maxWait time.Duration, completed func()) *modelRequest {
	r := &modelRequest{pool: p, start: p.now(), hold: hold, maxWait: maxWait, completed: completed}
	p.stats.requests++
	p.run().stats.requests++
	p.acquire(r)
	return r
}

func (r *modelRequest) waitForNew() {
	if r.waitedNew {
		return
	}
	r.waitedNew = true
	r.pool.stats.waitedNew++
	r.pool.run().stats.waitedNew++
}

func (r *modelRequest) armTimeout() {
	if r.timed {
		return
	}
	r.timed = true
	r.pool.run().sim.Schedule(r.start.Add(r.maxWait), "acquire-deadline", func() {
		if !r.done && !r.acquired {
			r.finish("timeout")
		}
	})
}

func (r *modelRequest) recordWait() {
	wait := r.pool.now().Sub(r.start)
	r.pool.run().stats.wait.add(wait)
	r.pool.stats.wait.add(wait)
}

func (r *modelRequest) use(c *physicalConn) {
	r.acquired = true
	r.conn = c
	c.request = r
	r.recordWait()
	if c.alive && r.pool.onFirstAcquire != nil {
		first := r.pool.onFirstAcquire
		r.pool.onFirstAcquire = nil
		first()
	}
	if !c.alive {
		r.finish("dead")
		return
	}
	r.pool.run().sim.After(r.hold, "query-release", func() {
		if !r.done {
			r.finish("")
		}
	})
}

func (r *modelRequest) finish(reason string) {
	if r.done {
		return
	}
	r.done = true
	p := r.pool
	if !r.acquired {
		r.recordWait()
	}
	if reason != "" {
		p.stats.failures[reason]++
		p.run().stats.failures[reason]++
	} else {
		p.stats.completed++
		p.run().stats.completed++
	}
	if r.conn != nil {
		r.conn.request = nil
		p.release(r.conn)
	}
	if reason == "dead" && p.reconnect() && !p.ephemeral {
		p.instance.rebuild()
	}
	if r.completed != nil {
		r.completed()
	}
}
