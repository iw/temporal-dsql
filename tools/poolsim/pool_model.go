package main

import (
	"math/rand"
	"time"
)

type modelPool struct {
	pendingAcquires              map[*modelRequest]struct{}
	healthStarted                bool
	finalSampled                 bool
	onFirstAcquire               func()
	instance                     *instance
	name                         string
	cfg                          ServiceConfig
	rng                          *rand.Rand
	stats                        *poolStats
	startedAt                    time.Time
	closed                       bool
	ephemeral                    bool
	idle                         []*physicalConn
	owned                        map[int64]*physicalConn
	constructing, physical       int
	waiters                      []*modelRequest
	healthRunning, healthPending bool
	lifetimeDebt                 int
}

func (p *modelPool) run() *simulationRun { return p.instance.run }
func (p *modelPool) now() time.Time      { return p.run().sim.Now() }

func (p *modelPool) total() int  { return len(p.owned) + p.constructing }
func (p *modelPool) limit() int  { return p.cfg.PGX.MaxConns }
func (p *modelPool) target() int { return p.cfg.PGX.MinConns }

// pgxpool.NewWithConfig's constructor adds nonnegative jitter, truncated to
// whole seconds, to MaxConnLifetime after ConnectConfig completes.
func (p *modelPool) lifetime() time.Duration {
	jitter := time.Duration(p.rng.Float64()*p.cfg.PGX.MaxConnLifetimeJitter.Seconds()) * time.Second
	return p.cfg.PGX.MaxConnLifetime + jitter
}

func (p *modelPool) start() {
	p.startedAt = p.now()
	// Constructors count against puddle MaxSize during the asynchronous fill.
	p.health()
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
				// Close connections whose constructor completes after shutdown.
				p.run().cluster.close(c)
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

func (p *modelPool) destroy(c *physicalConn, lifetime bool) {
	delete(p.owned, c.id)
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
	p.acquirePGX(r)
}

// puddle.Pool.acquire / tryAcquireIdleResource use GenStack.Pop: LIFO between
// health sweeps. pgxpool.Pool.Acquire pings after >1s idle, then hands out the connection.
func (p *modelPool) acquirePGX(r *modelRequest) {
	for len(p.idle) > 0 {
		n := len(p.idle) - 1
		c := p.idle[n]
		p.idle = p.idle[:n]
		if !c.alive && p.now().Sub(c.lastUsed) > time.Second {
			p.destroy(c, false)
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
		r.use(c)
	})
}

func (p *modelPool) wait(r *modelRequest) {
	if p.constructing > 0 {
		r.waitForNew()
	}
	p.waiters = append(p.waiters, r)
	r.armTimeout()
}

func (p *modelPool) dispatch() {
	for !p.closed && len(p.waiters) > 0 {
		if len(p.idle) == 0 && p.total() >= p.limit() {
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
		p.run().cluster.close(c)
		return
	}
	expired := p.now().After(c.expires)
	if !c.alive || expired {
		p.destroy(c, expired)
		p.triggerHealth()
	} else {
		c.lastUsed = p.now()
		p.idle = append(p.idle, c)
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
// return. Single-threaded events mean no concurrent acquisition during the sweep.
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
// returned.
func (p *modelPool) close() {
	if p.closed {
		return
	}
	p.closed = true
	for _, c := range p.owned {
		if c.request == nil {
			p.run().cluster.close(c)
			delete(p.owned, c.id)
		}
	}
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
	phase                            string
	phaseStats                       *poolStats
	pool                             *modelPool
	start                            time.Time
	hold, maxWait                    time.Duration
	conn                             *physicalConn
	done, acquired, timed, waitedNew bool
	completed                        func()
}

func (p *modelPool) request(hold, maxWait time.Duration, completed func()) *modelRequest {
	return p.requestInPhase(hold, maxWait, "", completed)
}

func (p *modelPool) requestInPhase(hold, maxWait time.Duration, phase string, completed func()) *modelRequest {
	r := &modelRequest{pool: p, start: p.now(), hold: hold, maxWait: maxWait, completed: completed, phase: phase, phaseStats: p.run().stats.phases[phase]}
	if r.phaseStats != nil {
		r.phaseStats.requests++
	}
	if !p.ephemeral && p.stats.requests == 0 && !p.startedAt.IsZero() {
		p.run().stats.startDelay.add(p.now().Sub(p.startedAt))
	}
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
	if r.phaseStats != nil {
		r.phaseStats.waitedNew++
	}
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
	if r.phaseStats != nil {
		r.phaseStats.wait.add(wait)
	}
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
		if r.phaseStats != nil {
			r.phaseStats.failures[reason]++
		}
	} else {
		p.stats.completed++
		p.run().stats.completed++
		if r.phaseStats != nil {
			r.phaseStats.completed++
		}
	}
	if r.conn != nil {
		r.conn.request = nil
		p.release(r.conn)
	}
	if r.completed != nil {
		r.completed()
	}
}
