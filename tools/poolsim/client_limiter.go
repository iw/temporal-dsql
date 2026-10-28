package main

import (
	"math/rand"
	"time"
)

type openJob struct {
	cancelled func() bool
	pool      *modelPool
	done      func(*physicalConn, string)
}

type instance struct {
	run                  *simulationRun
	name                 string
	cfg                  ServiceConfig
	main                 *modelPool
	ephemeral            []*modelPool
	generation           int
	closed               bool
	active, peakInflight int
	queue                []*openJob
	adaptive             *tokenBucket
}

// beforeConnect applies the shared instance semaphore to main and ephemeral
// constructors. Tokens are taken only after obtaining a slot so permits cannot
// accumulate behind a handshake queue and later burst past the server bucket.
func (i *instance) beforeConnect(p *modelPool, cancelled func() bool, done func(*physicalConn, string)) {
	i.queue = append(i.queue, &openJob{pool: p, cancelled: cancelled, done: done})
	i.pump()
}

func (i *instance) pump() {
	for !i.closed && i.active < i.cfg.InflightLimit && len(i.queue) > 0 {
		job := i.queue[0]
		i.queue[0] = nil
		i.queue = i.queue[1:]
		if job.pool.closed || (job.cancelled != nil && job.cancelled()) {
			job.done(nil, "closed")
			continue
		}
		i.active++
		i.peakInflight = max(i.peakInflight, i.active)
		i.run.limiter.acquire(i, func() {
			if job.pool.closed || i.closed || (job.cancelled != nil && job.cancelled()) {
				i.active--
				job.done(nil, "closed")
				i.pump()
				return
			}
			i.run.cluster.dial(job.pool, func(c *physicalConn, code string) {
				i.active--
				if i.run.limiter.mode == "adaptive" {
					i.adaptive.feedback(i.run.sim.Now(), code, i.run.cfg.Adaptive.Increase)
				}
				job.done(c, code)
				i.pump()
			})
		})
	}
}

type limiterWaiter struct {
	instance *instance
	done     func()
}

type clientLimiter struct {
	run       *simulationRun
	mode      string
	bucket    *tokenBucket
	perSecond *PerSecondLimiter
	rng       *rand.Rand
	waiting   []limiterWaiter
	scheduled bool
}

func (l *clientLimiter) acquire(i *instance, done func()) {
	if l.mode == "adaptive" {
		l.acquireAdaptive(i, done)
		return
	}
	l.waiting = append(l.waiting, limiterWaiter{i, done})
	l.pump()
}

// A single queue avoids O(pools^2) polling when the global bucket is exhausted.
// The DynamoDB latency is charged once per successful logical acquire, after
// its atomic token debit. Retries/internal conditional writes are abstracted.
func (l *clientLimiter) pump() {
	if l.scheduled || len(l.waiting) == 0 {
		return
	}
	if l.mode == "adaptive" {
		pending := l.waiting
		l.waiting = nil
		for _, w := range pending {
			l.acquireAdaptive(w.instance, w.done)
		}
		return
	}
	now := l.run.sim.Now()
	var ok bool
	var delay time.Duration
	if l.mode == "perSecond" {
		ok, delay = l.perSecond.Acquire(now, l.run.cfg.Endpoint, 1)
	} else {
		ok, delay = l.bucket.take(now)
	}
	if !ok {
		l.scheduled = true
		l.run.sim.After(delay, "client-token-wait", func() { l.scheduled = false; l.pump() })
		return
	}
	w := l.waiting[0]
	l.waiting[0] = limiterWaiter{}
	l.waiting = l.waiting[1:]
	// Independent RPC latency permits concurrent acquisitions, just like DDB.
	delay = 0
	if l.mode == "tokenBucket" {
		delay = randomLatency(l.rng, l.run.cfg.LimiterLatency)
	}
	l.run.sim.After(delay, "client-token", func() {
		if l.mode == "adaptive" {
			l.acquireAdaptive(w.instance, w.done)
		} else {
			w.done()
		}
	})
	l.pump()
}

func (l *clientLimiter) acquireAdaptive(i *instance, done func()) {
	if i.closed {
		done()
		return
	}
	ok, delay := i.adaptive.take(l.run.sim.Now())
	if ok {
		done()
		return
	}
	l.run.sim.After(delay, "adaptive-wait", func() { l.acquireAdaptive(i, done) })
}

func (l *clientLimiter) outage() {
	l.mode = "adaptive"
	// Each instance has its own full fallback bucket at switchover. Outstanding
	// global waits join that bucket rather than keeping stale DDB reservations.
	for _, i := range l.run.instances {
		i.adaptive = newTokenBucket(l.run.sim.Now(), l.run.cfg.Adaptive.InitialRate, l.run.cfg.Adaptive.Capacity)
	}
	l.scheduled = false
	l.pump()
}
