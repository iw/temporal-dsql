package fairshare

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/quotas"
)

type (
	// counters are the tokens each namespace's requests asked of a limit, by namespace. It's
	// safe for concurrent use.
	counters struct {
		m sync.Map // namespace → *atomic.Int64
	}

	sample struct {
		time  time.Time
		count int64
	}

	// countingLimiter counts each request's tokens before its limiter decides.
	countingLimiter struct {
		quotas.RequestRateLimiter
		exchange *Exchange
		class    *class
	}
)

func (c *counters) add(namespace string, tokens int) {
	v, ok := c.m.Load(namespace)
	if !ok {
		v, _ = c.m.LoadOrStore(namespace, new(atomic.Int64))
	}
	if count, ok := v.(*atomic.Int64); ok {
		count.Add(int64(tokens))
	}
}

func (c *counters) count(namespace string) (int64, bool) {
	v, ok := c.m.Load(namespace)
	if !ok {
		return 0, false
	}
	count, ok := v.(*atomic.Int64)
	if !ok {
		return 0, false
	}
	return count.Load(), true
}

func (c *counters) each(fn func(namespace string, count int64)) {
	c.m.Range(func(key, value any) bool {
		namespace, ok := key.(string)
		count, ok2 := value.(*atomic.Int64)
		if ok && ok2 {
			fn(namespace, count.Load())
		}
		return true
	})
}

func (c *counters) clear() {
	c.m.Clear()
}

func (l *countingLimiter) Allow(now time.Time, request quotas.Request) bool {
	l.exchange.count(l.class, request)
	return l.RequestRateLimiter.Allow(now, request)
}

func (l *countingLimiter) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	l.exchange.count(l.class, request)
	return l.RequestRateLimiter.Reserve(now, request)
}

func (l *countingLimiter) Wait(ctx context.Context, request quotas.Request) error {
	l.exchange.count(l.class, request)
	return l.RequestRateLimiter.Wait(ctx, request)
}

func (e *Exchange) count(c *class, request quotas.Request) {
	if request.Token > 0 && e.counting.Load() {
		c.counts.add(request.Caller, request.Token)
	}
}

// measure samples each namespace's count and returns the tokens a second each asked over
// the last window, by limit, leaving out the namespaces that asked none. It also returns,
// by limit, the namespaces whose window is still filling: those whose count started after
// an earlier round, so their rate counts from that round, before their first request, and
// rises until the round leaves the window.
func (e *Exchange) measure(now time.Time, window time.Duration) (own map[string]map[string]float64, filling map[string]map[string]bool) {
	own = make(map[string]map[string]float64, len(e.classes))
	filling = make(map[string]map[string]bool, len(e.classes))
	for _, c := range e.classes {
		rates := map[string]float64{}
		fills := map[string]bool{}
		c.counts.each(func(namespace string, count int64) {
			samples := c.samples[namespace]
			if len(samples) == 0 && !e.lastRound.IsZero() {
				// The namespace's count started after the last round.
				samples = append(samples, sample{time: e.lastRound})
				c.fillsFrom[namespace] = e.lastRound
			}
			if from, ok := c.fillsFrom[namespace]; ok {
				if now.Sub(from) <= window {
					fills[namespace] = true
				} else {
					delete(c.fillsFrom, namespace)
				}
			}
			samples = append(samples, sample{time: now, count: count})
			first := 0
			for first < len(samples)-1 && now.Sub(samples[first].time) > window {
				first++
			}
			samples = samples[first:]
			c.samples[namespace] = samples
			if span := now.Sub(samples[0].time).Seconds(); span > 0 {
				if rate := float64(count-samples[0].count) / span; rate > 0 {
					rates[namespace] = rate
				}
			}
		})
		own[c.name] = rates
		filling[c.name] = fills
	}
	e.lastRound = now
	return own, filling
}
