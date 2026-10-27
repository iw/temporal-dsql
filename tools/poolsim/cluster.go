package main

import (
	"math"
	"math/rand"
	"time"
)

const dsqlMaxAge = time.Hour

// tokenBucket uses elapsed simulated time, including fractional tokens. A failed
// take does not reserve tokens; callers serialize waiting acquisitions in FIFO order.
type tokenBucket struct {
	rate, capacity, tokens float64
	updated                time.Time
}

func newTokenBucket(now time.Time, rate, capacity float64) *tokenBucket {
	return &tokenBucket{rate: rate, capacity: capacity, tokens: capacity, updated: now}
}

func (b *tokenBucket) refill(now time.Time) {
	b.tokens = math.Min(b.capacity, b.tokens+now.Sub(b.updated).Seconds()*b.rate)
	b.updated = now
}

func (b *tokenBucket) take(now time.Time) (bool, time.Duration) {
	b.refill(now)
	if b.tokens >= 1-1e-9 {
		b.tokens = math.Max(0, b.tokens-1)
		return true, 0
	}
	return false, max(time.Nanosecond, time.Duration(math.Ceil((1-b.tokens)/b.rate*float64(time.Second))))
}

// feedback is AIMD per completed handshake, not per timer tick. Only 53400
// indicates congestion; 53300 must not masquerade as rate-limit feedback.
func (b *tokenBucket) feedback(now time.Time, code string, increase float64) {
	b.refill(now)
	switch code {
	case "":
		b.rate += increase
	case "53400":
		b.rate = math.Max(0.1, b.rate/2)
	default:
	}
}

type physicalConn struct {
	id                      int64
	pool                    *modelPool
	born, expires, lastUsed time.Time
	alive                   bool
	borrowed                bool // owned by database/sql rather than sitting in the reservoir
	request                 *modelRequest
}

type dsqlCluster struct {
	run        *comparisonRun
	cfg        ClusterConfig
	bucket     *tokenBucket
	rng        *rand.Rand
	nextID     int64
	conns      map[int64]*physicalConn
	open, peak int
}

func randomLatency(rng *rand.Rand, span LatencyRange) time.Duration {
	if span.Max <= span.Min {
		return span.Min
	}
	return span.Min + time.Duration(rng.Int63n(int64(span.Max-span.Min)+1))
}

// dial admits at dial start (reserving a cap slot during TLS), and reports both
// success and rejection only after the configured handshake latency.
func (c *dsqlCluster) dial(p *modelPool, done func(*physicalConn, string)) {
	now := c.run.sim.Now()
	code := ""
	ok, _ := c.bucket.take(now)
	if !ok {
		code = "53400"
	} else if c.open >= c.cfg.MaxConnections {
		code = "53300"
	}
	c.run.stats.attempts[int(now.Sub(c.run.start)/time.Second)]++
	var conn *physicalConn
	if code == "" {
		c.nextID++
		conn = &physicalConn{id: c.nextID, pool: p, born: now, alive: true}
		c.conns[conn.id] = conn
		c.open++
		c.peak = max(c.peak, c.open)
		p.physical++
		p.stats.peak = max(p.stats.peak, p.physical)

		c.run.sim.Schedule(conn.born.Add(dsqlMaxAge), "dsql-age-cut", func() {
			if !conn.alive {
				return
			}
			if conn.request == nil {
				c.run.stats.cutIdle++
				p.stats.cutIdle++
			} else {
				c.run.stats.cutInUse++
				p.stats.cutInUse++
			}
			c.drop(conn)
		})
	} else {
		c.run.stats.rejections[code]++
		p.stats.rejections[code]++
	}
	c.run.sim.After(randomLatency(c.rng, c.cfg.Handshake), "handshake", func() {
		if conn != nil && !conn.alive {
			done(nil, "dead")
			return
		}
		if conn != nil {
			c.run.stats.opens[int(c.run.sim.Now().Sub(c.run.start)/time.Second)]++
			p.stats.opens++
			conn.expires = c.run.sim.Now().Add(p.lifetime())
			conn.lastUsed = c.run.sim.Now()
		}
		done(conn, code)
	})
}

func (c *dsqlCluster) close(conn *physicalConn) {
	if !conn.alive {
		return
	}
	conn.alive = false
	delete(c.conns, conn.id)
	c.open--
	conn.pool.physical--
	c.run.stats.closures[int(c.run.sim.Now().Sub(c.run.start)/time.Second)]++
}

func (c *dsqlCluster) drop(conn *physicalConn) {
	c.close(conn)
	if conn.request != nil {
		conn.request.finish("dead")
	}
}
