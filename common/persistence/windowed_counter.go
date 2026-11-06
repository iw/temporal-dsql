package persistence

import (
	"sync"
	"time"
)

// windowedCounter counts requests, unhealthy errors, the persistence rate limiter's
// rejections, store facts, task attempts and the task scheduler's coupled refusals in
// fixed time windows, so error ratio, throughput, facts, attempts per call and refusals
// are all read over exactly the same span. The latency t-digests start a window at its
// first sample, so their quantiles cover about that span.
type windowedCounter struct {
	mu      sync.Mutex
	size    time.Duration
	buckets []counterBucket // ring, indexed by window number modulo len
}

type counterBucket struct {
	window int64 // absolute window number (unix nanos / size)
	windowCounts
}

// windowCounts are the counts in one window, or summed over several. headroom is the
// Headroom of the newest refusal.
type windowCounts struct {
	total, errors, rejected, attempts, refusals uint64
	facts                                       [numStoreFacts]uint64
	headroom                                    float64
}

func newWindowedCounter(size time.Duration, count int) *windowedCounter {
	return &windowedCounter{size: size, buckets: make([]counterBucket, count)}
}

func (c *windowedCounter) record(now time.Time, unhealthy bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.bucket(now)
	b.total++
	if unhealthy {
		b.errors++
	}
}

func (c *windowedCounter) recordRejected(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bucket(now).rejected++
}

func (c *windowedCounter) recordFact(now time.Time, fact StoreFact, count uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bucket(now).facts[fact] += count
}

func (c *windowedCounter) recordAttempt(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bucket(now).attempts++
}

func (c *windowedCounter) recordRefusal(now time.Time, headroom float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.bucket(now)
	b.refusals++
	b.headroom = headroom
}

// bucket returns now's bucket, cleared if it still holds an older window. Callers hold mu.
func (c *windowedCounter) bucket(now time.Time) *counterBucket {
	w := now.UnixNano() / int64(c.size)
	b := &c.buckets[w%int64(len(c.buckets))]
	if b.window != w {
		*b = counterBucket{window: w}
	}
	return b
}

// sum returns counts over the newest n windows (the current one is partial)
// and the wall-clock span they cover.
func (c *windowedCounter) sum(now time.Time, n int) (counts windowCounts, span time.Duration) {
	n = min(max(n, 1), len(c.buckets))
	cur := now.UnixNano() / int64(c.size)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range int64(n) {
		w := cur - k
		if b := c.buckets[w%int64(len(c.buckets))]; b.window == w {
			counts.total += b.total
			counts.errors += b.errors
			counts.rejected += b.rejected
			counts.attempts += b.attempts
			counts.refusals += b.refusals
			if counts.headroom == 0 && b.refusals > 0 {
				counts.headroom = b.headroom // the newest window's
			}
			for fact, count := range b.facts {
				counts.facts[fact] += count
			}
		}
	}
	span = time.Duration(n-1)*c.size + time.Duration(now.UnixNano()-cur*int64(c.size))
	return counts, span
}
