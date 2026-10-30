package persistence

import (
	"sync"
	"time"
)

// windowedCounter counts requests, unhealthy errors, the persistence rate limiter's
// rejections and store facts in fixed time windows aligned with the latency t-digest
// windows, so error ratio, throughput and facts can be read over exactly the same span
// as the latency quantile.
type windowedCounter struct {
	mu      sync.Mutex
	size    time.Duration
	buckets []counterBucket // ring, indexed by window number modulo len
}

type counterBucket struct {
	window int64 // absolute window number (unix nanos / size)
	windowCounts
}

// windowCounts are the counts in one window, or summed over several.
type windowCounts struct {
	total, errors, rejected uint64
	facts                   [numStoreFacts]uint64
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
			for fact, count := range b.facts {
				counts.facts[fact] += count
			}
		}
	}
	span = time.Duration(n-1)*c.size + time.Duration(now.UnixNano()-cur*int64(c.size))
	return counts, span
}
