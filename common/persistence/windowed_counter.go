package persistence

import (
	"sync"
	"time"
)

// windowedCounter counts requests and unhealthy errors in fixed time windows
// aligned with the latency t-digest windows, so error ratio and throughput can
// be read over exactly the same span as the latency quantile.
type windowedCounter struct {
	mu      sync.Mutex
	size    time.Duration
	buckets []counterBucket // ring, indexed by window number modulo len
}

type counterBucket struct {
	window        int64 // absolute window number (unix nanos / size)
	total, errors uint64
}

func newWindowedCounter(size time.Duration, count int) *windowedCounter {
	return &windowedCounter{size: size, buckets: make([]counterBucket, count)}
}

func (c *windowedCounter) record(now time.Time, unhealthy bool) {
	w := now.UnixNano() / int64(c.size)
	c.mu.Lock()
	defer c.mu.Unlock()
	b := &c.buckets[w%int64(len(c.buckets))]
	if b.window != w {
		*b = counterBucket{window: w}
	}
	b.total++
	if unhealthy {
		b.errors++
	}
}

// sum returns totals over the newest n windows (the current one is partial)
// and the wall-clock span they cover.
func (c *windowedCounter) sum(now time.Time, n int) (total, errors uint64, span time.Duration) {
	n = min(max(n, 1), len(c.buckets))
	cur := now.UnixNano() / int64(c.size)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range int64(n) {
		w := cur - k
		if b := c.buckets[w%int64(len(c.buckets))]; b.window == w {
			total += b.total
			errors += b.errors
		}
	}
	span = time.Duration(n-1)*c.size + time.Duration(now.UnixNano()-cur*int64(c.size))
	return total, errors, span
}
