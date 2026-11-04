package adaptivesim

import (
	"container/list"
	"time"

	"go.temporal.io/server/common/adaptive/memory"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/signals"
)

type (
	// workflowCache is history's host-level workflow cache: each workflow's mutable state,
	// of entryBytes, least recently used out. Limited by size, as with
	// history.cacheSizeBasedLimit, it evicts the least recently used entries past its
	// size; with no size, it keeps every entry.
	workflowCache struct {
		maxBytes, entryBytes int
		entries              map[int64]*list.Element
		order                *list.List // most recently used first
		stats                CacheStats
	}

	// CacheStats counts the workflow cache's lookups and evictions.
	CacheStats struct {
		Hits, Misses, Evictions int
		// PeakBytes is the most the cache held, and MaxBytes its size limit at the end.
		PeakBytes, MaxBytes int
	}

	// processHeap is the history process's heap, as the runtime reports it against
	// GOMEMLIMIT: the rest of the process, the workflow cache and the tasks the queues
	// hold. The heap goal is twice the live heap, at the default GOGC, held under the
	// limit.
	processHeap struct {
		cfg     Memory
		cache   *workflowCache
		history *history
		last    time.Time
		stats   MemoryStats
	}

	// MemoryStats is the host's heap, as the plane read it each second.
	MemoryStats struct {
		// PeakLive is the most the live heap reached, in bytes.
		PeakLive int
		// Pressed is how long the heap's pressure on the limit passed the memory budget's
		// HighWater, and OverLimit how long the live heap passed the limit itself.
		Pressed, OverLimit time.Duration
		// PeakLoaded is the most tasks the queues held at once, and PeakStored the most
		// they left in the store because their shards' queues were at their limits.
		PeakLoaded, PeakStored int
	}
)

var _ cache.Resizable = (*workflowCache)(nil)

func newWorkflowCache(maxBytes, entryBytes int) *workflowCache {
	return &workflowCache{
		maxBytes:   maxBytes,
		entryBytes: entryBytes,
		entries:    make(map[int64]*list.Element),
		order:      list.New(),
		stats:      CacheStats{MaxBytes: maxBytes},
	}
}

// get reports whether a workflow's state is cached, and counts the lookup.
func (c *workflowCache) get(wf int64) bool {
	e, ok := c.entries[wf]
	if !ok {
		c.stats.Misses++
		return false
	}
	c.stats.Hits++
	c.order.MoveToFront(e)
	return true
}

// put caches a workflow's state.
func (c *workflowCache) put(wf int64) {
	if e, ok := c.entries[wf]; ok {
		c.order.MoveToFront(e)
		return
	}
	c.entries[wf] = c.order.PushFront(wf)
	c.evict()
	c.stats.PeakBytes = max(c.stats.PeakBytes, c.bytes())
}

// remove drops a workflow's state, as a failed attempt clears it.
func (c *workflowCache) remove(wf int64) {
	if e, ok := c.entries[wf]; ok {
		c.order.Remove(e)
		delete(c.entries, wf)
	}
}

// SetMaxSize is the cache's resize, as the memory budget drives it.
func (c *workflowCache) SetMaxSize(maxBytes int) {
	c.maxBytes = maxBytes
	c.stats.MaxBytes = maxBytes
	c.evict()
}

func (c *workflowCache) evict() {
	for c.maxBytes > 0 && c.bytes() > c.maxBytes && c.order.Len() > 0 {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(int64))
		c.stats.Evictions++
	}
}

func (c *workflowCache) bytes() int {
	return c.order.Len() * c.entryBytes
}

// live is the live heap at now.
func (m *processHeap) live(now time.Time) int {
	live := m.cfg.Base + m.cache.bytes() + m.history.loaded*m.cfg.TaskBytes
	if m.cfg.Extra.holds(now) {
		live += m.cfg.Extra.Bytes
	}
	return live
}

// read is the runtime's and the cgroup's signals at now, as the plane reads them.
func (m *processHeap) read(now time.Time) (signals.Runtime, signals.Memory) {
	live := m.live(now)
	goal := 2 * live
	if m.cfg.Limit > 0 {
		goal = max(live, min(goal, m.cfg.Limit))
	}
	m.record(now, live, goal)
	return signals.Runtime{HeapLive: uint64(live), HeapGoal: uint64(goal)},
		signals.Memory{GoLimit: uint64(m.cfg.Limit)}
}

func (m *processHeap) record(now time.Time, live, goal int) {
	stats := &m.stats
	stats.PeakLive = max(stats.PeakLive, live)
	stats.PeakLoaded = max(stats.PeakLoaded, m.history.loaded)
	stats.PeakStored = max(stats.PeakStored, m.history.stored)
	if !m.last.IsZero() && m.cfg.Limit > 0 {
		elapsed := now.Sub(m.last)
		if float64(live)/float64(goal) > memory.DefaultSettings.HighWater {
			stats.Pressed += elapsed
		}
		if live > m.cfg.Limit {
			stats.OverLimit += elapsed
		}
	}
	m.last = now
}

// holds reports whether the extra heap is live at now.
func (e Extra) holds(now time.Time) bool {
	at := now.Sub(epoch)
	return e.Bytes > 0 && at >= e.At && at < e.At+e.Duration
}
