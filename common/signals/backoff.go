package signals

import (
	"time"

	"go.temporal.io/server/common/adaptive/capacity"
)

// Backoff is when a controller takes persistence to be backing off: while the capacity
// controller found the store overloaded in the last Hold, while errors, throttled calls and
// the persistence rate limiter's rejections pass MaxErrorRatio of at least MinSamples
// calls, or while more than MaxQueuedRatio of the store's connection acquires waited.
type Backoff struct {
	Hold           time.Duration
	MaxErrorRatio  float64
	MaxQueuedRatio float64
	MinSamples     int
}

// PersistenceBackingOff reads whether persistence is backing off from the snapshot: the
// capacity controller's latest decision, when it's recent, and persistence health.
func (s *Snapshot) PersistenceBackingOff(b Backoff) bool {
	if d, ok := s.Decisions[capacity.Name]; ok && s.Time.Sub(d.Time) <= b.Hold && capacity.Reason(d.Reason).Overloaded() {
		return true
	}
	h := s.Persistence
	if calls := h.Samples + h.Rejected; calls >= max(1, b.MinSamples) {
		failed := h.ErrorRatio*float64(h.Samples) + float64(h.Throttled+h.Rejected)
		if failed/float64(calls) > b.MaxErrorRatio {
			return true
		}
	}
	return h.Acquired > 0 && float64(h.Queued)/float64(h.Acquired) > b.MaxQueuedRatio
}
