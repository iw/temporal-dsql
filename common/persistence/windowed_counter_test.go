package persistence

import (
	"testing"
	"time"
)

func TestWindowedCounter(t *testing.T) {
	c := newWindowedCounter(time.Second, 5)
	base := time.Unix(1_700_000_000, 0)
	for i := range 10 { // window 0: 10 ok
		c.record(base.Add(time.Duration(i)*time.Millisecond), false)
	}
	for i := range 4 { // window 1: 4 errors
		c.record(base.Add(time.Second+time.Duration(i)*time.Millisecond), true)
	}
	now := base.Add(1500 * time.Millisecond)
	counts, span := c.sum(now, 2)
	if counts.total != 14 || counts.errors != 4 || span != 1500*time.Millisecond {
		t.Fatalf("got total=%d errs=%d span=%v", counts.total, counts.errors, span)
	}
	if counts, _ = c.sum(now, 1); counts.total != 4 {
		t.Fatalf("newest window only: got %d", counts.total)
	}
	// Windows that have rotated out of the ring are not counted.
	if counts, _ = c.sum(base.Add(10*time.Second), 5); counts.total != 0 {
		t.Fatalf("stale buckets counted: %d", counts.total)
	}
}

func TestWindowedCounterFacts(t *testing.T) {
	c := newWindowedCounter(time.Second, 5)
	base := time.Unix(1_700_000_000, 0)
	c.recordFact(base, StoreFactThrottled, 3)                  // window 0
	c.recordFact(base.Add(time.Second), StoreFactThrottled, 2) // window 1
	c.recordFact(base.Add(time.Second), StoreFactQueued, 7)    // window 1
	c.record(base.Add(time.Second+time.Millisecond), false)    // window 1
	now := base.Add(1500 * time.Millisecond)

	counts, _ := c.sum(now, 2)
	if counts.facts[StoreFactThrottled] != 5 || counts.facts[StoreFactQueued] != 7 || counts.total != 1 {
		t.Fatalf("got facts=%v total=%d", counts.facts, counts.total)
	}
	if counts, _ = c.sum(now, 1); counts.facts[StoreFactThrottled] != 2 {
		t.Fatalf("newest window only: got %v", counts.facts)
	}
	if counts, _ = c.sum(base.Add(10*time.Second), 5); counts.facts != ([numStoreFacts]uint64{}) {
		t.Fatalf("stale facts counted: %v", counts.facts)
	}
}
