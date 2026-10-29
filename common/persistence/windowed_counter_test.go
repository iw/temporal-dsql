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
	total, errs, span := c.sum(now, 2)
	if total != 14 || errs != 4 || span != 1500*time.Millisecond {
		t.Fatalf("got total=%d errs=%d span=%v", total, errs, span)
	}
	if total, _, _ = c.sum(now, 1); total != 4 {
		t.Fatalf("newest window only: got %d", total)
	}
	// Windows that have rotated out of the ring are not counted.
	if total, _, _ = c.sum(base.Add(10*time.Second), 5); total != 0 {
		t.Fatalf("stale buckets counted: %d", total)
	}
}
