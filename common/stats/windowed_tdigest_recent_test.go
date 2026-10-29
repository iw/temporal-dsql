package stats

import (
	"testing"
	"time"
)

func TestQuantileRecentIgnoresOldWindows(t *testing.T) {
	s, err := NewWindowedTDigest(WindowConfig{WindowSize: time.Second, WindowCount: 10})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)
	for i := range 1000 { // windows 0..4: slow (500ms)
		s.Record(500, base.Add(time.Duration(i)*5*time.Millisecond))
	}
	for i := range 1000 { // windows 5..7: fast (10ms)
		s.Record(10, base.Add(5*time.Second+time.Duration(i)*3*time.Millisecond))
	}
	now := base.Add(8 * time.Second)
	rq := s.(RecentQuantiler)
	if p99, n := rq.QuantileRecent(0.99, 3, now); p99 > 20 || n != 1000 {
		t.Fatalf("recent 3 windows should only see the fast data: p99=%v n=%v", p99, n)
	}
	if all := s.Quantile(0.99); all < 400 {
		t.Fatalf("Quantile over all windows should still see slow data, got %v", all)
	}
	if _, n := rq.QuantileRecent(0.99, 3, base.Add(60*time.Second)); n != 0 {
		t.Fatalf("stale windows must not count, got n=%v", n)
	}
}
