package main

import "time"

// RateLimiter models DSQL's cluster-wide connection rate limit.
// DSQL allows 100 new connections per second across the entire cluster.
type RateLimiter interface {
	// Acquire attempts to acquire n connection permits.
	// Returns (true, 0) if successful, (false, retryAfter) if denied.
	Acquire(now time.Time, endpoint string, n int) (ok bool, retryAfter time.Duration)

	// ConnectsThisSecond returns the number of connections made this second.
	ConnectsThisSecond(now time.Time, endpoint string) int
}

// PerSecondLimiter implements a per-second rate limiter.
// This models DSQL's cluster-wide rate limit of 100 connections/second.
type PerSecondLimiter struct {
	LimitPerSec int
	used        map[string]map[int64]int // endpoint -> unix_second -> count
}

// NewPerSecondLimiter creates a new rate limiter with the given limit.
func NewPerSecondLimiter(limit int) *PerSecondLimiter {
	return &PerSecondLimiter{
		LimitPerSec: limit,
		used:        make(map[string]map[int64]int),
	}
}

// Acquire attempts to acquire n connection permits.
func (l *PerSecondLimiter) Acquire(now time.Time, endpoint string, n int) (bool, time.Duration) {
	sec := now.Unix()

	m := l.used[endpoint]
	if m == nil {
		m = make(map[int64]int)
		l.used[endpoint] = m
	}

	if m[sec]+n <= l.LimitPerSec {
		m[sec] += n
		return true, 0
	}

	// Rate limited - retry after next second boundary plus small buffer
	next := time.Unix(sec+1, 0).Sub(now) + 5*time.Millisecond
	return false, next
}

// ConnectsThisSecond returns connections made this second for an endpoint.
func (l *PerSecondLimiter) ConnectsThisSecond(now time.Time, endpoint string) int {
	sec := now.Unix()
	m := l.used[endpoint]
	if m == nil {
		return 0
	}
	return m[sec]
}

// GlobalConnectsPerSecond returns all per-second connection counts.
// Used for assertions at the end of simulation.
func (l *PerSecondLimiter) GlobalConnectsPerSecond() map[int64]int {
	result := make(map[int64]int)
	for _, m := range l.used {
		for sec, count := range m {
			result[sec] += count
		}
	}
	return result
}
