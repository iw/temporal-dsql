package dsql

import (
	"context"

	"golang.org/x/time/rate"
)

const (
	// DefaultConnectionRateLimit is the default connections per second per instance.
	// DSQL cluster limit is 100/sec, so we default to 10/sec per instance
	// to allow ~10 instances to share the cluster safely.
	DefaultConnectionRateLimit = 10

	// DefaultConnectionBurstLimit is the default burst capacity per instance.
	// DSQL cluster burst is 1000, so we default to 100 per instance.
	DefaultConnectionBurstLimit = 100

	// ConnectionRateLimitEnvVar overrides the default rate limit.
	ConnectionRateLimitEnvVar = "DSQL_CONNECTION_RATE_LIMIT"

	// ConnectionBurstLimitEnvVar overrides the default burst limit.
	ConnectionBurstLimitEnvVar = "DSQL_CONNECTION_BURST_LIMIT"
)

// ConnectionRateLimiter limits the rate of new connection establishment
// to respect DSQL's cluster-wide connection rate limits.
type ConnectionRateLimiter struct {
	limiter *rate.Limiter
}

func newConnectionRateLimiter(resolved settings) *ConnectionRateLimiter {
	return &ConnectionRateLimiter{
		limiter: rate.NewLimiter(rate.Limit(resolved.Limiter.ConnectionRate), resolved.Limiter.ConnectionBurst),
	}
}

// Wait blocks until a connection can be established within rate limits.
// Returns an error if the context is cancelled.
func (r *ConnectionRateLimiter) Wait(ctx context.Context) error {
	return r.limiter.Wait(ctx)
}

// Allow reports whether a connection can be established now.
// Does not block or consume a token if not allowed.
func (r *ConnectionRateLimiter) Allow() bool {
	return r.limiter.Allow()
}
