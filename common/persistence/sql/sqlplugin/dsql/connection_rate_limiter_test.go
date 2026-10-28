package dsql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConnectionRateLimiterDefaults(t *testing.T) {

	limiter := newConnectionRateLimiter(testSettings(t, nil))
	require.NotNil(t, limiter)
	require.NotNil(t, limiter.limiter)
	require.InDelta(t, DefaultConnectionRateLimit, float64(limiter.limiter.Limit()), 0.001)
	require.Equal(t, DefaultConnectionBurstLimit, limiter.limiter.Burst())

	// Should allow burst of connections
	for i := 0; i < DefaultConnectionBurstLimit; i++ {
		require.True(t, limiter.Allow(), "should allow connection %d within burst", i)
	}
}

func TestConnectionRateLimiterAttributes(t *testing.T) {
	limiter := newConnectionRateLimiter(testSettings(t, map[string]string{"connection_rate_limit": "5", "connection_burst_limit": "10"}))
	require.NotNil(t, limiter)

	require.InDelta(t, 5, float64(limiter.limiter.Limit()), 0.001)
	require.Equal(t, 10, limiter.limiter.Burst())

	// Should allow burst of 10
	for i := 0; i < 10; i++ {
		require.True(t, limiter.Allow(), "should allow connection %d within burst", i)
	}

	// 11th should be rate limited
	require.False(t, limiter.Allow(), "should not allow connection beyond burst")
}

func TestConnectionRateLimiter_Wait(t *testing.T) {
	limiter := newConnectionRateLimiter(testSettings(t, map[string]string{"connection_rate_limit": "100", "connection_burst_limit": "5"}))

	// Exhaust burst
	for i := 0; i < 5; i++ {
		require.True(t, limiter.Allow())
	}

	// Wait should succeed (rate is 100/sec, so should be quick)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := limiter.Wait(ctx)
	require.NoError(t, err)
}

func TestConnectionRateLimiter_WaitTimeout(t *testing.T) {
	limiter := newConnectionRateLimiter(testSettings(t, map[string]string{"connection_rate_limit": "1", "connection_burst_limit": "1"}))

	// Exhaust burst
	require.True(t, limiter.Allow())

	// Wait with very short timeout should fail
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Millisecond)
	defer cancel()

	err := limiter.Wait(ctx)
	require.Error(t, err)
}
