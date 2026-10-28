package dsql

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewConnectionRateLimiter_Defaults(t *testing.T) {
	// Clear any env vars
	os.Unsetenv(ConnectionRateLimitEnvVar)
	os.Unsetenv(ConnectionBurstLimitEnvVar)

	limiter := NewConnectionRateLimiter()
	require.NotNil(t, limiter)
	require.NotNil(t, limiter.limiter)

	// Should allow burst of connections
	for i := 0; i < DefaultConnectionBurstLimit; i++ {
		require.True(t, limiter.Allow(), "should allow connection %d within burst", i)
	}
}

func TestNewConnectionRateLimiter_CustomEnvVars(t *testing.T) {
	// Set custom values
	os.Setenv(ConnectionRateLimitEnvVar, "5")
	os.Setenv(ConnectionBurstLimitEnvVar, "10")
	defer func() {
		os.Unsetenv(ConnectionRateLimitEnvVar)
		os.Unsetenv(ConnectionBurstLimitEnvVar)
	}()

	limiter := NewConnectionRateLimiter()
	require.NotNil(t, limiter)

	// Should allow burst of 10
	for i := 0; i < 10; i++ {
		require.True(t, limiter.Allow(), "should allow connection %d within burst", i)
	}

	// 11th should be rate limited
	require.False(t, limiter.Allow(), "should not allow connection beyond burst")
}

func TestConnectionRateLimiter_Wait(t *testing.T) {
	os.Setenv(ConnectionRateLimitEnvVar, "100")
	os.Setenv(ConnectionBurstLimitEnvVar, "5")
	defer func() {
		os.Unsetenv(ConnectionRateLimitEnvVar)
		os.Unsetenv(ConnectionBurstLimitEnvVar)
	}()

	limiter := NewConnectionRateLimiter()

	// Exhaust burst
	for i := 0; i < 5; i++ {
		require.True(t, limiter.Allow())
	}

	// Wait should succeed (rate is 100/sec, so should be quick)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := limiter.Wait(ctx)
	require.NoError(t, err)
}

func TestConnectionRateLimiter_WaitTimeout(t *testing.T) {
	os.Setenv(ConnectionRateLimitEnvVar, "1")
	os.Setenv(ConnectionBurstLimitEnvVar, "1")
	defer func() {
		os.Unsetenv(ConnectionRateLimitEnvVar)
		os.Unsetenv(ConnectionBurstLimitEnvVar)
	}()

	limiter := NewConnectionRateLimiter()

	// Exhaust burst
	require.True(t, limiter.Allow())

	// Wait with very short timeout should fail
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	err := limiter.Wait(ctx)
	require.Error(t, err)
}

func TestGetEnvInt(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		defaultVal int
		expected   int
	}{
		{"empty uses default", "", 42, 42},
		{"valid int", "100", 42, 100},
		{"invalid int uses default", "not-a-number", 42, 42},
		{"negative int", "-5", 42, -5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "TEST_ENV_INT"
			if tt.envValue != "" {
				os.Setenv(key, tt.envValue)
				defer os.Unsetenv(key)
			} else {
				os.Unsetenv(key)
			}

			result := getEnvInt(key, tt.defaultVal)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGetEnvDuration(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		defaultVal time.Duration
		expected   time.Duration
	}{
		{"empty uses default", "", 5 * time.Second, 5 * time.Second},
		{"valid duration", "10s", 5 * time.Second, 10 * time.Second},
		{"milliseconds", "500ms", 5 * time.Second, 500 * time.Millisecond},
		{"invalid duration uses default", "not-a-duration", 5 * time.Second, 5 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "TEST_ENV_DURATION"
			if tt.envValue != "" {
				os.Setenv(key, tt.envValue)
				defer os.Unsetenv(key)
			} else {
				os.Unsetenv(key)
			}

			result := getEnvDuration(key, tt.defaultVal)
			require.Equal(t, tt.expected, result)
		})
	}
}
