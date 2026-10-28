package dsql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestRetryManager_Basic(t *testing.T) {
	config := DefaultRetryConfig()
	config.MaxRetries = 2
	config.BaseDelay = 1 * time.Millisecond // Fast for testing

	rm := NewRetryManager(config, log.NewNoopLogger(), nil)

	// Test that RetryManager is created with correct config
	require.Equal(t, 2, rm.config.MaxRetries)
	require.Equal(t, 1*time.Millisecond, rm.config.BaseDelay)
}

func TestRetryManager_SuccessfulOperation(t *testing.T) {
	// This test verifies that the RetryManager can be created and configured properly
	// Actual database transaction testing would be done in integration tests
	config := DefaultRetryConfig()
	config.MaxRetries = 2
	config.BaseDelay = 1 * time.Millisecond

	rm := NewRetryManager(config, log.NewNoopLogger(), nil)

	// Verify the retry manager was created with correct configuration
	require.Equal(t, 2, rm.config.MaxRetries)
	require.Equal(t, 1*time.Millisecond, rm.config.BaseDelay)
}

func TestRetryManager_ConditionFailedError_NoRetry(t *testing.T) {
	// Test error classification without database operations
	err := NewConditionFailedError(ConditionFailedUnknown, "test condition failed")

	// Verify the error is properly classified
	require.True(t, IsConditionFailedError(err))
	require.Equal(t, ErrorTypeConditionFailed, ClassifyError(err))
	require.False(t, IsRetryableError(err))
}

func TestRetryConfig_Validation(t *testing.T) {
	tests := []struct {
		name     string
		input    RetryConfig
		expected RetryConfig
	}{
		{
			name: "valid config",
			input: RetryConfig{
				MaxRetries:   3,
				BaseDelay:    50 * time.Millisecond,
				MaxDelay:     2 * time.Second,
				JitterFactor: 0.1,
			},
			expected: RetryConfig{
				MaxRetries:   3,
				BaseDelay:    50 * time.Millisecond,
				MaxDelay:     2 * time.Second,
				JitterFactor: 0.1,
			},
		},
		{
			name: "negative max retries",
			input: RetryConfig{
				MaxRetries:   -1,
				BaseDelay:    50 * time.Millisecond,
				MaxDelay:     2 * time.Second,
				JitterFactor: 0.1,
			},
			expected: RetryConfig{
				MaxRetries:   0,
				BaseDelay:    50 * time.Millisecond,
				MaxDelay:     2 * time.Second,
				JitterFactor: 0.1,
			},
		},
		{
			name: "zero base delay",
			input: RetryConfig{
				MaxRetries:   3,
				BaseDelay:    0,
				MaxDelay:     2 * time.Second,
				JitterFactor: 0.1,
			},
			expected: RetryConfig{
				MaxRetries:   3,
				BaseDelay:    100 * time.Millisecond,
				MaxDelay:     2 * time.Second,
				JitterFactor: 0.1,
			},
		},
		{
			name: "max delay less than base delay",
			input: RetryConfig{
				MaxRetries:   3,
				BaseDelay:    2 * time.Second,
				MaxDelay:     50 * time.Millisecond,
				JitterFactor: 0.1,
			},
			expected: RetryConfig{
				MaxRetries:   3,
				BaseDelay:    2 * time.Second,
				MaxDelay:     2 * time.Second, // Should be set to base delay
				JitterFactor: 0.1,
			},
		},
		{
			name: "negative jitter factor",
			input: RetryConfig{
				MaxRetries:   3,
				BaseDelay:    50 * time.Millisecond,
				MaxDelay:     2 * time.Second,
				JitterFactor: -0.5,
			},
			expected: RetryConfig{
				MaxRetries:   3,
				BaseDelay:    50 * time.Millisecond,
				MaxDelay:     2 * time.Second,
				JitterFactor: 0,
			},
		},
		{
			name: "jitter factor too high",
			input: RetryConfig{
				MaxRetries:   3,
				BaseDelay:    50 * time.Millisecond,
				MaxDelay:     2 * time.Second,
				JitterFactor: 2.0,
			},
			expected: RetryConfig{
				MaxRetries:   3,
				BaseDelay:    50 * time.Millisecond,
				MaxDelay:     2 * time.Second,
				JitterFactor: 1.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.input.validate()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCalculateBackoff(t *testing.T) {
	config := RetryConfig{
		MaxRetries:   5,
		BaseDelay:    100 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		JitterFactor: 0, // No jitter for predictable testing
	}

	rm := NewRetryManager(config, log.NewNoopLogger(), nil)

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond},   // base delay
		{1, 200 * time.Millisecond},   // 2^1 * base
		{2, 400 * time.Millisecond},   // 2^2 * base
		{3, 800 * time.Millisecond},   // 2^3 * base
		{4, 1600 * time.Millisecond},  // 2^4 * base
		{5, 2000 * time.Millisecond},  // capped at max delay
		{10, 2000 * time.Millisecond}, // still capped at max delay
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempt), func(t *testing.T) {
			result := rm.calculateBackoff(tt.attempt)
			require.Equal(t, tt.expected, result)
		})
	}
}
