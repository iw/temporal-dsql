package dsql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

func TestRetryManagerCreationForTransactions(t *testing.T) {
	t.Run("retry manager logic works correctly", func(t *testing.T) {
		logger := log.NewNoopLogger()
		metricsHandler := metrics.NoopMetricsHandler
		config := RetryConfig{
			MaxRetries:   10,
			BaseDelay:    200,
			MaxDelay:     10000,
			JitterFactor: 0.5,
		}

		// Test 1: With logger and metrics - creates retry manager without a handle
		db1 := newDBWithDependencies(
			1, // dbKind
			"test",
			nil, // driver
			nil, // handle - not needed, since the retry manager never opens transactions
			nil, // tx
			logger,
			metricsHandler,
			config,
		)
		require.NotNil(t, db1.retryManager, "RetryManager should be created with logger and metrics")

		// Test 2: Without logger or metrics - should not create retry manager
		db2 := newDBWithDependencies(
			1, // dbKind
			"test",
			nil, // driver
			nil, // handle
			nil, // tx
			nil, // no logger
			nil, // no metrics
			config,
		)
		require.Nil(t, db2.retryManager, "RetryManager should not be created without logger and metrics")

	})
}

func TestRetryManagerDependencyExtraction(t *testing.T) {
	t.Run("GetDependencies and GetHandler work correctly", func(t *testing.T) {
		logger := log.NewNoopLogger()
		metricsHandler := metrics.NoopMetricsHandler
		config := RetryConfig{
			MaxRetries:   5,
			BaseDelay:    100,
			MaxDelay:     5000,
			JitterFactor: 0.25,
		}

		// Create the retry configuration and metrics shared with transactions.
		retryManager := NewRetryManager(config, logger, metricsHandler)
		require.NotNil(t, retryManager, "RetryManager should be created")

		// Extract dependencies
		extractedLogger, extractedMetrics, extractedConfig := retryManager.GetDependencies()

		// Verify extracted values
		require.NotNil(t, extractedLogger, "Extracted logger should not be nil")
		require.NotNil(t, extractedMetrics, "Extracted metrics should not be nil")
		require.Equal(t, config.MaxRetries, extractedConfig.MaxRetries)
		require.Equal(t, config.BaseDelay, extractedConfig.BaseDelay)
		require.Equal(t, config.MaxDelay, extractedConfig.MaxDelay)
		require.InDelta(t, config.JitterFactor, extractedConfig.JitterFactor, 1e-9)

		// Verify metrics handler can be extracted
		extractedHandler := extractedMetrics.GetHandler()
		require.Equal(t, metricsHandler, extractedHandler, "Extracted metrics handler should match original")
	})
}
