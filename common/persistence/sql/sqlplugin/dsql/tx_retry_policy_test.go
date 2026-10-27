package dsql

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

func TestDSQLTxRetryPolicy_Creation(t *testing.T) {
	cfg := RetryConfig{MaxRetries: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: 2 * time.Second, JitterFactor: 0.1}
	policy := NewDSQLTxRetryPolicy(&RetryManager{config: cfg})
	require.Equal(t, 3, policy.maxRetries)
	require.Equal(t, 50*time.Millisecond, policy.baseDelay)
	require.Equal(t, 2*time.Second, policy.maxDelay)
	require.InDelta(t, 0.1, policy.jitterFactor, 0.0001)
}

func TestDSQLTxRetryPolicy_Backoff(t *testing.T) {
	cfg := RetryConfig{MaxRetries: 6, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}
	var policy sqlplugin.TxRetryPolicy = NewDSQLTxRetryPolicy(&RetryManager{config: cfg})
	conflict := createPgError("40001", "serialization failure")
	for _, tc := range []struct {
		attempt int
		delay   time.Duration
		retry   bool
	}{
		{1, 100 * time.Millisecond, true},
		{2, 200 * time.Millisecond, true},
		{3, 400 * time.Millisecond, true},
		{4, 800 * time.Millisecond, true},
		{5, time.Second, true},
		{6, time.Second, true},
		{7, 0, false},
	} {
		t.Run(fmt.Sprintf("attempt_%d", tc.attempt), func(t *testing.T) {
			delay, retry := policy.TxRetryBackoff("test", tc.attempt, conflict)
			require.Equal(t, tc.retry, retry)
			require.Equal(t, tc.delay, delay)
		})
	}
}

func TestDSQLTxRetryPolicy_NoRetries(t *testing.T) {
	policy := NewDSQLTxRetryPolicy(&RetryManager{config: RetryConfig{MaxRetries: 0}})
	delay, retry := policy.TxRetryBackoff("test", 1, createPgError("40001", "conflict"))
	require.False(t, retry)
	require.Zero(t, delay)
}

func TestDSQLTxRetryPolicy_IsRetryableTxError(t *testing.T) {
	policy := NewDSQLTxRetryPolicy(&RetryManager{config: DefaultRetryConfig()})
	conflict := createPgError("40001", "serialization failure")
	condition := NewConditionFailedError(ConditionFailedShard, "test")
	for _, tc := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{"nil", nil, false},
		{"condition_failed", condition, false},
		{"wrapped_condition_failed", fmt.Errorf("wrapped: %w", condition), false},
		{"condition_and_conflict", errors.Join(condition, conflict), false},
		{"serialization_failure", conflict, true},
		{"wrapped_serialization_failure", fmt.Errorf("wrapped: %w", conflict), true},
		{"unsupported_feature", createPgError("0A000", "unsupported"), false},
		{"unique_violation", createPgError("23505", "duplicate"), false},
		{"occ_code", createPgError("OC000", "conflict"), false},
		{"generic", errors.New("SQLSTATE 40001"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.retryable, policy.IsRetryableTxError(tc.err))
		})
	}
}

func TestDSQLTxRetryPolicy_BackoffWithJitter(t *testing.T) {
	cfg := RetryConfig{MaxRetries: 5, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, JitterFactor: 0.25}
	policy := NewDSQLTxRetryPolicy(&RetryManager{config: cfg})
	for range 100 {
		delay, retry := policy.TxRetryBackoff("test", 2, createPgError("40001", "conflict"))
		require.True(t, retry)
		require.GreaterOrEqual(t, delay, 150*time.Millisecond)
		require.LessOrEqual(t, delay, 250*time.Millisecond)
	}
}

func TestDSQLTxRetryPolicy_Metrics(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	cfg := RetryConfig{MaxRetries: 1, BaseDelay: time.Millisecond}
	policy := NewDSQLTxRetryPolicy(&RetryManager{config: cfg, metrics: NewDSQLMetrics(handler)})
	conflict := createPgError("40001", "conflict")
	delay, retry := policy.TxRetryBackoff("UpdateShard", 1, conflict)
	require.True(t, retry)
	require.Equal(t, time.Millisecond, delay)
	_, retry = policy.TxRetryBackoff("UpdateShard", 2, conflict)
	require.False(t, retry)
	recordings := capture.Snapshot()
	for _, name := range []string{"dsql_tx_conflict_total", "dsql_tx_retry_total", "dsql_tx_backoff", "dsql_tx_error_class_total", "dsql_tx_exhausted_total"} {
		require.Len(t, recordings[name], 1, name)
		require.Equal(t, "UpdateShard", recordings[name][0].Tags["operation"])
	}
	require.Equal(t, "1", recordings["dsql_tx_retry_total"][0].Tags["attempt"])
	require.Equal(t, ErrorTypeRetryable.String(), recordings["dsql_tx_error_class_total"][0].Tags["error_class"])
	require.Equal(t, time.Millisecond, recordings["dsql_tx_backoff"][0].Value)
}
