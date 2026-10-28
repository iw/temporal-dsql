package dsql

import (
	"testing"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

// TestRetryConfig tests the default retry configuration
func TestRetryConfig(t *testing.T) {
	config := DefaultRetryConfig()

	if config.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries to be 5, got %d", config.MaxRetries)
	}

	if config.BaseDelay != 100*time.Millisecond {
		t.Errorf("Expected BaseDelay to be 100ms, got %v", config.BaseDelay)
	}

	if config.MaxDelay != 5*time.Second {
		t.Errorf("Expected MaxDelay to be 5s, got %v", config.MaxDelay)
	}

	if config.JitterFactor != 0.25 {
		t.Errorf("Expected JitterFactor to be 0.25, got %f", config.JitterFactor)
	}
}

// TestConditionFailedError tests the ConditionFailedError type
func TestConditionFailedError(t *testing.T) {
	err := &ConditionFailedError{Msg: "test condition failed"}

	if err.Error() != "condition failed: test condition failed" {
		t.Errorf("Expected error message 'condition failed: test condition failed', got '%s'", err.Error())
	}

	if !IsConditionFailedError(err) {
		t.Error("Expected IsConditionFailedError to return true for ConditionFailedError")
	}

	// Test with non-ConditionFailedError
	otherErr := &testError{msg: "other error"}
	if IsConditionFailedError(otherErr) {
		t.Error("Expected IsConditionFailedError to return false for non-ConditionFailedError")
	}
}

// TestErrorTypeString tests the ErrorType string representation
func TestErrorTypeString(t *testing.T) {
	tests := []struct {
		errorType ErrorType
		expected  string
	}{
		{ErrorTypeRetryable, "retryable"},
		{ErrorTypeConditionFailed, "condition_failed"},
		{ErrorTypePermanent, "permanent"},
		{ErrorTypeUnsupportedFeature, "unsupported_feature"},
		{ErrorTypeConnectionLimit, "connection_limit"},
		{ErrorTypeTransactionTimeout, "transaction_timeout"},
		{ErrorTypeUnknown, "unknown"},
	}

	for _, test := range tests {
		if test.errorType.String() != test.expected {
			t.Errorf("Expected ErrorType %d to have string '%s', got '%s'",
				test.errorType, test.expected, test.errorType.String())
		}
	}
}

// TestDSQLMetricsCreation tests that DSQLMetrics can be created
func TestDSQLMetricsCreation(t *testing.T) {
	// Create a no-op metrics handler for testing
	handler := &noOpMetricsHandler{}

	dsqlMetrics := NewDSQLMetrics(handler)
	if dsqlMetrics == nil {
		t.Error("Expected NewDSQLMetrics to return non-nil metrics")
	}

	// Test that nil handler returns no-op implementation
	nilMetrics := NewDSQLMetrics(nil)
	if nilMetrics == nil {
		t.Error("Expected NewDSQLMetrics with nil handler to return non-nil no-op metrics")
	}

	// Test that no-op metrics don't panic
	nilMetrics.IncTxErrorClass("test", "retryable")
	nilMetrics.IncTxExhausted("test")
	nilMetrics.IncTxConflict("test")
	nilMetrics.IncTxRetry("test", 1)
	nilMetrics.ObserveTxBackoff("test", time.Millisecond)

	// Test pool collector methods don't panic on no-op
	nilMetrics.StartPoolCollector(nil, time.Second)
	nilMetrics.StopPoolCollector()
}

// TestNewConditionFailedError tests the NewConditionFailedError function
func TestNewConditionFailedError(t *testing.T) {
	err := NewConditionFailedError(ConditionFailedUnknown, "test message with parameter")

	expected := "condition failed: test message with parameter"
	if err.Error() != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, err.Error())
	}
}

// Helper types for testing

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

// noOpMetricsHandler is a no-op implementation for testing
type noOpMetricsHandler struct{}

func (h *noOpMetricsHandler) WithTags(...metrics.Tag) metrics.Handler { return h }
func (h *noOpMetricsHandler) Counter(string) metrics.CounterIface     { return &noOpCounter{} }
func (h *noOpMetricsHandler) Gauge(string) metrics.GaugeIface         { return &noOpGauge{} }
func (h *noOpMetricsHandler) Timer(string) metrics.TimerIface         { return &noOpTimer{} }
func (h *noOpMetricsHandler) Histogram(string, metrics.MetricUnit) metrics.HistogramIface {
	return &noOpHistogram{}
}
func (h *noOpMetricsHandler) Stop(log.Logger)                        {}
func (h *noOpMetricsHandler) StartBatch(string) metrics.BatchHandler { return h }
func (h *noOpMetricsHandler) Close() error                           { return nil }

type noOpCounter struct{}

func (c *noOpCounter) Record(int64, ...metrics.Tag) {}

type noOpGauge struct{}

func (g *noOpGauge) Record(float64, ...metrics.Tag) {}

type noOpTimer struct{}

func (t *noOpTimer) Record(time.Duration, ...metrics.Tag) {}

type noOpHistogram struct{}

func (h *noOpHistogram) Record(int64, ...metrics.Tag) {}
