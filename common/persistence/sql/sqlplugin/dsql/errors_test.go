package dsql

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

// createPgError creates a pgconn.PgError for testing purposes
func createPgError(sqlState, message string) *pgconn.PgError {
	return &pgconn.PgError{
		Severity:         "ERROR",
		Code:             sqlState,
		Message:          message,
		Detail:           "",
		Hint:             "",
		Position:         0,
		InternalPosition: 0,
		InternalQuery:    "",
		Where:            "",
		SchemaName:       "",
		TableName:        "",
		ColumnName:       "",
		DataTypeName:     "",
		ConstraintName:   "",
		File:             "",
		Line:             0,
		Routine:          "",
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected ErrorType
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: ErrorTypeUnknown,
		},
		{
			name:     "condition failed error",
			err:      NewConditionFailedError(ConditionFailedUnknown, "test condition failed"),
			expected: ErrorTypeConditionFailed,
		},
		{
			name:     "serialization failure",
			err:      createPgError("40001", "serialization failure"),
			expected: ErrorTypeRetryable,
		},
		{
			name:     "unsupported feature",
			err:      createPgError("0A000", "feature not supported"),
			expected: ErrorTypeUnsupportedFeature,
		},
		{
			name:     "other postgres error",
			err:      createPgError("23505", "unique violation"),
			expected: ErrorTypePermanent,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: ErrorTypePermanent,
		},
		{
			name:     "generic error",
			err:      errors.New("generic error"),
			expected: ErrorTypePermanent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClassifyError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "condition failed error - not retryable",
			err:      NewConditionFailedError(ConditionFailedUnknown, "test condition failed"),
			expected: false,
		},
		{
			name:     "serialization failure - retryable",
			err:      createPgError("40001", "serialization failure"),
			expected: true,
		},
		{
			name:     "unsupported feature - not retryable",
			err:      createPgError("0A000", "feature not supported"),
			expected: false,
		},
		{
			name:     "other postgres error - not retryable",
			err:      createPgError("23505", "unique violation"),
			expected: false,
		},
		{
			name:     "context deadline exceeded - not retryable",
			err:      context.DeadlineExceeded,
			expected: false,
		},
		{
			name:     "generic error - not retryable",
			err:      errors.New("generic error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestWrapWithConditionFailed(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		msg      string
		args     []interface{}
		expected string
	}{
		{
			name:     "wrap nil error",
			err:      nil,
			msg:      "test message",
			expected: "condition failed: test message",
		},
		{
			name:     "wrap existing error",
			err:      errors.New("original error"),
			msg:      "test message",
			expected: "condition failed: test message: original error",
		},
		{
			name:     "wrap with format args",
			err:      nil,
			msg:      "test message %s %d",
			args:     []interface{}{"arg1", 42},
			expected: "condition failed: test message arg1 42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := WrapWithConditionFailed(tt.err, tt.msg, tt.args...)
			assert.Equal(t, tt.expected, result.Error())
			assert.True(t, IsConditionFailedError(result))
		})
	}
}

// TestClassifyError_DSQLSpecificCodes tests DSQL-specific error code classification
func TestClassifyError_DSQLSpecificCodes(t *testing.T) {
	tests := []struct {
		name     string
		sqlState string
		message  string
		expected ErrorType
	}{
		// DSQL OCC (Optimistic Concurrency Control) errors - should be retryable
		{
			name:     "DSQL OCC data conflict (OC000)",
			sqlState: "OC000",
			message:  "mutation conflicts with another transaction",
			expected: ErrorTypeRetryable,
		},
		{
			name:     "DSQL OCC schema conflict (OC001)",
			sqlState: "OC001",
			message:  "schema has been updated by another transaction",
			expected: ErrorTypeRetryable,
		},

		// Connection limit errors - should NOT be retryable
		{
			name:     "DSQL too many connections (53300)",
			sqlState: "53300",
			message:  "too many connections for cluster",
			expected: ErrorTypeConnectionLimit,
		},
		{
			name:     "DSQL connection rate exceeded (53400)",
			sqlState: "53400",
			message:  "connection rate limit exceeded",
			expected: ErrorTypeConnectionLimit,
		},

		// Transaction timeout - should NOT be retryable
		{
			name:     "DSQL transaction timeout (54000)",
			sqlState: "54000",
			message:  "transaction timeout",
			expected: ErrorTypeTransactionTimeout,
		},

		// Standard PostgreSQL serialization failure - should be retryable
		{
			name:     "PostgreSQL serialization failure (40001)",
			sqlState: "40001",
			message:  "could not serialize access due to concurrent update",
			expected: ErrorTypeRetryable,
		},

		// Feature not supported - should NOT be retryable
		{
			name:     "Feature not supported (0A000)",
			sqlState: "0A000",
			message:  "feature not supported",
			expected: ErrorTypeUnsupportedFeature,
		},

		// Other errors - permanent
		{
			name:     "Unique violation (23505)",
			sqlState: "23505",
			message:  "duplicate key value violates unique constraint",
			expected: ErrorTypePermanent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := createPgError(tt.sqlState, tt.message)
			result := ClassifyError(err)
			assert.Equal(t, tt.expected, result, "Error code %s should be classified as %s", tt.sqlState, tt.expected)
		})
	}
}

// TestIsRetryableError_DSQLSpecificCodes tests retryability of DSQL-specific errors
func TestIsRetryableError_DSQLSpecificCodes(t *testing.T) {
	tests := []struct {
		name      string
		sqlState  string
		message   string
		retryable bool
	}{
		// Retryable OCC errors
		{"OC000 is retryable", "OC000", "mutation conflicts", true},
		{"OC001 is retryable", "OC001", "schema conflict", true},
		{"40001 is retryable", "40001", "serialization failure", true},

		// Non-retryable errors
		{"53300 is not retryable", "53300", "too many connections", false},
		{"53400 is not retryable", "53400", "rate limit exceeded", false},
		{"54000 is not retryable", "54000", "transaction timeout", false},
		{"0A000 is not retryable", "0A000", "feature not supported", false},
		{"23505 is not retryable", "23505", "unique violation", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := createPgError(tt.sqlState, tt.message)
			result := IsRetryableError(err)
			assert.Equal(t, tt.retryable, result)
		})
	}
}

// TestClassifyError_StringBasedFallback tests error classification via string matching
// when errors are not wrapped as pgconn.PgError
func TestClassifyError_StringBasedFallback(t *testing.T) {
	tests := []struct {
		name     string
		errMsg   string
		expected ErrorType
	}{
		// DSQL OCC codes in error strings
		{
			name:     "OC000 in error string",
			errMsg:   "server error: FATAL: mutation conflicts with another transaction (SQLSTATE OC000)",
			expected: ErrorTypeRetryable,
		},
		{
			name:     "OC001 in error string",
			errMsg:   "ERROR: schema has been updated by another transaction (OC001)",
			expected: ErrorTypeRetryable,
		},

		// Connection limit codes in error strings
		{
			name:     "53300 in error string",
			errMsg:   "connection error: too many connections (SQLSTATE 53300)",
			expected: ErrorTypeConnectionLimit,
		},
		{
			name:     "53400 in error string",
			errMsg:   "ERROR: connection rate limit exceeded (53400)",
			expected: ErrorTypeConnectionLimit,
		},

		// Transaction timeout in error string
		{
			name:     "54000 in error string",
			errMsg:   "ERROR: transaction timeout (SQLSTATE 54000)",
			expected: ErrorTypeTransactionTimeout,
		},

		// Serialization failure in error string
		{
			name:     "40001 in error string",
			errMsg:   "could not serialize access due to concurrent update (SQLSTATE 40001)",
			expected: ErrorTypeRetryable,
		},

		// No recognizable code - permanent
		{
			name:     "unknown error",
			errMsg:   "some random database error",
			expected: ErrorTypePermanent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			result := ClassifyError(err)
			assert.Equal(t, tt.expected, result, "Error message '%s' should be classified as %s", tt.errMsg, tt.expected)
		})
	}
}
