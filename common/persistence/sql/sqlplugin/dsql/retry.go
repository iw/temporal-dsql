// Package dsql provides Aurora DSQL-specific helpers for Temporal's SQL persistence plugin.
//
// This file implements transaction retry semantics suitable for Aurora DSQL's
// optimistic concurrency control (OCC) model.
//
// Authoritative retry rules (DSQL plugin):
//  1. ConditionFailedError (CAS / fencing loss) => NEVER retry, return immediately.
//  2. SQLSTATE 40001 (serialization failure)     => retry entire transaction.
//  3. SQLSTATE OC000 (DSQL OCC data conflict)    => retry entire transaction.
//  4. SQLSTATE OC001 (DSQL OCC schema conflict)  => retry entire transaction.
//  5. SQLSTATE 0A000 (feature not supported)     => fail fast (implementation bug), no retry.
//  6. SQLSTATE 53300 (too many connections)      => fail fast, no retry (cluster limit).
//  7. SQLSTATE 53400 (connection rate exceeded)  => fail fast, no retry (rate limit).
//  8. All others                                 => permanent failure, no retry.
package dsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

// DSQL-specific SQL state codes
const (
	// Standard PostgreSQL serialization failure
	sqlStateSerializationFailure = "40001"

	// DSQL OCC (Optimistic Concurrency Control) error codes
	// These are DSQL-specific and indicate transaction conflicts
	sqlStateOCCDataConflict   = "OC000" // mutation conflicts with another transaction
	sqlStateOCCSchemaConflict = "OC001" // schema has been updated by another transaction

	// Feature not supported
	sqlStateFeatureNotSupported = "0A000"

	// DSQL connection limit error codes
	// These indicate cluster-wide resource exhaustion
	sqlStateTooManyConnections     = "53300" // TOO_MANY_CONNECTIONS - cluster connection limit reached
	sqlStateConnectionRateExceeded = "53400" // CONFIGURED_LIMIT_EXCEEDED - connection rate limit exceeded

	// Transaction timeout
	sqlStateTransactionTimeout = "54000" // max transaction time exceeded (5 min for DSQL)
)

type RetryConfig struct {
	MaxRetries   int           `yaml:"max_retries" json:"max_retries"`
	BaseDelay    time.Duration `yaml:"base_delay" json:"base_delay"`
	MaxDelay     time.Duration `yaml:"max_delay" json:"max_delay"`
	JitterFactor float64       `yaml:"jitter_factor" json:"jitter_factor"`
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:   5,
		BaseDelay:    100 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		JitterFactor: 0.25,
	}
}

func (c RetryConfig) validate() RetryConfig {
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = 100 * time.Millisecond
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = 5 * time.Second
	}
	if c.MaxDelay < c.BaseDelay {
		c.MaxDelay = c.BaseDelay
	}
	if c.JitterFactor < 0 {
		c.JitterFactor = 0
	}
	if c.JitterFactor > 1 {
		c.JitterFactor = 1
	}
	return c
}

type RetryManager struct {
	db      *sql.DB
	config  RetryConfig
	logger  log.Logger
	metrics DSQLMetrics
}

func NewRetryManager(db *sql.DB, cfg RetryConfig, logger log.Logger, mh metrics.Handler) *RetryManager {
	cfg = cfg.validate()
	return &RetryManager{
		db:      db,
		config:  cfg,
		logger:  logger,
		metrics: NewDSQLMetrics(mh),
	}
}

// RunTx executes fn in a transaction with DSQL retry semantics.
// op is used only for logging/metrics labels.
func (r *RetryManager) RunTx(ctx context.Context, op string, fn func(*sql.Tx) (interface{}, error)) (interface{}, error) {
	var zero interface{}

	maxAttempts := r.config.MaxRetries + 1
	var lastErr error

	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	start := time.Now()
	defer func() {
		r.metrics.ObserveTxLatency(op, time.Since(start))
	}()

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return zero, err
		}

		result, err := fn(tx)
		if err != nil {
			lastErr = err
			_ = tx.Rollback()

			cls := classifyError(err)
			r.metrics.IncTxErrorClass(op, cls.String())

			// Non-retryable errors - fail immediately
			if cls == ErrorTypeConditionFailed ||
				cls == ErrorTypeUnsupportedFeature ||
				cls == ErrorTypePermanent ||
				cls == ErrorTypeConnectionLimit ||
				cls == ErrorTypeTransactionTimeout {
				return zero, err
			}

			// Retry only on retryable class.
			if attempt == maxAttempts {
				r.metrics.IncTxExhausted(op)
				return zero, fmt.Errorf("dsql: max retries exceeded (op=%s attempts=%d): %w", op, attempt, lastErr)
			}

			r.metrics.IncTxConflict(op)
			r.metrics.IncTxRetry(op, attempt)

			delay := r.calculateBackoff(attempt - 1)
			r.metrics.ObserveTxBackoff(op, delay)

			if r.logger != nil {
				r.logger.Warn("dsql tx retryable error, retrying",
					tag.Operation(op),
					tag.Attempt(int32(attempt)),
					tag.NewDurationTag("backoff", delay),
					tag.Error(err),
				)
			}

			if err := sleepWithContext(ctx, &timer, delay); err != nil {
				return zero, err
			}
			continue
		}

		if err := tx.Commit(); err != nil {
			lastErr = err
			cls := classifyError(err)
			r.metrics.IncTxErrorClass(op, cls.String())

			if cls == ErrorTypeRetryable && attempt < maxAttempts {
				r.metrics.IncTxConflict(op)
				r.metrics.IncTxRetry(op, attempt)

				delay := r.calculateBackoff(attempt - 1)
				r.metrics.ObserveTxBackoff(op, delay)

				if r.logger != nil {
					r.logger.Warn("dsql tx commit retryable error, retrying",
						tag.Operation(op),
						tag.Attempt(int32(attempt)),
						tag.NewDurationTag("backoff", delay),
						tag.Error(err),
					)
				}

				if err := sleepWithContext(ctx, &timer, delay); err != nil {
					return zero, err
				}
				continue
			}

			if cls == ErrorTypeRetryable && attempt == maxAttempts {
				r.metrics.IncTxExhausted(op)
				return zero, fmt.Errorf("dsql: max retries exceeded (op=%s attempts=%d): %w", op, attempt, lastErr)
			}

			return zero, err
		}

		return result, nil
	}

	return zero, fmt.Errorf("dsql: unexpected retry loop exit (op=%s): %w", op, lastErr)
}

func (r *RetryManager) RunTxVoid(ctx context.Context, op string, fn func(*sql.Tx) error) error {
	_, err := r.RunTx(ctx, op, func(tx *sql.Tx) (interface{}, error) {
		return nil, fn(tx)
	})
	return err
}

func classifyError(err error) ErrorType {
	if err == nil {
		return ErrorTypeUnknown
	}
	if IsConditionFailedError(err) {
		return ErrorTypeConditionFailed
	}

	// Try to extract PostgreSQL error code from pgconn.PgError
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return classifyByCode(pgErr.SQLState())
	}

	// Fallback: check error string for DSQL-specific error codes
	// pgx sometimes wraps errors in a way that doesn't expose PgError directly
	// Error messages may contain patterns like "SQLSTATE OC000" or "(OC000)"
	errStr := err.Error()
	if code := extractSQLStateFromString(errStr); code != "" {
		return classifyByCode(code)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorTypePermanent
	}
	return ErrorTypePermanent
}

// classifyByCode classifies an error based on its SQL state code
func classifyByCode(code string) ErrorType {
	switch code {
	// Retryable OCC errors - these should be retried
	case sqlStateSerializationFailure, // Standard PostgreSQL serialization failure
		sqlStateOCCDataConflict,   // DSQL: mutation conflicts with another transaction
		sqlStateOCCSchemaConflict: // DSQL: schema has been updated by another transaction
		return ErrorTypeRetryable

	// Feature not supported - implementation bug, fail fast
	case sqlStateFeatureNotSupported:
		return ErrorTypeUnsupportedFeature

	// Connection limit errors - cluster resource exhaustion, fail fast
	// These should not be retried as they indicate cluster-wide limits
	case sqlStateTooManyConnections, // Cluster connection limit reached
		sqlStateConnectionRateExceeded: // Connection rate limit exceeded
		return ErrorTypeConnectionLimit

	// Transaction timeout - fail fast, transaction took too long
	case sqlStateTransactionTimeout:
		return ErrorTypeTransactionTimeout

	default:
		return ErrorTypePermanent
	}
}

// extractSQLStateFromString attempts to extract a SQL state code from an error string.
// pgx error messages may contain patterns like:
// - "SQLSTATE OC000"
// - "(OC000)"
// - "error code OC000"
func extractSQLStateFromString(errStr string) string {
	// Check for DSQL OCC codes
	if strings.Contains(errStr, sqlStateOCCDataConflict) {
		return sqlStateOCCDataConflict
	}
	if strings.Contains(errStr, sqlStateOCCSchemaConflict) {
		return sqlStateOCCSchemaConflict
	}

	// Check for connection limit codes
	if strings.Contains(errStr, sqlStateTooManyConnections) {
		return sqlStateTooManyConnections
	}
	if strings.Contains(errStr, sqlStateConnectionRateExceeded) {
		return sqlStateConnectionRateExceeded
	}

	// Check for transaction timeout
	if strings.Contains(errStr, sqlStateTransactionTimeout) {
		return sqlStateTransactionTimeout
	}

	// Check for serialization failure
	if strings.Contains(errStr, sqlStateSerializationFailure) {
		return sqlStateSerializationFailure
	}

	return ""
}

// ClassifyError is the exported version of classifyError for testing
func ClassifyError(err error) ErrorType {
	return classifyError(err)
}

// IsRetryableError checks if an error should be retried
func IsRetryableError(err error) bool {
	return classifyError(err) == ErrorTypeRetryable
}

// RunTxWithRetry is a standalone function for running transactions with retry logic
// This is a backwards-compatible helper that creates a temporary RetryManager
func RunTxWithRetry(ctx context.Context, db *sql.DB, op string, fn func(*sql.Tx) (interface{}, error)) (interface{}, error) {
	cfg := DefaultRetryConfig()
	rm := NewRetryManager(db, cfg, nil, nil) // Use nil logger and metrics for simple usage
	return rm.RunTx(ctx, op, fn)
}

// RunTxWithRetryVoid is a standalone function for running void transactions with retry logic
// This is a backwards-compatible helper that creates a temporary RetryManager
func RunTxWithRetryVoid(ctx context.Context, db *sql.DB, op string, fn func(*sql.Tx) error) error {
	cfg := DefaultRetryConfig()
	rm := NewRetryManager(db, cfg, nil, nil) // Use nil logger and metrics for simple usage
	return rm.RunTxVoid(ctx, op, fn)
}

func (r *RetryManager) calculateBackoff(attempt int) time.Duration {
	delay := float64(r.config.BaseDelay) * math.Pow(2, float64(attempt))
	if delay > float64(r.config.MaxDelay) {
		delay = float64(r.config.MaxDelay)
	}
	if r.config.JitterFactor > 0 {
		j := (rand.Float64()*2 - 1) * r.config.JitterFactor
		delay = delay + (delay * j)
	}
	if delay < 0 {
		delay = 0
	}
	return time.Duration(delay)
}

func sleepWithContext(ctx context.Context, timer **time.Timer, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if *timer == nil {
		*timer = time.NewTimer(d)
	} else {
		(*timer).Reset(d)
	}
	select {
	case <-(*timer).C:
		return nil
	case <-ctx.Done():
		if !(*timer).Stop() {
			select {
			case <-(*timer).C:
			default:
			}
		}
		return ctx.Err()
	}
}

// GetDependencies returns the dependencies used by this RetryManager
func (r *RetryManager) GetDependencies() (log.Logger, DSQLMetrics, RetryConfig) {
	return r.logger, r.metrics, r.config
}
