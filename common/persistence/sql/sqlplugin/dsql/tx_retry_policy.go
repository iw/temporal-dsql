package dsql

import (
	"errors"
	"math"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

// dsqlTxRetryPolicy enables SqlStore txExecute retry for DSQL only.
// It also records DSQL-specific metrics for transaction retries.
type dsqlTxRetryPolicy struct {
	maxRetries   int
	baseDelay    time.Duration
	maxDelay     time.Duration
	jitterFactor float64
	metrics      DSQLMetrics
}

var _ sqlplugin.TxRetryPolicy = (*dsqlTxRetryPolicy)(nil)

func NewDSQLTxRetryPolicy(rm *RetryManager) *dsqlTxRetryPolicy {
	cfg := rm.config.validate()
	return &dsqlTxRetryPolicy{
		maxRetries:   cfg.MaxRetries,
		baseDelay:    cfg.BaseDelay,
		maxDelay:     cfg.MaxDelay,
		jitterFactor: cfg.JitterFactor,
		metrics:      rm.metrics,
	}
}

func (p *dsqlTxRetryPolicy) TxRetryBackoff(operation string, attempt int, err error) (time.Duration, bool) {
	if attempt > p.maxRetries {
		if p.metrics != nil {
			p.metrics.IncTxExhausted(operation)
		}
		return 0, false
	}

	delay := float64(p.baseDelay) * math.Pow(2, float64(attempt-1))
	if delay > float64(p.maxDelay) {
		delay = float64(p.maxDelay)
	}
	if p.jitterFactor > 0 {
		j := (rand.Float64()*2 - 1) * p.jitterFactor
		delay = delay + (delay * j)
	}
	if delay < 0 {
		delay = 0
	}
	backoff := time.Duration(delay)
	if p.metrics != nil {
		p.metrics.IncTxConflict(operation)
		p.metrics.IncTxRetry(operation, attempt)
		p.metrics.ObserveTxBackoff(operation, backoff)
		p.metrics.IncTxErrorClass(operation, classifyError(err).String())
	}
	return backoff, true
}

// IsRetryableTxError returns true only for retryable OCC conflicts.
// ConditionFailedError must never be retried.
func (p *dsqlTxRetryPolicy) IsRetryableTxError(err error) bool {
	if IsConditionFailedError(err) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "40001"
	}
	return false
}
