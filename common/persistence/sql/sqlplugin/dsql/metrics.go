package dsql

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/server/common/metrics"
)

// DSQLMetrics defines the interface for DSQL-specific metrics.
// The transaction retry policy uses it to record conflicts, retries and exhaustion.
type DSQLMetrics interface {
	// IncTxErrorClass increments the counter for a specific error class
	IncTxErrorClass(op string, errorClass string)

	// IncTxExhausted increments the counter when retry budget is exhausted
	IncTxExhausted(op string)

	// IncTxConflict increments the counter for transaction conflicts (SQLSTATE 40001)
	IncTxConflict(op string)

	// IncTxRetry increments the counter for retry attempts
	IncTxRetry(op string, attempt int)

	// ObserveTxBackoff records the backoff delay duration
	ObserveTxBackoff(op string, delay time.Duration)

	// StartPoolCollector starts a background goroutine that periodically samples
	// connection pool statistics and records them as metrics.
	StartPoolCollector(pool *pgxpool.Pool, interval time.Duration)

	// StopPoolCollector stops the background pool metrics collector.
	StopPoolCollector()

	// GetHandler returns the underlying metrics handler, or nil if not available
	GetHandler() metrics.Handler
}

// dsqlMetricsImpl implements the DSQLMetrics interface using Temporal's metrics system
type dsqlMetricsImpl struct {
	handler      metrics.Handler
	txErrorClass metrics.CounterIface
	txExhausted  metrics.CounterIface
	txConflict   metrics.CounterIface
	txRetry      metrics.CounterIface
	txBackoff    metrics.TimerIface

	// Connection pool metrics
	poolMaxOpen          metrics.GaugeIface
	poolOpen             metrics.GaugeIface
	poolInUse            metrics.GaugeIface
	poolIdle             metrics.GaugeIface
	poolWaitCount        metrics.CounterIface
	poolConstructing     metrics.GaugeIface
	poolNewConns         metrics.CounterIface
	poolLifetimeDestroys metrics.CounterIface
	poolCanceledAcquires metrics.CounterIface

	// Pool collector state
	poolCollectorMu     sync.Mutex
	poolCollectorCancel context.CancelFunc
	poolCollectorDone   chan struct{}
	lastPoolCounters    [4]int64
}

// NewDSQLMetrics creates a new DSQLMetrics implementation.
// If metricsHandler is nil, returns a no-op implementation.
func NewDSQLMetrics(metricsHandler metrics.Handler) DSQLMetrics {
	if metricsHandler == nil {
		return &noOpDSQLMetrics{}
	}

	return &dsqlMetricsImpl{
		handler:      metricsHandler,
		txErrorClass: metricsHandler.Counter("dsql_tx_error_class_total"),
		txExhausted:  metricsHandler.Counter("dsql_tx_exhausted_total"),
		txConflict:   metricsHandler.Counter("dsql_tx_conflict_total"),
		txRetry:      metricsHandler.Counter("dsql_tx_retry_total"),
		txBackoff:    metricsHandler.Timer("dsql_tx_backoff"),

		// Connection pool metrics
		poolMaxOpen:          metricsHandler.Gauge("dsql_pool_max_open"),
		poolOpen:             metricsHandler.Gauge("dsql_pool_open"),
		poolInUse:            metricsHandler.Gauge("dsql_pool_in_use"),
		poolIdle:             metricsHandler.Gauge("dsql_pool_idle"),
		poolWaitCount:        metricsHandler.Counter("dsql_pool_wait_total"),
		poolConstructing:     metricsHandler.Gauge("dsql_pool_constructing"),
		poolNewConns:         metricsHandler.Counter("dsql_pool_new_connections_total"),
		poolLifetimeDestroys: metricsHandler.Counter("dsql_pool_lifetime_destroys_total"),
		poolCanceledAcquires: metricsHandler.Counter("dsql_pool_canceled_acquires_total"),
	}
}

// GetHandler returns the underlying metrics handler, or nil if this is a no-op implementation
func (m *dsqlMetricsImpl) GetHandler() metrics.Handler {
	return m.handler
}

// GetHandler returns nil for no-op implementation
func (n *noOpDSQLMetrics) GetHandler() metrics.Handler {
	return nil
}

// IncTxErrorClass increments the counter for a specific error class
func (m *dsqlMetricsImpl) IncTxErrorClass(op string, errorClass string) {
	m.txErrorClass.Record(1,
		metrics.StringTag("operation", op),
		metrics.StringTag("error_class", errorClass),
	)
}

// IncTxExhausted increments the counter when retry budget is exhausted
func (m *dsqlMetricsImpl) IncTxExhausted(op string) {
	m.txExhausted.Record(1, metrics.StringTag("operation", op))
}

// IncTxConflict increments the counter for transaction conflicts (SQLSTATE 40001)
func (m *dsqlMetricsImpl) IncTxConflict(op string) {
	m.txConflict.Record(1, metrics.StringTag("operation", op))
}

// IncTxRetry increments the counter for retry attempts
func (m *dsqlMetricsImpl) IncTxRetry(op string, attempt int) {
	m.txRetry.Record(1,
		metrics.StringTag("operation", op),
		metrics.StringTag("attempt", strconv.Itoa(attempt)),
	)
}

// ObserveTxBackoff records the backoff delay duration
func (m *dsqlMetricsImpl) ObserveTxBackoff(op string, delay time.Duration) {
	m.txBackoff.Record(delay, metrics.StringTag("operation", op))
}

// StartPoolCollector samples pgxpool, which owns all physical connections.
func (m *dsqlMetricsImpl) StartPoolCollector(pool *pgxpool.Pool, interval time.Duration) {
	m.poolCollectorMu.Lock()
	defer m.poolCollectorMu.Unlock()
	if m.poolCollectorCancel != nil || pool == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.poolCollectorCancel = cancel
	m.poolCollectorDone = make(chan struct{})
	m.lastPoolCounters = [4]int64{}
	go func() {
		defer close(m.poolCollectorDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.recordPoolStats(pool.Stat())
			}
		}
	}()
}

// StopPoolCollector waits for the collector to exit before returning.
func (m *dsqlMetricsImpl) StopPoolCollector() {
	m.poolCollectorMu.Lock()
	defer m.poolCollectorMu.Unlock()
	if m.poolCollectorCancel != nil {
		m.poolCollectorCancel()
		<-m.poolCollectorDone
		m.poolCollectorCancel = nil
	}
}

func (m *dsqlMetricsImpl) recordPoolStats(stats *pgxpool.Stat) {
	m.poolMaxOpen.Record(float64(stats.MaxConns()))
	m.poolOpen.Record(float64(stats.TotalConns()))
	m.poolInUse.Record(float64(stats.AcquiredConns()))
	m.poolIdle.Record(float64(stats.IdleConns()))
	m.poolConstructing.Record(float64(stats.ConstructingConns()))
	values := [4]int64{stats.EmptyAcquireCount(), stats.NewConnsCount(), stats.MaxLifetimeDestroyCount(), stats.CanceledAcquireCount()}
	counters := [4]metrics.CounterIface{m.poolWaitCount, m.poolNewConns, m.poolLifetimeDestroys, m.poolCanceledAcquires}
	for i, value := range values {
		if delta := value - m.lastPoolCounters[i]; delta > 0 {
			counters[i].Record(delta)
		}
	}
	m.lastPoolCounters = values
}

// noOpDSQLMetrics is a no-op implementation of DSQLMetrics for when metrics are disabled
type noOpDSQLMetrics struct{}

func (n *noOpDSQLMetrics) IncTxErrorClass(op string, errorClass string)                  {}
func (n *noOpDSQLMetrics) IncTxExhausted(op string)                                      {}
func (n *noOpDSQLMetrics) IncTxConflict(op string)                                       {}
func (n *noOpDSQLMetrics) IncTxRetry(op string, attempt int)                             {}
func (n *noOpDSQLMetrics) ObserveTxBackoff(op string, delay time.Duration)               {}
func (n *noOpDSQLMetrics) StartPoolCollector(pool *pgxpool.Pool, interval time.Duration) {}
func (n *noOpDSQLMetrics) StopPoolCollector()                                            {}
