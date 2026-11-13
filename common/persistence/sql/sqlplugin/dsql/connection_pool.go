package dsql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/session"
	"go.temporal.io/server/common/resolver"
)

const (
	// DSQL closes connections at 60 minutes: 50m + up to 4m jitter + a 1m
	// health-check interval leaves about 5m for a transaction before that limit.
	poolMaxConnLifetime       = 50 * time.Minute
	poolMaxConnLifetimeJitter = 4 * time.Minute
	poolHealthCheckPeriod     = time.Minute
	connectionAttemptLimit    = 8
	// EphemeralPoolSize bounds short-lived startup pools.
	EphemeralPoolSize   = 5
	poolPingTimeout     = 30 * time.Second
	poolMetricsInterval = 15 * time.Second
	poolWarmupPoll      = 50 * time.Millisecond

	cancelRequestsSkippedMetric = "dsql_cancel_requests_skipped_total"
	connectLatencyMetric        = "dsql_pool_connect_latency"
	connectFailuresMetric       = "dsql_pool_connect_failures_total"
)

var (
	errConnectThrottled     = errors.New("DSQL connection throttled")
	errCancelRequestSkipped = errors.New("DSQL cancel request skipped")
	connectionAttemptSlots  = make(chan struct{}, connectionAttemptLimit)
)

func (p *plugin) poolConfig(cfg *config.SQL, r resolver.ServiceResolver, tokenProvider driver.TokenProvider, logger log.Logger) (*pgxpool.Config, error) {
	dsqlConfig := *cfg
	dsqlConfig.ConnectAttributes = make(map[string]string, len(cfg.ConnectAttributes))
	for key, value := range cfg.ConnectAttributes {
		if !isPluginAttribute(key) {
			dsqlConfig.ConnectAttributes[key] = value
		}
	}
	dsqlConfig.User = dsqlUser(cfg)
	dsqlConfig.Password = "placeholder"
	if cfg.Password != "" {
		p.passwordWarningOnce.Do(func() {
			logger.Warn("DSQL password is ignored; connections use IAM authentication")
		})
	}
	dsn, err := session.BuildDSN(&dsqlConfig, r)
	if err != nil {
		return nil, fmt.Errorf("failed to build DSN: %w", err)
	}
	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if mode := parsedDSN.Query().Get("sslmode"); mode != "verify-full" {
		p.sslModeWarningOnce.Do(func() {
			logger.Warn("DSQL server certificate and hostname are not fully verified; use sslmode=verify-full", tag.NewStringTag("sslmode", mode))
		})
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	maxConns := cfg.MaxConns
	if maxConns == 0 {
		maxConns = session.DefaultMaxConns
	}
	if cfg.PoolSizeHint == "ephemeral" {
		maxConns = EphemeralPoolSize
	}
	if maxConns < 1 || int64(maxConns) > math.MaxInt32 {
		return nil, fmt.Errorf("invalid DSQL maxConns: %d", maxConns)
	}
	poolConfig.MaxConns = int32(maxConns)
	poolConfig.MinConns = poolConfig.MaxConns
	if cfg.PoolSizeHint == "ephemeral" {
		poolConfig.MinConns = 0
	}
	poolConfig.MaxConnLifetime = poolMaxConnLifetime
	if cfg.MaxConnLifetime > 0 && cfg.MaxConnLifetime < poolMaxConnLifetime {
		poolConfig.MaxConnLifetime = cfg.MaxConnLifetime
	} else if cfg.MaxConnLifetime > poolMaxConnLifetime {
		p.lifetimeWarningOnce.Do(func() {
			logger.Warn("DSQL maxConnLifetime capped at 50 minutes", tag.NewDurationTag("configured", cfg.MaxConnLifetime))
		})
	}
	poolConfig.MaxConnLifetimeJitter = poolMaxConnLifetimeJitter
	poolConfig.HealthCheckPeriod = poolHealthCheckPeriod
	poolConfig.BeforeConnect = p.beforeConnect(tokenProvider)
	return poolConfig, nil
}

func (p *plugin) beforeConnect(tokenProvider driver.TokenProvider) func(context.Context, *pgx.ConnConfig) error {
	return func(ctx context.Context, cfg *pgx.ConnConfig) error {
		select {
		case connectionAttemptSlots <- struct{}{}:
			defer func() { <-connectionAttemptSlots }()
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", errConnectThrottled, ctx.Err())
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: %w", errConnectThrottled, err)
		}
		if err := p.rateLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("%w: %w", errConnectThrottled, err)
		}
		token, err := tokenProvider(ctx)
		if err != nil {
			return err
		}
		cfg.Password = token
		return nil
	}
}

// poolConnector owns the pool because stdlib's connector does not close it.
type poolConnector struct {
	sqldriver.Connector
	pool      *pgxpool.Pool
	metrics   DSQLMetrics
	cancel    context.CancelFunc
	closeOnce sync.Once
}

var _ io.Closer = (*poolConnector)(nil)

func (c *poolConnector) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.metrics.StopPoolCollector()
		c.pool.Close()
	})
	return nil
}

func openPoolDB(cfg *pgxpool.Config, metricsHandler metrics.Handler) (*sqlx.DB, *poolConnector, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg = cfg.Copy()
	beforeConnect := cfg.BeforeConnect
	handler := metricsHandler
	if handler == nil {
		handler = metrics.NoopMetricsHandler
	}
	cancelsSkipped := handler.Counter(cancelRequestsSkippedMetric)
	tracer := &connectTracer{latency: handler.Timer(connectLatencyMetric), failures: handler.Counter(connectFailuresMetric)}
	cfg.ConnConfig.Tracer = tracer
	// Health-check refills use a background context in pgxpool. Cancel their
	// limiter/token waits too when the owning database is closed.
	cfg.BeforeConnect = func(connectCtx context.Context, connConfig *pgx.ConnConfig) error {
		hookCtx, hookCancel := context.WithCancel(connectCtx)
		stop := context.AfterFunc(ctx, hookCancel)
		defer stop()
		defer hookCancel()
		if ctx.Err() != nil {
			hookCancel()
		}
		if err := beforeConnect(hookCtx, connConfig); err != nil {
			tracer.failed(err)
			return err
		}
		skipCancelRequests(connConfig, cancelsSkipped)
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	collector := NewDSQLMetrics(metricsHandler)
	connector := &poolConnector{Connector: stdlib.GetPoolConnector(pool), pool: pool, metrics: collector, cancel: cancel}
	sqlDB := sql.OpenDB(connector)
	sqlDB.SetMaxIdleConns(0)
	db := sqlx.NewDb(sqlDB, "pgx")
	db.MapperFunc(strcase.ToSnake)
	collector.StartPoolCollector(pool, poolMetricsInterval)
	return db, connector, nil
}

// connectTracer times each connection's handshake and counts failed connection
// attempts. pgxpool counts attempts but not their outcomes, and its background fill drops
// their errors. pgx reads connect tracers from the query tracer, whose calls are left as
// they are.
type connectTracer struct {
	latency  metrics.TimerIface
	failures metrics.CounterIface
}

type connectStartKey struct{}

var _ pgx.ConnectTracer = (*connectTracer)(nil)

func (t *connectTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (t *connectTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *connectTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	return context.WithValue(ctx, connectStartKey{}, time.Now())
}

func (t *connectTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	if data.Err != nil {
		t.failed(data.Err)
		return
	}
	if start, ok := ctx.Value(connectStartKey{}).(time.Time); ok {
		t.latency.Record(time.Since(start))
	}
}

// failed counts a failed connection attempt. pgxpool cancels the rest of its fill after
// one fails, and closing a pool cancels its connects; neither is a failure of its own.
func (t *connectTracer) failed(err error) {
	if !errors.Is(err, context.Canceled) {
		t.failures.Record(1)
	}
}

// poolFill reports how many connections a pool has open and how many it keeps.
func poolFill(pool *pgxpool.Pool) func() (open, target int32) {
	target := pool.Config().MinConns
	return func() (int32, int32) {
		stat := pool.Stat()
		return stat.TotalConns() - stat.ConstructingConns(), target
	}
}

// waitForPool waits until a pool has opened the connections it keeps, or until the
// timeout, so that a server starts with full pools rather than opening connections under
// load. It reports whether the pool filled.
func waitForPool(fill func() (open, target int32), timeout time.Duration, logger log.Logger) bool {
	open, target := fill()
	if open >= target {
		return true
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	poll := time.NewTicker(poolWarmupPoll)
	defer poll.Stop()
	for {
		select {
		case <-deadline.C:
			open, target = fill()
			logger.Warn("DSQL pool did not fill within its warm-up timeout",
				tag.NewInt32("open", open), tag.NewInt32("target", target), tag.NewDurationTag("timeout", timeout))
			return false
		case <-poll.C:
			if open, target = fill(); open >= target {
				return true
			}
		}
	}
}

// skipCancelRequests refuses the dials pgx makes once a connection is up. pgx makes them to
// send a cancel request whenever it cuts a statement off or closes a connection after an
// error. DSQL answers each with 0A000 and lets the statement run on, so the dial would only
// add a TCP and TLS handshake.
func skipCancelRequests(cfg *pgx.ConnConfig, skipped metrics.CounterIface) {
	dial := cfg.DialFunc
	if dial == nil {
		return
	}
	afterConnect := cfg.AfterConnect
	var connected atomic.Bool
	cfg.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if connected.Load() {
			skipped.Record(1)
			return nil, errCancelRequestSkipped
		}
		return dial(ctx, network, address)
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
		if afterConnect != nil {
			if err := afterConnect(ctx, conn); err != nil {
				return err
			}
		}
		connected.Store(true)
		return nil
	}
}
