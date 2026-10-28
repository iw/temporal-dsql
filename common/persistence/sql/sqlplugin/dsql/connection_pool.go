package dsql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/jackc/pgx/v5"
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
)

var (
	errConnectThrottled    = errors.New("DSQL connection throttled")
	connectionAttemptSlots = make(chan struct{}, connectionAttemptLimit)
)

func (p *plugin) poolConfig(cfg *config.SQL, r resolver.ServiceResolver, tokenProvider driver.TokenProvider, logger log.Logger) (*pgxpool.Config, error) {
	dsqlConfig := *cfg
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
		return beforeConnect(hookCtx, connConfig)
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

func (p *plugin) warnIgnoredSettings(logger log.Logger) {
	p.ignoredSettingsOnce.Do(func() {
		var ignored []string
		// Retired settings are recognized only to emit a migration warning.
		for _, name := range []string{
			"DSQL_RESERVOIR_ENABLED", "DSQL_RESERVOIR_TARGET_READY", "DSQL_RESERVOIR_LOW_WATERMARK",
			"DSQL_RESERVOIR_BASE_LIFETIME", "DSQL_RESERVOIR_LIFETIME_JITTER", "DSQL_RESERVOIR_GUARD_WINDOW",
			"DSQL_RESERVOIR_INITIAL_FILL_TIMEOUT", "DSQL_RESERVOIR_INFLIGHT_LIMIT",
			"DSQL_POOL_WARMUP_STAGGER_DURATION", "DSQL_POOL_KEEPER_TICK_INTERVAL", "DSQL_POOL_KEEPER_MAX_CONNS_PER_TICK",
			"DSQL_STAGGERED_STARTUP", "DSQL_STAGGERED_STARTUP_MAX_DELAY",
		} {
			if _, set := os.LookupEnv(name); set {
				ignored = append(ignored, name)
			}
		}
		if len(ignored) > 0 {
			logger.Warn("DSQL connection settings are ignored with pgxpool", tag.NewStringTag("ignored_settings", strings.Join(ignored, ", ")))
		}
	})
}
