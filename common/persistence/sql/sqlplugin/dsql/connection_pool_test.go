package dsql

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/session"
	"go.temporal.io/server/common/resolver"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type poolTestLimiter func(context.Context) error

func (f poolTestLimiter) Wait(ctx context.Context) error { return f(ctx) }

func testPoolConfig(t *testing.T, p *plugin, cfg *config.SQL) *pgxpool.Config {
	t.Helper()
	cfg.ConnectAddr = "127.0.0.1:5432"
	cfg.DatabaseName = "postgres"
	c, err := p.poolConfig(cfg, resolver.NewNoopResolver(), func(context.Context) (string, error) { return "token", nil }, log.NewNoopLogger())
	require.NoError(t, err)
	return c
}

func TestPoolConfig(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cfg      config.SQL
		max, min int32
		lifetime time.Duration
	}{
		{"default", config.SQL{}, session.DefaultMaxConns, session.DefaultMaxConns, 50 * time.Minute},
		{"service", config.SQL{MaxConns: 17, MaxIdleConns: 1}, 17, 17, 50 * time.Minute},
		{"ephemeral", config.SQL{MaxConns: 100, PoolSizeHint: "ephemeral"}, 5, 0, 50 * time.Minute},
		{"short_lifetime", config.SQL{MaxConnLifetime: 10 * time.Minute}, 100, 100, 10 * time.Minute},
		{"capped_lifetime", config.SQL{MaxConnLifetime: time.Hour}, 100, 100, 50 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testPoolConfig(t, &plugin{}, &tc.cfg)
			require.Equal(t, tc.max, c.MaxConns)
			require.Equal(t, tc.min, c.MinConns)
			require.Equal(t, tc.lifetime, c.MaxConnLifetime)
			require.Equal(t, 4*time.Minute, c.MaxConnLifetimeJitter)
			require.Equal(t, time.Minute, c.HealthCheckPeriod)
			defaults, err := pgxpool.ParseConfig("host=127.0.0.1")
			require.NoError(t, err)
			require.Equal(t, defaults.MaxConnIdleTime, c.MaxConnIdleTime)
			require.Nil(t, c.BeforeAcquire)
			require.Nil(t, c.AfterRelease)
			require.Equal(t, adminUser, c.ConnConfig.User)
			require.Equal(t, "postgres", c.ConnConfig.Database)
		})
	}
}

func TestBeforeConnect(t *testing.T) {
	for _, failure := range []string{"none", "limiter", "token"} {
		t.Run(failure, func(t *testing.T) {
			cause := errors.New("test failure")
			waits, tokens := 0, 0
			p := &plugin{rateLimiter: poolTestLimiter(func(context.Context) error {
				waits++
				if failure == "limiter" {
					return cause
				}
				return nil
			})}
			hook := p.beforeConnect(func(context.Context) (string, error) {
				tokens++
				if failure == "token" {
					return "", cause
				}
				return "fresh-token", nil
			})
			cfg := &pgx.ConnConfig{}
			for range 2 {
				err := hook(t.Context(), cfg)
				switch failure {
				case "limiter":
					require.ErrorIs(t, err, errConnectThrottled)
					require.ErrorIs(t, err, cause)
				case "token":
					require.Same(t, cause, err)
				default:
					require.NoError(t, err)
				}
			}
			require.Equal(t, 2, waits)
			if failure == "limiter" {
				require.Zero(t, tokens)
			} else {
				require.Equal(t, 2, tokens)
			}
			if failure == "none" {
				require.Equal(t, "fresh-token", cfg.Password)
			} else {
				require.Empty(t, cfg.Password)
			}
		})
	}
}

func TestBeforeConnectProcessLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{}, 32)
	results := make(chan error, 16)
	var active, peak atomic.Int32
	limiter := poolTestLimiter(func(ctx context.Context) error {
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old; old = peak.Load() {
			if peak.CompareAndSwap(old, current) {
				break
			}
		}
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	// Different plugin instances and pool configs must share the same semaphore.
	pools := []*pgxpool.Config{
		testPoolConfig(t, &plugin{rateLimiter: limiter}, &config.SQL{}),
		testPoolConfig(t, &plugin{rateLimiter: limiter}, &config.SQL{}),
	}
	for i := range 16 {
		go func() { results <- pools[i%2].BeforeConnect(ctx, &pgx.ConnConfig{}) }()
	}
	require.Eventually(t, func() bool { return len(entered) == connectionAttemptLimit }, time.Second, time.Millisecond)
	require.Equal(t, int32(8), active.Load())
	// With all slots occupied, cancel a caller that must never reach its limiter.
	waitingCtx, waitingCancel := context.WithCancel(t.Context())
	waitingCancel()
	err := pools[0].BeforeConnect(waitingCtx, &pgx.ConnConfig{})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, errConnectThrottled)
	cancel()
	for range 16 {
		select {
		case err := <-results:
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, err, errConnectThrottled)
		case <-time.After(time.Second):
			t.Fatal("hook did not exit after cancellation")
		}
	}
	require.Equal(t, int32(8), peak.Load())
	require.Zero(t, active.Load())
	require.Empty(t, connectionAttemptSlots)
}

func TestPoolErrorsThroughSQL(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		kind string
	}{
		{"throttled", fmt.Errorf("limiter: %w", errConnectThrottled), "overload"},
		{"throttled_nested_pg_error", fmt.Errorf("%w: %w", errConnectThrottled, &pgconn.PgError{Code: "40001"}), "overload"},
		{"rate", &pgconn.PgError{Code: "53400"}, "overload"},
		{"capacity", &pgconn.PgError{Code: "53300"}, "overload"},
		{"bad_conn", sqldriver.ErrBadConn, "unavailable"},
		{"eof", io.EOF, "unavailable"},
		{"unexpected_eof", io.ErrUnexpectedEOF, "unavailable"},
		{"reset", syscall.ECONNRESET, "unavailable"},
		{"aborted", syscall.ECONNABORTED, "unavailable"},
		{"refused", syscall.ECONNREFUSED, "unavailable"},
		{"class08", &pgconn.PgError{Code: "08007"}, "unavailable"},
		{"iam", errors.New("IAM ACCESS DENIED"), "unavailable"},
		{"conflict", &pgconn.PgError{Code: "40001"}, "same"},
		{"unsupported", &pgconn.PgError{Code: "0A000"}, "same"},
		{"size", &pgconn.PgError{Code: "54000"}, "size"},
		{"other", errors.New("ordinary error"), "same"},
		{"read_only", &pgconn.PgError{Code: "25006"}, "same"},
		{"cannot_connect_now", &pgconn.PgError{Code: "57P03"}, "same"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testPoolConfig(t, &plugin{}, &config.SQL{PoolSizeHint: "ephemeral"})
			wrapped := fmt.Errorf("test wrapper: %w", tc.err)
			c.BeforeConnect = func(context.Context, *pgx.ConnConfig) error { return wrapped }
			sqlDB, _, err := openPoolDB(c, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, sqlDB.Close()) }()
			_, err = sqlDB.ExecContext(t.Context(), "SELECT 1")
			require.ErrorIs(t, err, tc.err)
			if expected, ok := tc.err.(*pgconn.PgError); ok {
				var actual *pgconn.PgError
				require.ErrorAs(t, err, &actual)
				require.Same(t, expected, actual)
			}
			converted := (&db{}).maybeConvertError(err)
			switch tc.kind {
			case "overload":
				var exhausted *serviceerror.ResourceExhausted
				require.ErrorAs(t, converted, &exhausted)
				require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, exhausted.Cause)
				require.False(t, persistence.OperationPossiblySucceeded(converted))
			case "unavailable":
				var unavailable *serviceerror.Unavailable
				require.ErrorAs(t, converted, &unavailable)
				require.Equal(t, "database connection lost: "+err.Error(), unavailable.Message)
			case "size":
				var sizeErr *persistence.TransactionSizeLimitError
				require.ErrorAs(t, converted, &sizeErr)
			default:
				require.Same(t, err, converted)
			}
		})
	}
	require.NoError(t, (&db{}).maybeConvertError(nil))
}

func TestPoolConnectErrorThroughSQL(t *testing.T) {
	for _, code := range []string{"", "53300", "53400", "08006"} {
		t.Run("code_"+code, func(t *testing.T) {
			p := &plugin{rateLimiter: poolTestLimiter(func(context.Context) error { return nil })}
			c := testPoolConfig(t, p, &config.SQL{PoolSizeHint: "ephemeral"})
			cause := errors.New("dial failure")
			if code != "" {
				cause = &pgconn.PgError{Code: code}
			}
			c.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) { return nil, cause }
			sqlDB, _, err := openPoolDB(c, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, sqlDB.Close()) }()
			err = sqlDB.PingContext(t.Context())
			require.ErrorIs(t, err, cause)
			var connectErr *pgconn.ConnectError
			require.ErrorAs(t, err, &connectErr)
			if code != "" {
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				require.Equal(t, code, pgErr.Code)
			}
			converted := (&db{}).maybeConvertError(err)
			if code == "53300" || code == "53400" {
				var exhausted *serviceerror.ResourceExhausted
				require.ErrorAs(t, converted, &exhausted)
			} else {
				var unavailable *serviceerror.Unavailable
				require.ErrorAs(t, converted, &unavailable)
			}
		})
	}
}

func TestPoolClose(t *testing.T) {
	entered := make(chan struct{}, 3)
	p := &plugin{rateLimiter: poolTestLimiter(func(ctx context.Context) error {
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})}
	c := testPoolConfig(t, p, &config.SQL{MaxConns: 3})
	sqlDB, connector, err := openPoolDB(c, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	require.Zero(t, sqlDB.Stats().MaxOpenConnections)
	require.Eventually(t, func() bool { return len(entered) == 3 }, time.Second, time.Millisecond)
	// Exercise the plugin -> handle -> sql.DB -> connector ownership chain.
	p.driver = &driver.PGXDriver{}
	generic, err := p.CreateDB(sqlplugin.DbKindMain, &config.SQL{
		Connect: func(*config.SQL) (*sqlx.DB, error) { return sqlDB, nil },
	}, nil, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	closed := make(chan error, 1)
	go func() { closed <- generic.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("close did not cancel background limiter waits")
	}
	_, err = connector.pool.Acquire(t.Context())
	require.ErrorContains(t, err, "closed pool")
	require.Zero(t, connector.pool.Stat().TotalConns())
	collector := connector.metrics.(*dsqlMetricsImpl)
	select {
	case <-collector.poolCollectorDone:
	default:
		t.Fatal("metrics collector outlived Close")
	}
	require.NoError(t, generic.Close())
}

func TestPoolLeaseUnsupported(t *testing.T) {
	t.Setenv(DistributedConnLeaseEnabledEnvVar, "true")
	p := &plugin{}
	_, err := p.createDSQLConnection(&config.SQL{}, nil, log.NewNoopLogger(), nil)
	require.EqualError(t, err, "DSQL connection lease was removed (#33). Size the pools so the fleet's total connections, including the overlap during deploys, fits the cluster's connection limit, then unset DSQL_DISTRIBUTED_CONN_LEASE_ENABLED")
}

func TestPoolWarnings(t *testing.T) {
	core, observed := observer.New(zap.WarnLevel)
	logger := log.NewZapLogger(zap.New(core))
	p := &plugin{}
	t.Setenv("DSQL_RESERVOIR_ENABLED", "")
	t.Setenv("DSQL_POOL_KEEPER_TICK_INTERVAL", "1s")
	retired := []string{
		"DSQL_DISTRIBUTED_CONN_LEASE_TABLE", "DSQL_DISTRIBUTED_CONN_LIMIT",
		"DSQL_SLOT_BLOCK_SIZE", "DSQL_SLOT_BLOCK_COUNT", "DSQL_SLOT_BLOCK_TTL", "DSQL_SLOT_BLOCK_RENEW_INTERVAL",
	}
	for _, name := range retired {
		t.Setenv(name, "")
	}
	for range 2 {
		p.warnIgnoredSettings(logger)
	}
	require.Equal(t, 1, observed.Len())
	fields := observed.All()[0].ContextMap()
	require.Contains(t, fields["ignored_settings"], "DSQL_RESERVOIR_ENABLED")
	require.Contains(t, fields["ignored_settings"], "DSQL_POOL_KEEPER_TICK_INTERVAL")
	for _, name := range retired {
		require.Contains(t, fields["ignored_settings"], name)
	}
	for range 2 {
		_, err := p.poolConfig(&config.SQL{ConnectAddr: "127.0.0.1:5432", MaxConnLifetime: time.Hour}, resolver.NewNoopResolver(), nil, logger)
		require.NoError(t, err)
	}
	require.Equal(t, 2, observed.Len())
}

func TestPoolMetrics(t *testing.T) {
	c := testPoolConfig(t, &plugin{}, &config.SQL{PoolSizeHint: "ephemeral"})
	c.BeforeConnect = func(context.Context, *pgx.ConnConfig) error { return errors.New("test connect failure") }
	sqlDB, connector, err := openPoolDB(c, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	m := NewDSQLMetrics(handler).(*dsqlMetricsImpl)
	for range 2 {
		require.Error(t, sqlDB.PingContext(t.Context()))
		m.recordPoolStats(connector.pool.Stat())
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = connector.pool.Acquire(canceled)
	require.ErrorIs(t, err, context.Canceled)
	m.recordPoolStats(connector.pool.Stat())
	snapshot := capture.Snapshot()
	require.Len(t, snapshot["dsql_pool_new_connections_total"], 2)
	for _, record := range snapshot["dsql_pool_new_connections_total"] {
		require.Equal(t, int64(1), record.Value)
	}
	require.Len(t, snapshot["dsql_pool_canceled_acquires_total"], 1)
	require.Equal(t, int64(1), snapshot["dsql_pool_canceled_acquires_total"][0].Value)
	require.InDelta(t, 5, snapshot["dsql_pool_max_open"][0].Value, 0.001)
	require.InDelta(t, 0, snapshot["dsql_pool_open"][0].Value, 0.001)
	require.InDelta(t, 0, snapshot["dsql_pool_constructing"][0].Value, 0.001)
	m.StartPoolCollector(connector.pool, time.Millisecond)
	m.StartPoolCollector(connector.pool, time.Millisecond)
	require.Eventually(t, func() bool { return len(capture.Snapshot()["dsql_pool_open"]) > 3 }, time.Second, time.Millisecond)
	m.StopPoolCollector()
	m.StopPoolCollector()
	m.StartPoolCollector(connector.pool, time.Millisecond)
	m.StopPoolCollector()
	select {
	case <-m.poolCollectorDone:
	default:
		t.Fatal("collector did not stop")
	}
}

func TestPoolRoleAndToken(t *testing.T) {
	for _, user := range []string{"", "admin", "temporal"} {
		t.Run("user="+user, func(t *testing.T) {
			core, logs := observer.New(zap.InfoLevel)
			logger := log.NewZapLogger(zap.New(core))
			cache := NewTokenCache(&mockCredentialsProvider{}, logger, nil)
			t.Cleanup(cache.Stop)
			p := &plugin{rateLimiter: poolTestLimiter(func(context.Context) error { return nil })}
			cfg := &config.SQL{User: user, Password: "ignored-password", ConnectAddr: "db.example.invalid:5432", DatabaseName: "postgres"}
			expectedUser, action := user, "DbConnect"
			if user == "" || user == "admin" {
				expectedUser, action = "admin", "DbConnectAdmin"
			}
			for range 2 {
				provider := p.connectionTokenProvider(cfg, cache, "db.example.invalid", "us-east-1", logger)
				pool, err := p.poolConfig(cfg, resolver.NewNoopResolver(), provider, logger)
				require.NoError(t, err)
				require.Equal(t, expectedUser, pool.ConnConfig.User)
				require.NotEqual(t, cfg.Password, pool.ConnConfig.Password)
				require.NoError(t, pool.BeforeConnect(t.Context(), pool.ConnConfig))
				parsed, err := url.Parse("https://" + pool.ConnConfig.Password)
				require.NoError(t, err)
				require.Equal(t, action, parsed.Query().Get("Action"))
				require.NotNil(t, cache.cachedToken(poolKey{"db.example.invalid", "us-east-1", expectedUser, GetConfiguredTokenDuration()}))
				require.Equal(t, "ignored-password", cfg.Password)
			}
			require.Equal(t, 1, logs.FilterMessage("DSQL password is ignored; connections use IAM authentication").Len())
			require.Equal(t, expectedUser, logs.FilterMessage("Creating DSQL connection with IAM authentication").All()[0].ContextMap()["user"])
		})
	}
}

func TestPoolTLSWarnings(t *testing.T) {
	for _, mode := range []string{"", "verify-full", "verify-ca", "require", "prefer", "allow", "disable"} {
		t.Run("sslmode="+mode, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			logger := log.NewZapLogger(zap.New(core))
			p := &plugin{}
			cfg := &config.SQL{ConnectAddr: "db.example.invalid:5432", DatabaseName: "postgres"}
			if mode != "" {
				cfg.ConnectAttributes = map[string]string{" sslmode ": " " + mode + " "}
			}
			for range 2 {
				pool, err := p.poolConfig(cfg, resolver.NewNoopResolver(), nil, logger)
				require.NoError(t, err)
				if mode == "" || mode == "verify-full" {
					require.NotNil(t, pool.ConnConfig.TLSConfig)
					require.False(t, pool.ConnConfig.TLSConfig.InsecureSkipVerify)
					require.Equal(t, "db.example.invalid", pool.ConnConfig.TLSConfig.ServerName)
				}
				if mode == "disable" {
					require.Nil(t, pool.ConnConfig.TLSConfig)
				}
				if mode == "require" {
					require.True(t, pool.ConnConfig.TLSConfig.InsecureSkipVerify)
				}
			}
			if mode == "" || mode == "verify-full" {
				require.Zero(t, logs.Len())
			} else {
				require.Equal(t, 1, logs.Len())
				require.Contains(t, logs.All()[0].Message, "not fully verified")
				require.Equal(t, mode, logs.All()[0].ContextMap()["sslmode"])
			}
		})
	}
}
