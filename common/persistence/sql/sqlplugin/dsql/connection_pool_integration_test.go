//go:build integration

package dsql

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
	"go.temporal.io/server/common/resolver"
)

func TestDSQLPoolIntegration(t *testing.T) {
	testDSQLPoolIntegration(t, "")
}

func TestDSQLPoolRoleIntegration(t *testing.T) {
	user := os.Getenv("DSQL_TEST_USER")
	if user == "" {
		t.Skip("set DSQL_TEST_USER to test an IAM-mapped database role")
	}
	testDSQLPoolIntegration(t, user)
}

func testDSQLPoolIntegration(t *testing.T, user string) {
	t.Helper()
	endpoint := os.Getenv("DSQL_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DSQL_TEST_ENDPOINT to run against a live DSQL cluster")
	}
	region := os.Getenv("AWS_REGION")
	require.NotEmpty(t, region, "AWS_REGION must be set")
	logger := log.NewTestLogger()
	p := &plugin{driver: &driver.PGXDriver{}, rateLimiter: NewConnectionRateLimiter()}
	cache, err := p.getOrInitTokenCache(t.Context(), region, logger, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	cfg := &config.SQL{
		DatabaseName: "postgres", ConnectAddr: net.JoinHostPort(endpoint, "5432"), MaxConns: 3,
		User: user,
	}
	t.Cleanup(cache.Stop)
	provider := p.connectionTokenProvider(cfg, cache, endpoint, region, logger)
	c, err := p.poolConfig(cfg, resolver.NewNoopResolver(), provider, logger)
	require.NoError(t, err)
	sqlDB, connector, err := openPoolDB(c, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	cfg.Connect = func(*config.SQL) (*sqlx.DB, error) { return sqlDB, nil }
	pluginDB, err := p.CreateDB(sqlplugin.DbKindMain, cfg, nil, logger, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pluginDB.Close()) })
	require.Eventually(t, func() bool {
		stats := connector.pool.Stat()
		return stats.TotalConns() == 3 && stats.ConstructingConns() == 0
	}, 2*time.Minute, 20*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var value int
	require.NoError(t, pluginDB.(*db).GetContext(ctx, &value, "SELECT 1"))
	require.Equal(t, 1, value)
	var connectedUser string
	require.NoError(t, pluginDB.(*db).GetContext(ctx, &connectedUser, "SELECT current_user"))
	require.Equal(t, dsqlUser(cfg), connectedUser)
	require.NoError(t, pluginDB.Close())
	_, err = connector.pool.Acquire(ctx)
	require.ErrorContains(t, err, "closed pool")
	require.Zero(t, connector.pool.Stat().TotalConns())
}
