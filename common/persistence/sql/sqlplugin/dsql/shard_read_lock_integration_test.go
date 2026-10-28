//go:build integration

package dsql_test

import (
	"context"
	"database/sql"
	"errors"
	"math/rand/v2"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/resolver"
)

// TestReadLockShardsForKeyShareIntegration checks the ownership fence and concurrent
// readers against a DSQL cluster with the Temporal schema already installed.
// DSQL_TEST_ENDPOINT is the cluster hostname; credentials use the default AWS chain.
// Run from the repository root:
//
//	DSQL_TEST_ENDPOINT='<cluster-hostname>' go test -tags integration,disable_grpc_modules,test_dep ./common/persistence/sql/sqlplugin/dsql -run '^TestReadLockShardsForKeyShareIntegration$' -count=1 -timeout=5m -v
func TestReadLockShardsForKeyShareIntegration(t *testing.T) {
	endpoint := os.Getenv("DSQL_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DSQL_TEST_ENDPOINT to run against a live DSQL cluster")
	}

	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, &config.SQL{
		PluginName:      dsql.PluginName,
		DatabaseName:    "postgres",
		ConnectAddr:     net.JoinHostPort(endpoint, "5432"),
		ConnectProtocol: "tcp",
		MaxConns:        3,
		MaxIdleConns:    3,
		PoolSizeHint:    "ephemeral",
		TLS:             &auth.TLS{Enabled: true, EnableHostVerification: true},
	}, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, ok := db.(sqlplugin.Conn)
	require.True(t, ok)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	// Random high IDs avoid real shards and allow independent maintainer runs.
	baseID := int32(1_000_000_000) + rand.Int32N(1_000_000_000)
	shardIDs := [3]int32{baseID, baseID + 1, baseID + 2}
	for _, shardID := range shardIDs {
		_, err := db.InsertIntoShards(ctx, &sqlplugin.ShardsRow{
			ShardID: shardID, RangeID: 1, Data: []byte("key-share-test"), DataEncoding: "Proto3",
		})
		require.NoError(t, err)
		// Only delete rows successfully inserted by this run, even after partial setup.
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
			defer cleanupCancel()
			_, err := conn.ExecContext(cleanupCtx, "DELETE FROM shards WHERE shard_id = $1", shardID)
			require.NoError(t, err)
		})
	}

	beginTx := func(t *testing.T) sqlplugin.Tx {
		t.Helper()
		tx, err := db.BeginTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() {
			if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
				require.NoError(t, err)
			}
		})
		return tx
	}
	updateShard := func(t *testing.T, tx sqlplugin.Tx, shardID int32, rangeID int64) {
		t.Helper()
		result, err := tx.UpdateShards(ctx, &sqlplugin.ShardsRow{
			ShardID: shardID, RangeID: rangeID, Data: []byte("key-share-updated"), DataEncoding: "Proto3",
		})
		require.NoError(t, err)
		rows, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rows)
	}
	filter := sqlplugin.ShardsFilter{ShardID: shardIDs[0]}

	t.Run("fence", func(t *testing.T) {
		txA := beginTx(t)
		rangeID, err := txA.ReadLockShards(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(1), rangeID)
		updateShard(t, txA, shardIDs[1], 2)

		txB := beginTx(t)
		rangeID, err = txB.WriteLockShards(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(1), rangeID)
		updateShard(t, txB, shardIDs[0], rangeID+1)
		require.NoError(t, txB.Commit())

		err = txA.Commit()
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "40001", pgErr.SQLState())
	})

	t.Run("readers_do_not_conflict", func(t *testing.T) {
		txA := beginTx(t)
		rangeA, err := txA.ReadLockShards(ctx, filter)
		require.NoError(t, err)
		updateShard(t, txA, shardIDs[1], 3)

		txB := beginTx(t)
		rangeB, err := txB.ReadLockShards(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, rangeA, rangeB)
		updateShard(t, txB, shardIDs[2], 3)

		require.NoError(t, txB.Commit())
		require.NoError(t, txA.Commit())
	})
}
