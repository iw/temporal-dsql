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

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/resolver"
)

// TestDSQLQuotasIntegration checks bounded deletes and typed column-size errors
// against a DSQL cluster with the Temporal schema already installed.
// DSQL_TEST_ENDPOINT is the cluster hostname; credentials use the default AWS chain.
// Run from the repository root:
//
//	DSQL_TEST_ENDPOINT='<cluster-hostname>' go test -tags integration,disable_grpc_modules,test_dep ./common/persistence/sql/sqlplugin/dsql -run '^TestDSQLQuotasIntegration$' -count=1 -timeout=5m -v
func TestDSQLQuotasIntegration(t *testing.T) {
	endpoint := os.Getenv("DSQL_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DSQL_TEST_ENDPOINT to run against a live DSQL cluster")
	}

	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, &config.SQL{
		PluginName: dsql.PluginName, DatabaseName: "postgres",
		ConnectAddr: net.JoinHostPort(endpoint, "5432"), ConnectProtocol: "tcp",
		MaxConns: 3, MaxIdleConns: 3, PoolSizeHint: "ephemeral",
		TLS: &auth.TLS{Enabled: true, EnableHostVerification: true},
	}, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, ok := db.(sqlplugin.Conn)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	shardID := int32(1_000_000_001) + rand.Int32N(999_999_999)
	_, err = db.InsertIntoShards(ctx, &sqlplugin.ShardsRow{
		ShardID: shardID, RangeID: 1, Data: []byte("quota-test"), DataEncoding: "Proto3",
	})
	require.NoError(t, err)
	// Reserve the shard before writing tasks so cleanup cannot touch another run.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cleanupCancel()
		_, err := conn.ExecContext(cleanupCtx, "DELETE FROM shards WHERE shard_id = $1", shardID)
		require.NoError(t, err)
	})
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cleanupCancel()
		// Keep cleanup bounded even if the range-delete implementation fails.
		for first := int64(1); first <= 3500; first += 1000 {
			_, err := conn.ExecContext(cleanupCtx,
				"DELETE FROM transfer_tasks WHERE shard_id = $1 AND task_id >= $2 AND task_id < $3",
				shardID, first, min(first+1000, 3501))
			require.NoError(t, err)
		}
	})

	t.Run("transfer_tasks", func(t *testing.T) {
		for first := int64(1); first <= 3500; first += 1000 {
			func() {
				tx, err := db.BeginTx(ctx)
				require.NoError(t, err)
				defer func() {
					if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
						require.NoError(t, err)
					}
				}()
				var rows []sqlplugin.TransferTasksRow
				for id := first; id < min(first+1000, 3501); id++ {
					rows = append(rows, sqlplugin.TransferTasksRow{
						ShardID: shardID, TaskID: id, Data: []byte("task"), DataEncoding: "Proto3",
					})
				}
				_, err = tx.InsertIntoTransferTasks(ctx, rows)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
			}()
		}
		result, err := db.RangeDeleteFromTransferTasks(ctx, sqlplugin.TransferTasksRangeFilter{
			ShardID: shardID, InclusiveMinTaskID: 1, ExclusiveMaxTaskID: 3501,
		})
		require.NoError(t, err)
		deleted, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(3500), deleted)
		var remaining int64
		require.NoError(t, conn.GetContext(ctx, &remaining, "SELECT COUNT(*) FROM transfer_tasks WHERE shard_id = $1", shardID))
		require.Zero(t, remaining)
	})

	t.Run("oversized_column", func(t *testing.T) {
		tx, err := db.BeginTx(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, tx.Rollback()) }()
		_, err = tx.UpdateShards(ctx, &sqlplugin.ShardsRow{
			ShardID: shardID, RangeID: 2, Data: make([]byte, 1024*1024+1), DataEncoding: "Proto3",
		})
		var sizeErr *persistence.TransactionSizeLimitError
		require.ErrorAs(t, err, &sizeErr)
		require.False(t, persistence.OperationPossiblySucceeded(err))
	})
}
