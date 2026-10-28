//go:build integration

package dsql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
)

// Run with DSQL_TEST_ENDPOINT set, using the default AWS credential
// chain and a cluster with the Temporal schema installed:
//
// go test -tags integration,disable_grpc_modules,test_dep ./common/persistence/sql/sqlplugin/dsql -run '^TestDSQL(ExecutionMaps|Queues)Integration$' -count=1 -timeout=5m -v
func TestDSQLExecutionMapsIntegration(t *testing.T) {
	db, _ := newBehaviorIntegrationDB(t)
	conn, ok := db.(sqlplugin.Conn)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	shardID := int32(1_000_000_001) + rand.Int32N(999_999_999)
	_, err := db.InsertIntoShards(ctx, &sqlplugin.ShardsRow{
		ShardID: shardID, RangeID: 1, Data: []byte("map-test"), DataEncoding: "Proto3",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		_, err := conn.ExecContext(ctx, "DELETE FROM shards WHERE shard_id = $1", shardID)
		require.NoError(t, err)
	})
	namespaceID, runID := primitives.NewUUID(), primitives.NewUUID()
	workflowID := "map-test-" + primitives.NewUUID().String()
	for _, table := range []string{"activity_info_maps", "chasm_node_maps"} {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
			defer cancel()
			_, err := conn.ExecContext(ctx, "DELETE FROM "+table+" WHERE shard_id = $1 AND namespace_id = $2 AND workflow_id = $3 AND run_id = $4",
				shardID, namespaceID.String(), workflowID, runID.String())
			require.NoError(t, err)
		})
	}
	for _, phase := range []string{"insert", "replace"} {
		t.Run(phase, func(t *testing.T) {
			var activities []sqlplugin.ActivityInfoMapsRow
			var nodes []sqlplugin.ChasmNodeMapsRow
			for i := range 3 {
				activities = append(activities, sqlplugin.ActivityInfoMapsRow{
					ShardID: shardID, NamespaceID: namespaceID, WorkflowID: workflowID, RunID: runID,
					ScheduleID: int64(i + 1), Data: []byte(fmt.Sprintf("%s-activity-%d", phase, i)), DataEncoding: "Proto3",
				})
				nodes = append(nodes, sqlplugin.ChasmNodeMapsRow{
					ShardID: shardID, NamespaceID: namespaceID, WorkflowID: workflowID, RunID: runID,
					ChasmPath: fmt.Sprintf("node-%d", i), Metadata: []byte(phase + "-metadata"), MetadataEncoding: "Proto3",
					Data: []byte(fmt.Sprintf("%s-node-%d", phase, i)), DataEncoding: "Proto3",
				})
			}
			tx, err := db.BeginTx(ctx)
			require.NoError(t, err)
			defer func() {
				if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
					require.NoError(t, err)
				}
			}()
			result, err := tx.ReplaceIntoActivityInfoMaps(ctx, activities)
			require.NoError(t, err)
			affected, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(3), affected)
			result, err = tx.ReplaceIntoChasmNodeMaps(ctx, nodes)
			require.NoError(t, err)
			affected, err = result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(3), affected)
			require.NoError(t, tx.Commit())
			gotActivities, err := db.SelectAllFromActivityInfoMaps(ctx, sqlplugin.ActivityInfoMapsAllFilter{
				ShardID: shardID, NamespaceID: namespaceID, WorkflowID: workflowID, RunID: runID,
			})
			require.NoError(t, err)
			require.ElementsMatch(t, activities, gotActivities)
			gotNodes, err := db.SelectAllFromChasmNodeMaps(ctx, sqlplugin.ChasmNodeMapsAllFilter{
				ShardID: shardID, NamespaceID: namespaceID, WorkflowID: workflowID, RunID: runID,
			})
			require.NoError(t, err)
			require.ElementsMatch(t, nodes, gotNodes)
		})
	}
}

func newBehaviorIntegrationDB(t *testing.T) (sqlplugin.DB, config.SQL) {
	t.Helper()
	endpoint := os.Getenv("DSQL_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DSQL_TEST_ENDPOINT to run against a live DSQL cluster")
	}
	cfg := config.SQL{
		PluginName: dsql.PluginName, DatabaseName: "postgres",
		ConnectAddr: net.JoinHostPort(endpoint, "5432"), ConnectProtocol: "tcp",
		MaxConns: 3, MaxIdleConns: 3, PoolSizeHint: "ephemeral",
		TLS: &auth.TLS{Enabled: true, EnableHostVerification: true},
	}
	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, &cfg,
		resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, cfg
}
