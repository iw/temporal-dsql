package dsql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
)

func newQuotaTestConfig(t *testing.T, pluginName string) (*config.SQL, *shardTestDriver) {
	t.Helper()
	db, fake := newShardTestDB(t, 7)
	// A branch cleanup must work even when the pool has only one connection.
	db.SetMaxOpenConns(1)
	return &config.SQL{
		PluginName: pluginName, DatabaseName: "quota_test",
		Connect: func(*config.SQL) (*sqlx.DB, error) { return db, nil },
	}, fake
}

func newQuotaTestDB(t *testing.T) (sqlplugin.DB, *shardTestDriver) {
	t.Helper()
	cfg, fake := newQuotaTestConfig(t, dsql.PluginName)
	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, cfg,
		resolver.NewNoopResolver(), log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, fake
}

func newQuotaTestFactory(t *testing.T, pluginName string) (*persistencesql.Factory, *shardTestDriver) {
	t.Helper()
	cfg, fake := newQuotaTestConfig(t, pluginName)
	factory := persistencesql.NewFactory(*cfg, nil, "test", log.NewNoopLogger(),
		metrics.NoopMetricsHandler, serialization.NewSerializer())
	t.Cleanup(factory.Close)
	return factory, fake
}

func TestTaskRangeDeleteQuotas(t *testing.T) {
	start := time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	for _, tc := range []struct {
		table string
		keys  string
		args  []driver.Value
		run   func(context.Context, sqlplugin.DB) (sql.Result, error)
	}{
		{"history_immediate_tasks", "shard_id, category_id, task_id", []driver.Value{int64(7), int64(4), int64(10), int64(9000)},
			func(ctx context.Context, db sqlplugin.DB) (sql.Result, error) {
				return db.RangeDeleteFromHistoryImmediateTasks(ctx, sqlplugin.HistoryImmediateTasksRangeFilter{
					ShardID: 7, CategoryID: 4, InclusiveMinTaskID: 10, ExclusiveMaxTaskID: 9000})
			}},
		{"history_scheduled_tasks", "shard_id, category_id, visibility_timestamp, task_id", []driver.Value{int64(7), int64(4), start, end},
			func(ctx context.Context, db sqlplugin.DB) (sql.Result, error) {
				return db.RangeDeleteFromHistoryScheduledTasks(ctx, sqlplugin.HistoryScheduledTasksRangeFilter{
					ShardID: 7, CategoryID: 4, InclusiveMinVisibilityTimestamp: start, ExclusiveMaxVisibilityTimestamp: end})
			}},
		{"transfer_tasks", "shard_id, task_id", []driver.Value{int64(7), int64(10), int64(9000)},
			func(ctx context.Context, db sqlplugin.DB) (sql.Result, error) {
				return db.RangeDeleteFromTransferTasks(ctx, sqlplugin.TransferTasksRangeFilter{
					ShardID: 7, InclusiveMinTaskID: 10, ExclusiveMaxTaskID: 9000})
			}},
		{"timer_tasks", "shard_id, visibility_timestamp, task_id", []driver.Value{int64(7), start, end},
			func(ctx context.Context, db sqlplugin.DB) (sql.Result, error) {
				return db.RangeDeleteFromTimerTasks(ctx, sqlplugin.TimerTasksRangeFilter{
					ShardID: 7, InclusiveMinVisibilityTimestamp: start, ExclusiveMaxVisibilityTimestamp: end})
			}},
		{"replication_tasks", "shard_id, task_id", []driver.Value{int64(7), int64(10), int64(9000)},
			func(ctx context.Context, db sqlplugin.DB) (sql.Result, error) {
				return db.RangeDeleteFromReplicationTasks(ctx, sqlplugin.ReplicationTasksRangeFilter{
					ShardID: 7, InclusiveMinTaskID: 10, ExclusiveMaxTaskID: 9000})
			}},
		{"replication_tasks_dlq", "source_cluster_name, shard_id, task_id", []driver.Value{"source", int64(7), int64(10), int64(9000)},
			func(ctx context.Context, db sqlplugin.DB) (sql.Result, error) {
				return db.RangeDeleteFromReplicationDLQTasks(ctx, sqlplugin.ReplicationDLQTasksRangeFilter{
					SourceClusterName: "source", ShardID: 7, InclusiveMinTaskID: 10, ExclusiveMaxTaskID: 9000})
			}},
		{"visibility_tasks", "shard_id, task_id", []driver.Value{int64(7), int64(10), int64(9000)},
			func(ctx context.Context, db sqlplugin.DB) (sql.Result, error) {
				return db.RangeDeleteFromVisibilityTasks(ctx, sqlplugin.VisibilityTasksRangeFilter{
					ShardID: 7, InclusiveMinTaskID: 10, ExclusiveMaxTaskID: 9000})
			}},
	} {
		t.Run(tc.table, func(t *testing.T) {
			for _, rowBytes := range []int64{100, 200 * 1024} {
				t.Run(fmt.Sprint(rowBytes), func(t *testing.T) {
					db, fake := newQuotaTestDB(t)
					fake.tables = map[string]*quotaTestTable{tc.table: {rows: 7500, rowBytes: rowBytes}}
					result, err := tc.run(t.Context(), db)
					require.NoError(t, err)
					total, err := result.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, int64(7500), total)
					require.Zero(t, fake.tables[tc.table].rows)
					require.Zero(t, fake.beginCount())
					statements, _, _ := fake.snapshot()
					limit := int64(1000)
					for i, attempt := range fake.deletes {
						require.Equal(t, limit, attempt.limit)
						require.LessOrEqual(t, attempt.rows, int64(1000))
						require.Contains(t, statements[i], "WHERE ("+tc.keys+") IN ( SELECT "+tc.keys)
						require.Contains(t, statements[i], "ORDER BY "+tc.keys)
						require.Equal(t, append(append([]driver.Value(nil), tc.args...), limit), fake.statementArgs(i))
						if attempt.exceeded {
							limit /= 2
						}
					}
					if rowBytes == 100 {
						require.Len(t, statements, 8)
					} else {
						require.Less(t, limit, int64(1000))
					}
				})
			}
		})
	}
}

func TestRangeDeleteQuotaFailures(t *testing.T) {
	for _, history := range []bool{false, true} {
		for _, quota := range []bool{false, true} {
			t.Run(fmt.Sprintf("history=%v/quota=%v", history, quota), func(t *testing.T) {
				db, fake := newQuotaTestDB(t)
				failure := error(errors.New("delete failed"))
				limits := []int64{1000, 500, 250, 125, 62, 31, 15, 7, 3, 1}
				if history {
					limits = []int64{100, 50, 25, 12, 6, 3, 1}
				}
				if quota {
					failure = fmt.Errorf("wrapped quota: %w", &pgconn.PgError{Code: "54000"})
				} else {
					limits = limits[:1]
				}
				for range limits {
					fake.execErrors = append(fake.execErrors, failure)
				}
				var err error
				if history {
					_, err = db.RangeDeleteFromHistoryNode(t.Context(), sqlplugin.HistoryNodeDeleteFilter{
						ShardID: 7, TreeID: primitives.NewUUID(), BranchID: primitives.NewUUID(), MinNodeID: 1})
				} else {
					_, err = db.RangeDeleteFromTransferTasks(t.Context(), sqlplugin.TransferTasksRangeFilter{
						ShardID: 7, InclusiveMinTaskID: 1, ExclusiveMaxTaskID: 2})
				}
				if quota {
					var sizeErr *persistence.TransactionSizeLimitError
					require.ErrorAs(t, err, &sizeErr)
					require.False(t, persistence.OperationPossiblySucceeded(err))
				} else {
					require.ErrorIs(t, err, failure)
				}
				statements, _, _ := fake.snapshot()
				require.Len(t, statements, len(limits))
				for i, limit := range limits {
					args := fake.statementArgs(i)
					require.Equal(t, limit, args[len(args)-1])
				}
			})
		}
	}
}

func TestDeleteHistoryBranchQuotas(t *testing.T) {
	for _, pluginName := range []string{dsql.PluginName, postgresql.PluginName} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fail=%v", pluginName, fail), func(t *testing.T) {
				factory, fake := newQuotaTestFactory(t, pluginName)
				store, err := factory.NewExecutionStore()
				require.NoError(t, err)
				t.Cleanup(store.Close)
				nodes := int64(10)
				if pluginName == dsql.PluginName {
					nodes = 3500
				}
				fake.tables = map[string]*quotaTestTable{
					"history_node": {rows: nodes, rowBytes: 200 * 1024},
					"history_tree": {rows: 1, rowBytes: 100},
				}
				request := &persistence.InternalDeleteHistoryBranchRequest{
					ShardID:    7,
					BranchInfo: &persistencespb.HistoryBranch{TreeId: primitives.NewUUID().String(), BranchId: primitives.NewUUID().String()},
				}
				request.BranchRanges = []persistence.InternalDeleteHistoryBranchRange{
					{BranchId: request.BranchInfo.BranchId, BeginNodeId: 10},
					{BranchId: primitives.NewUUID().String(), BeginNodeId: 20},
				}
				if fail {
					fake.execErrors = []error{nil, errors.New("node delete failed")}
					if pluginName == dsql.PluginName {
						// Allow the initial 54000 and a successful smaller batch first.
						fake.execErrors = []error{nil, nil, errors.New("node delete failed")}
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				err = store.DeleteHistoryBranch(ctx, request)
				if fail {
					require.ErrorContains(t, err, "node delete failed")
					require.Equal(t, int64(1), fake.tables["history_tree"].rows)
					statements, _, _ := fake.snapshot()
					for _, statement := range statements {
						require.True(t, strings.HasPrefix(statement, "DELETE FROM history_node "))
					}
					// A retry finishes partially deleted branches using the retained tree row.
					require.NoError(t, store.DeleteHistoryBranch(ctx, request))
				} else {
					require.NoError(t, err)
				}
				require.Zero(t, fake.beginCount())
				require.Zero(t, fake.tables["history_node"].rows)
				require.Zero(t, fake.tables["history_tree"].rows)
				statements, _, _ := fake.snapshot()
				require.True(t, strings.HasPrefix(statements[len(statements)-1], "DELETE FROM history_tree "))
				for _, statement := range statements[:len(statements)-1] {
					require.True(t, strings.HasPrefix(statement, "DELETE FROM history_node "))
				}
				if pluginName == dsql.PluginName {
					require.Equal(t, quotaTestDelete{"history_node", 100, 100, true}, fake.deletes[0])
					require.Equal(t, quotaTestDelete{"history_node", 50, 50, false}, fake.deletes[1])
					require.Contains(t, statements[0], "WHERE (shard_id, tree_id, branch_id, node_id, txn_id) IN ( SELECT shard_id, tree_id, branch_id, node_id, txn_id")
					require.Equal(t, []driver.Value{int64(7), request.BranchInfo.TreeId, request.BranchInfo.BranchId, int64(10), int64(100)}, fake.statementArgs(0))
				}
				require.NoError(t, store.DeleteHistoryBranch(ctx, request), "deletion is idempotent")
			})
		}
	}
}

func TestStoreTransactionSizeLimitError(t *testing.T) {
	for _, operation := range []string{"UpdateShard", "AppendHistoryNodesExistingBranch", "AppendHistoryNodesNewBranch"} {
		t.Run(operation, func(t *testing.T) {
			factory, fake := newQuotaTestFactory(t, dsql.PluginName)
			data := persistence.NewDataBlob(make([]byte, 1024*1024+1), "Proto3")
			var err error
			if operation == "UpdateShard" {
				store, createErr := factory.NewShardStore()
				require.NoError(t, createErr)
				t.Cleanup(store.Close)
				err = store.UpdateShard(t.Context(), &persistence.InternalUpdateShardRequest{
					ShardID: 7, PreviousRangeID: 7, RangeID: 8, ShardInfo: data})
			} else {
				store, createErr := factory.NewExecutionStore()
				require.NoError(t, createErr)
				t.Cleanup(store.Close)
				err = store.AppendHistoryNodes(t.Context(), &persistence.InternalAppendHistoryNodesRequest{
					ShardID: 7, IsNewBranch: operation == "AppendHistoryNodesNewBranch",
					BranchInfo: &persistencespb.HistoryBranch{TreeId: primitives.NewUUID().String(), BranchId: primitives.NewUUID().String()},
					Node:       persistence.InternalHistoryNode{NodeID: 1, TransactionID: 1, Events: data},
					TreeInfo:   persistence.NewDataBlob([]byte("tree"), "Proto3"),
				})
			}
			var sizeErr *persistence.TransactionSizeLimitError
			require.ErrorAs(t, err, &sizeErr)
			require.Contains(t, sizeErr.Msg, "54000")
			require.False(t, persistence.OperationPossiblySucceeded(err))
			_, commits, rollbacks := fake.snapshot()
			require.Zero(t, commits)
			if operation == "AppendHistoryNodesExistingBranch" {
				require.Zero(t, fake.beginCount())
			} else {
				require.Equal(t, 1, fake.beginCount())
				require.Equal(t, 1, rollbacks)
			}
		})
	}
}
