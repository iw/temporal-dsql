package dsql_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
)

func TestHistoryTreeUUIDPagination(t *testing.T) {
	db, fake := newUUIDTestDB(t)
	for i, key := range [][3]int{{0, 1, 1}, {0, 1, 2}, {0, 1, 3}, {0, 2, 1}, {0, 2, 2}, {1, 1, 1}, {1, 1, 2}} {
		fake.historyTrees = append(fake.historyTrees, sqlplugin.HistoryTreeRow{
			ShardID: int32(key[0]), TreeID: uuidTestID(key[1]), BranchID: uuidTestID(key[2]),
			Data: []byte(fmt.Sprintf("branch-%d", i)), DataEncoding: "Proto3",
		})
	}
	page := sqlplugin.HistoryTreeBranchPage{Limit: 2}
	var got []sqlplugin.HistoryTreeRow
	for offset := 0; offset < len(fake.historyTrees); offset += page.Limit {
		var rows []sqlplugin.HistoryTreeRow
		require.NotPanics(t, func() {
			var err error
			rows, err = db.PaginateBranchesFromHistoryTree(t.Context(), page)
			require.NoError(t, err)
		}, "page starting at row %d", offset)
		if offset == 0 {
			args := fake.statementArgs(0)
			require.Equal(t, dsql.NilUUID, args[1])
			require.Equal(t, dsql.NilUUID, args[2])
		}
		require.Equal(t, fake.historyTrees[offset:min(offset+page.Limit, len(fake.historyTrees))], rows)
		for _, row := range rows {
			require.Len(t, row.TreeID, 16)
			require.Len(t, row.BranchID, 16)
		}
		got = append(got, rows...)
		last := rows[len(rows)-1]
		page.ShardID, page.TreeID, page.BranchID = last.ShardID, last.TreeID, last.BranchID
	}
	require.Equal(t, fake.historyTrees, got)
	rows, err := db.PaginateBranchesFromHistoryTree(t.Context(), page)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestHistoryTreeUUIDPointRead(t *testing.T) {
	db, fake := newUUIDTestDB(t)
	fake.historyTrees = []sqlplugin.HistoryTreeRow{{
		ShardID: 7, TreeID: uuidTestID(1), BranchID: uuidTestID(2), Data: []byte("branch"), DataEncoding: "Proto3",
	}}
	rows, err := db.SelectFromHistoryTree(t.Context(), sqlplugin.HistoryTreeSelectFilter{ShardID: 7, TreeID: uuidTestID(1)})
	require.NoError(t, err)
	require.Equal(t, []sqlplugin.HistoryTreeRow{{BranchID: uuidTestID(2), Data: []byte("branch"), DataEncoding: "Proto3"}}, rows)
	require.Len(t, rows[0].BranchID, 16)
}

func TestTaskQueueUUIDPagination(t *testing.T) {
	for _, version := range []sqlplugin.MatchingTaskVersion{sqlplugin.MatchingTaskVersion1, sqlplugin.MatchingTaskVersion2} {
		for _, hashRange := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/hash_range=%t", version, hashRange), func(t *testing.T) {
				db, fake := newUUIDTestDB(t)
				var want []sqlplugin.TaskQueuesRow
				for i := range 7 {
					logicalID := []byte(fmt.Sprintf("namespace/task-queue-%d/type/subqueue", i))
					want = append(want, sqlplugin.TaskQueuesRow{
						RangeHash: 42, TaskQueueID: primitives.MustParseUUID(dsql.TaskQueueIDToUUIDString(logicalID)),
						RangeID: int64(i + 1), Data: logicalID, DataEncoding: "Proto3",
					})
				}
				slices.SortFunc(want, func(a, b sqlplugin.TaskQueuesRow) int { return bytes.Compare(a.TaskQueueID, b.TaskQueueID) })
				table := "task_queues"
				if version == sqlplugin.MatchingTaskVersion2 {
					table = "task_queues_v2"
				}
				fake.taskQueues = map[string][]sqlplugin.TaskQueuesRow{table: want}
				pageSize := 2
				filter := sqlplugin.TaskQueuesFilter{RangeHash: 42, TaskQueueIDGreaterThan: []byte{}, PageSize: &pageSize}
				if hashRange {
					filter = sqlplugin.TaskQueuesFilter{RangeHashGreaterThanEqualTo: 40, RangeHashLessThanEqualTo: 50, PageSize: &pageSize}
				}
				var got []sqlplugin.TaskQueuesRow
				for offset := 0; offset < len(want); offset += pageSize {
					rows, err := db.SelectFromTaskQueues(t.Context(), filter, version)
					require.NoError(t, err)
					if offset == 0 {
						args := fake.statementArgs(0)
						require.Equal(t, dsql.NilUUID, args[len(args)-2])
					}
					require.Equal(t, want[offset:min(offset+pageSize, len(want))], rows)
					for _, row := range rows {
						require.Len(t, row.TaskQueueID, 16)
					}
					got = append(got, rows...)
					filter.TaskQueueIDGreaterThan = rows[len(rows)-1].TaskQueueID
				}
				require.Equal(t, want, got)
				rows, err := db.SelectFromTaskQueues(t.Context(), filter, version)
				require.NoError(t, err)
				require.Empty(t, rows)

				// Point lookups still hash the logical ID, while returned IDs are stored UUIDs.
				for _, row := range want {
					rows, err := db.SelectFromTaskQueues(t.Context(), sqlplugin.TaskQueuesFilter{
						RangeHash: row.RangeHash, TaskQueueID: row.Data,
					}, version)
					require.NoError(t, err)
					require.Equal(t, []sqlplugin.TaskQueuesRow{row}, rows)
				}
			})
		}
	}
}

func TestClusterMembershipUUIDPagination(t *testing.T) {
	db, fake := newUUIDTestDB(t)
	start := time.Date(2026, time.October, 1, 1, 2, 3, 0, time.UTC)
	for i := range 7 {
		fake.clusterMembers = append(fake.clusterMembers, sqlplugin.ClusterMembershipRow{
			HostID: uuidTestID(i + 1), RPCAddress: fmt.Sprintf("192.0.2.%d", i+1), RPCPort: uint16(7233 + i), Role: persistence.History,
			SessionStart: start, LastHeartbeat: start.Add(time.Minute), RecordExpiry: start.Add(time.Hour),
		})
	}
	filter := &sqlplugin.ClusterMembershipFilter{MaxRecordCount: 2}
	var got []sqlplugin.ClusterMembershipRow
	for offset := 0; offset < len(fake.clusterMembers); offset += filter.MaxRecordCount {
		rows, err := db.GetClusterMembers(t.Context(), filter)
		require.NoError(t, err)
		require.Equal(t, fake.clusterMembers[offset:min(offset+filter.MaxRecordCount, len(fake.clusterMembers))], rows)
		for _, row := range rows {
			require.Len(t, row.HostID, 16)
			_, err := uuid.FromBytes(row.HostID)
			require.NoError(t, err)
		}
		got = append(got, rows...)
		filter.HostIDGreaterThan = rows[len(rows)-1].HostID
	}
	require.Equal(t, fake.clusterMembers, got)
	rows, err := db.GetClusterMembers(t.Context(), filter)
	require.NoError(t, err)
	require.Empty(t, rows)
	for _, row := range fake.clusterMembers {
		rows, err := db.GetClusterMembers(t.Context(), &sqlplugin.ClusterMembershipFilter{HostIDEquals: row.HostID})
		require.NoError(t, err)
		require.Equal(t, []sqlplugin.ClusterMembershipRow{row}, rows)
	}
}

func TestUUIDReadErrors(t *testing.T) {
	for _, operation := range []string{"history_page", "history_point", "task_page_v1", "task_point_v1", "task_page_v2", "task_point_v2", "membership"} {
		t.Run(operation, func(t *testing.T) {
			db, fake := newUUIDTestDB(t)
			queryError := &pgconn.PgError{Code: "40001", Message: "read conflict"}
			fake.queryErrors = []error{queryError}
			var err error
			switch operation {
			case "history_page":
				_, err = db.PaginateBranchesFromHistoryTree(t.Context(), sqlplugin.HistoryTreeBranchPage{TreeID: uuidTestID(1), BranchID: uuidTestID(1), Limit: 2})
			case "history_point":
				_, err = db.SelectFromHistoryTree(t.Context(), sqlplugin.HistoryTreeSelectFilter{TreeID: uuidTestID(1)})
			case "task_page_v1", "task_page_v2", "task_point_v1", "task_point_v2":
				version := sqlplugin.MatchingTaskVersion1
				if operation == "task_page_v2" || operation == "task_point_v2" {
					version = sqlplugin.MatchingTaskVersion2
				}
				pageSize := 2
				filter := sqlplugin.TaskQueuesFilter{RangeHash: 42, TaskQueueIDGreaterThan: []byte{}, PageSize: &pageSize}
				if operation == "task_point_v1" || operation == "task_point_v2" {
					filter = sqlplugin.TaskQueuesFilter{RangeHash: 42, TaskQueueID: []byte("logical-id")}
				}
				_, err = db.SelectFromTaskQueues(t.Context(), filter, version)
			case "membership":
				_, err = db.GetClusterMembers(t.Context(), &sqlplugin.ClusterMembershipFilter{})
			default:
				t.Fatalf("unknown operation: %s", operation)
			}
			require.ErrorIs(t, err, queryError)
		})
	}
}

func uuidTestID(n int) primitives.UUID {
	return primitives.MustParseUUID(fmt.Sprintf("%08x-0000-4000-8000-000000000000", n))
}

func newUUIDTestDB(t *testing.T) (sqlplugin.DB, *shardTestDriver) {
	t.Helper()
	fakeDB, fake := newShardTestDB(t, 0)
	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, &config.SQL{
		PluginName: dsql.PluginName, DatabaseName: "uuid_test",
		Connect: func(*config.SQL) (*sqlx.DB, error) { return fakeDB, nil },
	}, resolver.NewNoopResolver(), log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, fake
}
