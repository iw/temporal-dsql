//go:build integration

package dsql_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/primitives"
)

// TestDSQLFairTaskRangeDeleteIntegration checks matching's fair task range delete on a
// new queue, which is missing from the table statistics as a new worker's queue is.
// DSQL_TEST_ENDPOINT is the cluster hostname; credentials use the default AWS chain.
// Run from the repository root:
//
//	DSQL_TEST_ENDPOINT='<cluster-hostname>' go test -tags integration,disable_grpc_modules,test_dep ./common/persistence/sql/sqlplugin/dsql -run '^TestDSQLFairTaskRangeDeleteIntegration$' -count=1 -timeout=5m -v
func TestDSQLFairTaskRangeDeleteIntegration(t *testing.T) {
	db, _ := newBehaviorIntegrationDB(t)
	conn, ok := db.(sqlplugin.Conn)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	rangeHash := rand.Uint32()
	queueID := []byte("fair-task-range-delete-" + primitives.NewUUID().String())
	neighbourID := []byte("fair-task-range-delete-" + primitives.NewUUID().String())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		for _, id := range [][]byte{queueID, neighbourID} {
			_, err := conn.ExecContext(ctx, "DELETE FROM tasks_v2 WHERE range_hash = $1 AND task_queue_id = $2",
				rangeHash, dsql.TaskQueueIDToUUIDString(id))
			require.NoError(t, err)
		}
	})
	// Fifty tasks share each pass, as tasks with different fairness keys can.
	const tasksPerPass = 50
	insertTasks := func(id []byte, first, last int64) {
		rows := make([]sqlplugin.TasksRowV2, 0, last-first+1)
		for taskID := first; taskID <= last; taskID++ {
			rows = append(rows, sqlplugin.TasksRowV2{
				RangeHash: rangeHash, TaskQueueID: id,
				FairLevel: sqlplugin.FairLevel{TaskPass: 1 + (taskID-1)/tasksPerPass, TaskID: taskID},
				Data:      []byte("task"), DataEncoding: "Proto3",
			})
		}
		_, err := db.InsertIntoTasksV2(ctx, rows)
		require.NoError(t, err)
	}
	for first := int64(1); first <= 2000; first += 1000 {
		insertTasks(queueID, first, first+999)
	}
	insertTasks(neighbourID, 1, 10)

	requireNoJoin(t, conn, dsql.RangeDeleteTaskV2Query,
		rangeHash, dsql.TaskQueueIDToUUIDString(queueID), int64(41), int64(1), 100)

	filter := func(maxPass, maxTaskID int64, limit int) sqlplugin.TasksFilterV2 {
		return sqlplugin.TasksFilterV2{
			RangeHash: rangeHash, TaskQueueID: queueID,
			ExclusiveMaxLevel: &sqlplugin.FairLevel{TaskPass: maxPass, TaskID: maxTaskID}, Limit: &limit,
		}
	}
	deleteBatch := func(maxPass, maxTaskID int64, limit int) int64 {
		result, err := db.DeleteFromTasksV2(ctx, filter(maxPass, maxTaskID, limit))
		require.NoError(t, err)
		deleted, err := result.RowsAffected()
		require.NoError(t, err)
		return deleted
	}
	type remainingTasks struct {
		Count     int64 `db:"count"`
		FirstPass int64 `db:"first_pass"`
		FirstID   int64 `db:"first_id"`
	}
	remaining := func(id []byte) remainingTasks {
		var row remainingTasks
		require.NoError(t, conn.GetContext(ctx, &row,
			"SELECT count(*) AS count, "+
				"COALESCE((SELECT pass FROM tasks_v2 WHERE range_hash = $1 AND task_queue_id = $2 ORDER BY pass, task_id LIMIT 1), 0) AS first_pass, "+
				"COALESCE((SELECT task_id FROM tasks_v2 WHERE range_hash = $1 AND task_queue_id = $2 ORDER BY pass, task_id LIMIT 1), 0) AS first_id "+
				"FROM tasks_v2 WHERE range_hash = $1 AND task_queue_id = $2",
			rangeHash, dsql.TaskQueueIDToUUIDString(id)))
		return row
	}

	// A full batch covers passes 1 and 2.
	require.EqualValues(t, 100, deleteBatch(41, 1, 100))
	require.Equal(t, remainingTasks{Count: 1900, FirstPass: 3, FirstID: 101}, remaining(queueID))

	// A batch that ends inside pass 4 leaves the rest of that pass. Core runs fair
	// deletes inside a transaction when it checks queue ownership.
	tx, err := db.BeginTx(ctx)
	require.NoError(t, err)
	result, err := tx.DeleteFromTasksV2(ctx, filter(41, 1, 75))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	deleted, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 75, deleted)
	require.Equal(t, remainingTasks{Count: 1825, FirstPass: 4, FirstID: 176}, remaining(queueID))

	// Fewer rows than the limit lie below a bound inside pass 5.
	require.EqualValues(t, 50, deleteBatch(5, 226, 100))
	require.EqualValues(t, 0, deleteBatch(5, 226, 100))
	require.Equal(t, remainingTasks{Count: 1775, FirstPass: 5, FirstID: 226}, remaining(queueID))

	require.Equal(t, remainingTasks{Count: 10, FirstPass: 1, FirstID: 1}, remaining(neighbourID))
}
