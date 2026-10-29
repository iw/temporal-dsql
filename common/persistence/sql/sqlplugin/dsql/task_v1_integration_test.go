//go:build integration

package dsql_test

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/primitives"
)

// TestDSQLTaskRangeDeleteIntegration checks matching's task range delete on a new
// queue, which is missing from the table statistics as a new worker's sticky queue is.
// DSQL_TEST_ENDPOINT is the cluster hostname; credentials use the default AWS chain.
// Run from the repository root:
//
//	DSQL_TEST_ENDPOINT='<cluster-hostname>' go test -tags integration,disable_grpc_modules,test_dep ./common/persistence/sql/sqlplugin/dsql -run '^TestDSQLTaskRangeDeleteIntegration$' -count=1 -timeout=5m -v
func TestDSQLTaskRangeDeleteIntegration(t *testing.T) {
	db, _ := newBehaviorIntegrationDB(t)
	conn, ok := db.(sqlplugin.Conn)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	rangeHash := rand.Uint32()
	queueID := []byte("task-range-delete-" + primitives.NewUUID().String())
	neighbourID := []byte("task-range-delete-" + primitives.NewUUID().String())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		for _, id := range [][]byte{queueID, neighbourID} {
			_, err := conn.ExecContext(ctx, "DELETE FROM tasks WHERE range_hash = $1 AND task_queue_id = $2",
				rangeHash, dsql.TaskQueueIDToUUIDString(id))
			require.NoError(t, err)
		}
	})
	insertTasks := func(id []byte, first, last int64) {
		rows := make([]sqlplugin.TasksRow, 0, last-first+1)
		for taskID := first; taskID <= last; taskID++ {
			rows = append(rows, sqlplugin.TasksRow{
				RangeHash: rangeHash, TaskQueueID: id, TaskID: taskID, Data: []byte("task"), DataEncoding: "Proto3",
			})
		}
		_, err := db.InsertIntoTasks(ctx, rows)
		require.NoError(t, err)
	}
	for first := int64(1); first <= 2000; first += 1000 {
		insertTasks(queueID, first, first+999)
	}
	insertTasks(neighbourID, 1, 10)

	// The plan must not depend on the queue's row estimate: no join, so no subquery reruns.
	var plan string
	require.NoError(t, conn.GetContext(ctx, &plan, "EXPLAIN (FORMAT JSON) "+dsql.RangeDeleteTaskQuery,
		rangeHash, dsql.TaskQueueIDToUUIDString(queueID), int64(1001), 100))
	var explained []struct{ Plan planNode }
	require.NoError(t, json.Unmarshal([]byte(plan), &explained))
	require.Len(t, explained, 1)
	for _, nodeType := range explained[0].Plan.nodeTypes() {
		require.NotContains(t, nodeType, "Join", plan)
		require.NotEqual(t, "Nested Loop", nodeType, plan)
	}

	deleteBatch := func(exclusiveMaxTaskID int64, limit int) int64 {
		result, err := db.DeleteFromTasks(ctx, sqlplugin.TasksFilter{
			RangeHash: rangeHash, TaskQueueID: queueID, ExclusiveMaxTaskID: &exclusiveMaxTaskID, Limit: &limit,
		})
		require.NoError(t, err)
		deleted, err := result.RowsAffected()
		require.NoError(t, err)
		return deleted
	}
	remaining := func(id []byte) (count, minTaskID int64) {
		var row struct {
			Count     int64 `db:"count"`
			MinTaskID int64 `db:"min_task_id"`
		}
		require.NoError(t, conn.GetContext(ctx, &row,
			"SELECT count(*) AS count, COALESCE(min(task_id), 0) AS min_task_id FROM tasks WHERE range_hash = $1 AND task_queue_id = $2",
			rangeHash, dsql.TaskQueueIDToUUIDString(id)))
		return row.Count, row.MinTaskID
	}

	require.EqualValues(t, 100, deleteBatch(1001, 100))
	require.EqualValues(t, 100, deleteBatch(1001, 100))
	count, minTaskID := remaining(queueID)
	require.EqualValues(t, 1800, count)
	require.EqualValues(t, 201, minTaskID)

	// Fewer rows than the limit lie below the bound.
	require.EqualValues(t, 50, deleteBatch(251, 100))
	require.EqualValues(t, 0, deleteBatch(251, 100))
	require.EqualValues(t, 750, deleteBatch(1001, 1000))
	count, minTaskID = remaining(queueID)
	require.EqualValues(t, 1000, count)
	require.EqualValues(t, 1001, minTaskID)

	count, minTaskID = remaining(neighbourID)
	require.EqualValues(t, 10, count)
	require.EqualValues(t, 1, minTaskID)
}

type planNode struct {
	NodeType string     `json:"Node Type"`
	Plans    []planNode `json:"Plans"`
}

func (n planNode) nodeTypes() []string {
	types := []string{n.NodeType}
	for _, child := range n.Plans {
		types = append(types, child.nodeTypes()...)
	}
	return types
}
