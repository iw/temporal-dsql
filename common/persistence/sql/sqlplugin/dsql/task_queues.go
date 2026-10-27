package dsql

import (
	"context"
	"database/sql"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

// DSQL returns UUID columns as text; core uses the stored UUID bytes as its page cursor.
type dsqlTaskQueueRow struct {
	RangeHash    uint32 `db:"range_hash"`
	TaskQueueID  string `db:"task_queue_id"`
	RangeID      int64  `db:"range_id"`
	Data         []byte `db:"data"`
	DataEncoding string `db:"data_encoding"`
}

func (r *dsqlTaskQueueRow) toPluginRow() sqlplugin.TaskQueuesRow {
	return sqlplugin.TaskQueuesRow{
		RangeHash:    r.RangeHash,
		TaskQueueID:  uuidStringToBytes(r.TaskQueueID),
		RangeID:      r.RangeID,
		Data:         r.Data,
		DataEncoding: r.DataEncoding,
	}
}

const (
	taskQueueCreatePart = `INTO task_queues_v2 (range_hash, task_queue_id, range_id, data, data_encoding) ` +
		`VALUES (:range_hash, :task_queue_id, :range_id, :data, :data_encoding)`

	// (default range ID: initialRangeID == 1)
	createTaskQueueQry = `INSERT ` + taskQueueCreatePart

	// NOTE: updateTaskQueueQry is intentionally unfenced (matches upstream Postgres plugin semantics).
	// On DSQL, callers should prefer UpdateTaskQueuesWithFencing / UpdateTaskQueuesAutoFenced for correctness.
	updateTaskQueueQry = `UPDATE task_queues_v2 SET
	range_id = :range_id,
	data = :data,
	data_encoding = :data_encoding
	WHERE
	range_hash = :range_hash AND
	task_queue_id = :task_queue_id
	`

	listTaskQueueRowSelect = `SELECT range_hash, task_queue_id, range_id, data, data_encoding FROM task_queues_v2 `

	listTaskQueueWithHashRangeQry = listTaskQueueRowSelect +
		`WHERE range_hash >= $1 AND range_hash <= $2 AND task_queue_id > $3 ORDER BY task_queue_id ASC LIMIT $4`

	listTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = $1 AND task_queue_id > $2 ORDER BY task_queue_id ASC LIMIT $3`

	getTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = $1 AND task_queue_id=$2`

	deleteTaskQueueQry = `DELETE FROM task_queues_v2 WHERE range_hash=$1 AND task_queue_id=$2 AND range_id=$3`

	lockTaskQueueQry = `SELECT range_id FROM task_queues_v2 ` +
		`WHERE range_hash=$1 AND task_queue_id=$2 FOR UPDATE`
)

// InsertIntoTaskQueues inserts one or more rows into task_queues[_v2] table
func (pdb *db) InsertIntoTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	// DSQL keying: task_queues_v2.task_queue_id is stored as UUID (DB column type UUID).
	// Bind as a string UUID, not []byte (which would be treated as BYTEA).
	args := map[string]interface{}{
		"range_hash":    row.RangeHash,
		"task_queue_id": TaskQueueIDToUUIDString(row.TaskQueueID),
		"range_id":      row.RangeID,
		"data":          row.Data,
		"data_encoding": row.DataEncoding,
	}

	return pdb.NamedExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(createTaskQueueQry, v),
		args,
	)
}

// UpdateTaskQueues updates a row in task_queues[_v2] table.
//
// DSQL Note: This function is called within a transaction after lockTaskQueue has already:
// 1. Read the current range_id with FOR UPDATE
// 2. Validated that range_id matches the expected PrevRangeID
//
// Under DSQL's OCC model:
// - The FOR UPDATE in lockTaskQueue marks the row in the transaction's read set
// - If another transaction modifies the row before we commit, DSQL returns SQLSTATE 40001
// - The txExecute retry loop handles 40001 by retrying the entire transaction
//
// Therefore, we can use a simple unfenced UPDATE (matching PostgreSQL behavior) because
// the transaction-level OCC provides the necessary consistency guarantees.
func (pdb *db) UpdateTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	taskQueueID := TaskQueueIDToUUIDString(row.TaskQueueID)

	args := map[string]interface{}{
		"range_hash":    row.RangeHash,
		"task_queue_id": taskQueueID,
		"range_id":      row.RangeID,
		"data":          row.Data,
		"data_encoding": row.DataEncoding,
	}

	return pdb.NamedExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(updateTaskQueueQry, v),
		args,
	)
}

// UpdateTaskQueuesAutoFenced performs a conditional update without relying on any inferred
// expected range_id (e.g. "newRangeID-1").
//
// It reads the current range_id and attempts to update only if the row is unchanged.
// Updates are allowed when:
// - row.RangeID == currentRangeID (metadata-only update, e.g., ack level, backlog count)
// - row.RangeID > currentRangeID (range_id increment for ownership changes)
// Updates are rejected when:
// - row.RangeID < currentRangeID (stale update from old owner)
func (pdb *db) UpdateTaskQueuesAutoFenced(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	taskQueueID := TaskQueueIDToUUIDString(row.TaskQueueID)

	var currentRangeID int64
	var err error

	lockQry := sqlplugin.SwitchTaskQueuesTable(lockTaskQueueQry, v)

	// We intentionally use lockTaskQueueQry (FOR UPDATE). In DSQL this does not block, but it is
	// a supported primitive and matches the intention of reading a stable fencing token.
	if pdb.tx != nil {
		err = pdb.tx.QueryRowContext(ctx, lockQry, row.RangeHash, taskQueueID).Scan(&currentRangeID)
	} else {
		err = pdb.QueryRowContext(ctx, lockQry, row.RangeHash, taskQueueID).Scan(&currentRangeID)
	}
	if err != nil {
		return nil, err
	}

	// Reject stale updates: new range_id must be >= current range_id.
	// This allows:
	// - Same range_id (metadata updates like ack level, backlog count)
	// - Higher range_id (ownership changes)
	// But rejects:
	// - Lower range_id (stale updates from old owners)
	if row.RangeID < currentRangeID {
		return nil, NewConditionFailedError(
			ConditionFailedTaskQueue,
			fmt.Sprintf("task queue %s range_id is stale (current=%d new=%d)", taskQueueID, currentRangeID, row.RangeID),
		)
	}

	// Attempt fenced update against the observed current value.
	return pdb.UpdateTaskQueuesWithFencing(ctx, row, currentRangeID, v)
}

// SelectFromTaskQueues reads one or more rows from task_queues[_v2] table
func (pdb *db) SelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	switch {
	case filter.TaskQueueID != nil:
		if filter.RangeHashLessThanEqualTo != 0 || filter.RangeHashGreaterThanEqualTo != 0 {
			return nil, serviceerror.NewInternal("shardID range not supported for specific selection")
		}
		return pdb.selectFromTaskQueues(ctx, filter, v)
	case filter.RangeHashLessThanEqualTo != 0 && filter.PageSize != nil:
		if filter.RangeHashLessThanEqualTo < filter.RangeHashGreaterThanEqualTo {
			return nil, serviceerror.NewInternal("range of hashes bound is invalid")
		}
		return pdb.rangeSelectFromTaskQueues(ctx, filter, v)
	case filter.TaskQueueIDGreaterThan != nil && filter.PageSize != nil:
		return pdb.rangeSelectFromTaskQueues(ctx, filter, v)
	default:
		return nil, serviceerror.NewInternal("invalid set of query filter params")
	}
}

func (pdb *db) selectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var row dsqlTaskQueueRow

	err = pdb.GetContext(ctx,
		&row,
		sqlplugin.SwitchTaskQueuesTable(getTaskQueueQry, v),
		filter.RangeHash,
		TaskQueueIDToUUIDString(filter.TaskQueueID),
	)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskQueuesRow{row.toPluginRow()}, nil
}

func (pdb *db) rangeSelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var dsqlRows []dsqlTaskQueueRow

	// DSQL: task_queue_id is UUID type, so we need a valid UUID for comparison.
	// Use nil UUID (all zeros) as the minimum value when no filter is specified.
	// This ensures "task_queue_id > $N" works correctly with UUID ordering.
	gt := NilUUID
	if len(filter.TaskQueueIDGreaterThan) > 0 {
		// This cursor is the stored UUID from the previous page, not a logical queue ID.
		gt = BytesToUUIDString(filter.TaskQueueIDGreaterThan)
	}

	if filter.RangeHashLessThanEqualTo > 0 {
		err = pdb.SelectContext(ctx,
			&dsqlRows,
			sqlplugin.SwitchTaskQueuesTable(listTaskQueueWithHashRangeQry, v),
			filter.RangeHashGreaterThanEqualTo,
			filter.RangeHashLessThanEqualTo,
			gt,
			*filter.PageSize,
		)
	} else {
		err = pdb.SelectContext(ctx,
			&dsqlRows,
			sqlplugin.SwitchTaskQueuesTable(listTaskQueueQry, v),
			filter.RangeHash,
			gt,
			*filter.PageSize,
		)
	}
	if err != nil {
		return nil, err
	}

	rows := make([]sqlplugin.TaskQueuesRow, len(dsqlRows))
	for i := range dsqlRows {
		rows[i] = dsqlRows[i].toPluginRow()
	}
	return rows, nil
}

// DeleteFromTaskQueues deletes a row from task_queues[_v2] table
func (pdb *db) DeleteFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(deleteTaskQueueQry, v),
		filter.RangeHash,
		TaskQueueIDToUUIDString(filter.TaskQueueID),
		*filter.RangeID,
	)
}

// LockTaskQueues locks a row in task_queues[_v2] table
// DSQL Override: Wraps FOR UPDATE operation in retry logic for SQLSTATE 40001 handling
// Call-site contract: All subsequent updates must use CAS with range_id fencing
func (pdb *db) LockTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) (int64, error) {
	taskQueueID := TaskQueueIDToUUIDString(filter.TaskQueueID)

	// Use retry manager if available (for non-transaction contexts)
	if pdb.tx == nil && pdb.retryManager != nil {
		result, err := pdb.retryManager.RunTx(ctx, "LockTaskQueues", func(tx *sql.Tx) (interface{}, error) {
			var rangeID int64
			query := sqlplugin.SwitchTaskQueuesTable(lockTaskQueueQry, v)
			err := tx.QueryRowContext(ctx, query, filter.RangeHash, taskQueueID).Scan(&rangeID)
			return rangeID, err
		})
		if err != nil {
			return 0, err
		}
		return result.(int64), nil
	}

	// Direct execution for transaction contexts (retry handled at higher level)
	var rangeID int64
	err := pdb.GetContext(ctx,
		&rangeID,
		sqlplugin.SwitchTaskQueuesTable(lockTaskQueueQry, v),
		filter.RangeHash,
		taskQueueID,
	)
	return rangeID, err
}

// UpdateTaskQueuesWithCAS performs a conditional update on task_queues table
// using range_id as fencing token.
func (pdb *db) UpdateTaskQueuesWithCAS(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	expectedRangeID int64,
	v sqlplugin.MatchingTaskVersion,
) error {
	const updateTaskQueueWithCASQry = `UPDATE task_queues_v2 SET
		range_id = :range_id,
		data = :data,
		data_encoding = :data_encoding
		WHERE
		range_hash = :range_hash AND
		task_queue_id = :task_queue_id AND
		range_id = :expected_range_id`

	taskQueueID := TaskQueueIDToUUIDString(row.TaskQueueID)

	args := map[string]interface{}{
		"range_hash":        row.RangeHash,
		"task_queue_id":     taskQueueID,
		"range_id":          row.RangeID,
		"data":              row.Data,
		"data_encoding":     row.DataEncoding,
		"expected_range_id": expectedRangeID,
	}

	result, err := pdb.NamedExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(updateTaskQueueWithCASQry, v),
		args,
	)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return NewConditionFailedError(
			ConditionFailedTaskQueue,
			fmt.Sprintf("task_queue %s range_id changed from expected %d (CAS update failed)", taskQueueID, expectedRangeID),
		)
	}

	return nil
}

// UpdateTaskQueuesWithFencing performs a fenced update on task_queues table using range_id as fencing token
func (pdb *db) UpdateTaskQueuesWithFencing(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	expectedRangeID int64,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	err := pdb.UpdateTaskQueuesWithCAS(ctx, row, expectedRangeID, v)
	if err != nil {
		return nil, err
	}

	// Return a dummy result since UpdateTaskQueuesWithCAS doesn't return sql.Result
	// This maintains interface compatibility
	return &dummyResult{rowsAffected: 1}, nil
}

// UpdateTaskQueueRangeWithCAS performs a conditional range_id increment on task_queues table.
func (pdb *db) UpdateTaskQueueRangeWithCAS(
	ctx context.Context,
	rangeHash uint32,
	taskQueueID string,
	expectedRangeID int64,
	v sqlplugin.MatchingTaskVersion,
) (int64, error) {
	newRangeID := expectedRangeID + 1
	const updateTaskQueueRangeWithCASQry = `UPDATE task_queues_v2 
		SET range_id = $1 
		WHERE range_hash = $2 AND task_queue_id = $3 AND range_id = $4`

	result, err := pdb.ExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(updateTaskQueueRangeWithCASQry, v),
		newRangeID,
		rangeHash,
		taskQueueID,
		expectedRangeID,
	)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if rowsAffected == 0 {
		return 0, NewConditionFailedError(
			ConditionFailedTaskQueue,
			fmt.Sprintf("task_queue %s range_id changed from expected %d (CAS range increment failed)", taskQueueID, expectedRangeID),
		)
	}

	return newRangeID, nil
}
