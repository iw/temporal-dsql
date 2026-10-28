package dsql

import (
	"context"
	"database/sql"

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
func (pdb *db) LockTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) (int64, error) {
	taskQueueID := TaskQueueIDToUUIDString(filter.TaskQueueID)

	var rangeID int64
	err := pdb.GetContext(ctx,
		&rangeID,
		sqlplugin.SwitchTaskQueuesTable(lockTaskQueueQry, v),
		filter.RangeHash,
		taskQueueID,
	)
	return rangeID, err
}
