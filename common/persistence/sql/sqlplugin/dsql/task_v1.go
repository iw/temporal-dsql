package dsql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	getTaskMinMaxQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id=$2 AND task_id >= $3 AND task_id < $4 ` +
		`ORDER BY task_id LIMIT $5`

	getTaskMinQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id = $2 AND task_id >= $3 ORDER BY task_id LIMIT $4`

	createTaskQry = `INSERT INTO ` +
		`tasks(range_hash, task_queue_id, task_id, data, data_encoding) ` +
		`VALUES(:range_hash, :task_queue_id, :task_id, :data, :data_encoding)`

	rangeDeleteTaskQry = `DELETE FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id = $2 AND task_id IN (SELECT task_id FROM
		 tasks WHERE range_hash = $1 AND task_queue_id = $2 AND task_id < $3 ` +
		`ORDER BY task_queue_id,task_id LIMIT $4 )`
)

// InsertIntoTasks inserts one or more rows into tasks table
func (pdb *db) InsertIntoTasks(
	ctx context.Context,
	rows []sqlplugin.TasksRow,
) (sql.Result, error) {
	// DSQL keying: tasks.task_queue_id is stored as UUID (or UUID-compatible text).
	// sqlplugin.TasksRow.TaskQueueID is []byte (original taskQueueId), so we must not
	// mutate it to []byte(uuidString) (that would bind as BYTEA). Instead, build a
	// slice of insert rows where task_queue_id is a string.
	type insertRow struct {
		RangeHash    uint32 `db:"range_hash"`
		TaskQueueID  string `db:"task_queue_id"`
		TaskID       int64  `db:"task_id"`
		Data         []byte `db:"data"`
		DataEncoding string `db:"data_encoding"`
	}

	insertRows := make([]insertRow, 0, len(rows))
	for _, r := range rows {
		insertRows = append(insertRows, insertRow{
			RangeHash:    r.RangeHash,
			TaskQueueID:  TaskQueueIDToUUIDString(r.TaskQueueID),
			TaskID:       r.TaskID,
			Data:         r.Data,
			DataEncoding: r.DataEncoding,
		})
	}

	return pdb.NamedExecContext(ctx, createTaskQry, insertRows)
}

// SelectFromTasks reads one or more rows from tasks table
func (pdb *db) SelectFromTasks(
	ctx context.Context,
	filter sqlplugin.TasksFilter,
) ([]sqlplugin.TasksRow, error) {
	var err error
	var rows []sqlplugin.TasksRow

	taskQueueID := TaskQueueIDToUUIDString(filter.TaskQueueID)

	switch {
	case filter.ExclusiveMaxTaskID != nil:
		err = pdb.SelectContext(ctx,
			&rows,
			getTaskMinMaxQry,
			filter.RangeHash,
			taskQueueID,
			*filter.InclusiveMinTaskID,
			*filter.ExclusiveMaxTaskID,
			*filter.PageSize,
		)
	default:
		err = pdb.SelectContext(ctx,
			&rows,
			getTaskMinQry,
			filter.RangeHash,
			taskQueueID,
			*filter.InclusiveMinTaskID,
			*filter.PageSize,
		)
	}
	return rows, err
}

// DeleteFromTasks deletes multiple rows from tasks table
func (pdb *db) DeleteFromTasks(
	ctx context.Context,
	filter sqlplugin.TasksFilter,
) (sql.Result, error) {
	if filter.ExclusiveMaxTaskID == nil {
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskID parameter")
	}
	if filter.Limit == nil || *filter.Limit == 0 {
		return nil, serviceerror.NewInternal("missing limit parameter")
	}

	taskQueueID := TaskQueueIDToUUIDString(filter.TaskQueueID)

	return pdb.ExecContext(ctx,
		rangeDeleteTaskQry,
		filter.RangeHash,
		taskQueueID,
		*filter.ExclusiveMaxTaskID,
		*filter.Limit,
	)
}