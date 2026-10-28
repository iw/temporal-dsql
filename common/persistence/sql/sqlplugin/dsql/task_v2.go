package dsql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	getTaskV2Qry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE range_hash = $1 ` +
		`AND task_queue_id = $2 ` +
		`AND (pass, task_id) >= ($3, $4) ` +
		`ORDER BY pass, task_id`

	getTaskV2QryWithLimit = getTaskV2Qry + ` LIMIT $5`

	createTaskV2Qry = `INSERT INTO ` +
		`tasks_v2 ( range_hash,  task_queue_id,  task_id,       pass,  data,  data_encoding) ` +
		`VALUES   (:range_hash, :task_queue_id, :task_id, :task_pass, :data, :data_encoding)`

	rangeDeleteTaskV2Qry = `DELETE FROM tasks_v2 ` +
		`WHERE range_hash = $1 AND task_queue_id = $2 AND task_id IN (SELECT task_id FROM ` +
		`tasks_v2 WHERE range_hash = $1 AND task_queue_id = $2 AND (pass, task_id) < ($3, $4) ` +
		`ORDER BY task_queue_id,pass,task_id LIMIT $5 )`
)

type tasksV2InsertRow struct {
	RangeHash    uint32 `db:"range_hash"`
	TaskQueueID  string `db:"task_queue_id"`
	TaskID       int64  `db:"task_id"`
	TaskPass     int64  `db:"task_pass"`
	Data         []byte `db:"data"`
	DataEncoding string `db:"data_encoding"`
}

// InsertIntoTasks inserts one or more rows into tasks_v2 table.
func (pdb *db) InsertIntoTasksV2(
	ctx context.Context,
	rows []sqlplugin.TasksRowV2,
) (sql.Result, error) {
	// DSQL keying: tasks_v2.task_queue_id is stored as UUID (DB column type UUID).
	// We must bind it as a string UUID (or uuid.UUID), not []byte (which would be treated as BYTEA).
	insertRows := make([]tasksV2InsertRow, 0, len(rows))
	for _, r := range rows {
		insertRows = append(insertRows, tasksV2InsertRow{
			RangeHash:    r.RangeHash,
			TaskQueueID:  TaskQueueIDToUUIDString(r.TaskQueueID),
			TaskID:       r.TaskID,
			TaskPass:     r.TaskPass,
			Data:         r.Data,
			DataEncoding: r.DataEncoding,
		})
	}

	return pdb.NamedExecContext(ctx, createTaskV2Qry, insertRows)
}

// SelectFromTasks reads one or more rows from tasks_v2 table.
func (pdb *db) SelectFromTasksV2(
	ctx context.Context,
	filter sqlplugin.TasksFilterV2,
) ([]sqlplugin.TasksRowV2, error) {
	if filter.InclusiveMinLevel == nil {
		return nil, serviceerror.NewInternal("missing InclusiveMinLevel")
	}

	taskQueueID := TaskQueueIDToUUIDString(filter.TaskQueueID)

	var err error
	var rows []sqlplugin.TasksRowV2
	switch {
	case filter.PageSize != nil:
		err = pdb.SelectContext(ctx,
			&rows,
			getTaskV2QryWithLimit,
			filter.RangeHash,
			taskQueueID,
			filter.InclusiveMinLevel.TaskPass,
			filter.InclusiveMinLevel.TaskID,
			*filter.PageSize,
		)
	default:
		err = pdb.SelectContext(ctx,
			&rows,
			getTaskV2Qry,
			filter.RangeHash,
			taskQueueID,
			filter.InclusiveMinLevel.TaskPass,
			filter.InclusiveMinLevel.TaskID,
		)
	}
	return rows, err
}

// DeleteFromTasks deletes multiple rows from tasks_v2 table.
func (pdb *db) DeleteFromTasksV2(
	ctx context.Context,
	filter sqlplugin.TasksFilterV2,
) (sql.Result, error) {
	if filter.ExclusiveMaxLevel == nil {
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskLevel")
	}
	if filter.Limit == nil || *filter.Limit == 0 {
		return nil, serviceerror.NewInternal("missing limit parameter")
	}

	taskQueueID := TaskQueueIDToUUIDString(filter.TaskQueueID)

	return pdb.ExecContext(ctx,
		rangeDeleteTaskV2Qry,
		filter.RangeHash,
		taskQueueID,
		filter.ExclusiveMaxLevel.TaskPass,
		filter.ExclusiveMaxLevel.TaskID,
		*filter.Limit,
	)
}