package dsql

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	getTaskQueueUserDataQry = `SELECT data, data_encoding, version FROM task_queue_user_data ` +
		`WHERE namespace_id = $1 AND task_queue_name = $2`

	updateTaskQueueUserDataQry = `UPDATE task_queue_user_data SET ` +
		`data = $1, ` +
		`data_encoding = $2, ` +
		`version = $3 ` +
		`WHERE namespace_id = $4 ` +
		`AND task_queue_name = $5 ` +
		`AND version = $6`

	insertTaskQueueUserDataQry = `INSERT INTO task_queue_user_data` +
		`(namespace_id, task_queue_name, data, data_encoding, version) ` +
		`VALUES ($1, $2, $3, $4, 1)`

	listTaskQueueUserDataQry = `SELECT task_queue_name, data, data_encoding, version FROM task_queue_user_data WHERE namespace_id = $1 AND task_queue_name > $2 LIMIT $3`

	addBuildIDToTaskQueueMappingQry    = `INSERT INTO build_id_to_task_queue (namespace_id, build_id, task_queue_name) VALUES `
	removeBuildIDToTaskQueueMappingQry = `DELETE FROM build_id_to_task_queue WHERE namespace_id = $1 AND task_queue_name = $2 AND build_id IN (`
	listTaskQueuesByBuildIDQry         = `SELECT task_queue_name FROM build_id_to_task_queue WHERE namespace_id = $1 AND build_id = $2`
	countTaskQueuesByBuildIDQry        = `SELECT COUNT(*) FROM build_id_to_task_queue WHERE namespace_id = $1 AND build_id = $2`
)

func (pdb *db) GetTaskQueueUserData(ctx context.Context, request *sqlplugin.GetTaskQueueUserDataRequest) (*sqlplugin.VersionedBlob, error) {
	var row sqlplugin.VersionedBlob
	// Convert UUID bytes to string for DSQL VARCHAR compatibility
	namespaceIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", request.NamespaceID[0:4], request.NamespaceID[4:6], request.NamespaceID[6:8], request.NamespaceID[8:10], request.NamespaceID[10:16])
	err := pdb.GetContext(ctx, &row, getTaskQueueUserDataQry, namespaceIDStr, request.TaskQueueName)
	return &row, err
}

func (pdb *db) UpdateTaskQueueUserData(ctx context.Context, request *sqlplugin.UpdateTaskQueueDataRequest) error {
	namespaceIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", request.NamespaceID[0:4], request.NamespaceID[4:6], request.NamespaceID[6:8], request.NamespaceID[8:10], request.NamespaceID[10:16])

	if request.Version == 0 {
		_, err := pdb.ExecContext(
			ctx,
			insertTaskQueueUserDataQry,
			namespaceIDStr,
			request.TaskQueueName,
			request.Data,
			request.DataEncoding)
		return err
	}
	result, err := pdb.ExecContext(
		ctx,
		updateTaskQueueUserDataQry,
		request.Data,
		request.DataEncoding,
		request.Version+1,
		namespaceIDStr,
		request.TaskQueueName,
		request.Version)
	if err != nil {
		return err
	}
	numRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if numRows != 1 {
		return &persistence.ConditionFailedError{Msg: "Expected exactly one row to be updated"}
	}
	return nil
}

func (pdb *db) ListTaskQueueUserDataEntries(ctx context.Context, request *sqlplugin.ListTaskQueueUserDataEntriesRequest) ([]sqlplugin.TaskQueueUserDataEntry, error) {
	var rows []sqlplugin.TaskQueueUserDataEntry
	namespaceIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", request.NamespaceID[0:4], request.NamespaceID[4:6], request.NamespaceID[6:8], request.NamespaceID[8:10], request.NamespaceID[10:16])
	err := pdb.SelectContext(ctx, &rows, listTaskQueueUserDataQry, namespaceIDStr, request.LastTaskQueueName, request.Limit)
	return rows, err
}

func (pdb *db) AddToBuildIdToTaskQueueMapping(ctx context.Context, request sqlplugin.AddToBuildIdToTaskQueueMapping) error { //nolint:staticcheck // sqlplugin names this method
	query := addBuildIDToTaskQueueMappingQry
	var params []any
	namespaceIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", request.NamespaceID[0:4], request.NamespaceID[4:6], request.NamespaceID[6:8], request.NamespaceID[8:10], request.NamespaceID[10:16])

	for idx, buildID := range request.BuildIds {
		query += fmt.Sprintf("($%d, $%d, $%d)", idx*3+1, idx*3+2, idx*3+3)
		if idx < len(request.BuildIds)-1 {
			query += ", "
		}
		params = append(params, namespaceIDStr, buildID, request.TaskQueueName)
	}

	_, err := pdb.ExecContext(ctx, query, params...)
	return err
}

func (pdb *db) RemoveFromBuildIdToTaskQueueMapping(ctx context.Context, request sqlplugin.RemoveFromBuildIdToTaskQueueMapping) error { //nolint:staticcheck // sqlplugin names this method
	query := removeBuildIDToTaskQueueMappingQry
	// Golang doesn't support appending a string slice to an any slice which is essentially what we're doing here.
	params := make([]any, len(request.BuildIds)+2)
	namespaceIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", request.NamespaceID[0:4], request.NamespaceID[4:6], request.NamespaceID[6:8], request.NamespaceID[8:10], request.NamespaceID[10:16])
	params[0] = namespaceIDStr
	params[1] = request.TaskQueueName
	for idx, buildID := range request.BuildIds {
		sep := ", "
		if idx == len(request.BuildIds)-1 {
			sep = ")"
		}
		query += fmt.Sprintf("$%d%s", idx+3, sep)
		params[idx+2] = buildID
	}

	_, err := pdb.ExecContext(ctx, query, params...)
	return err
}

func (pdb *db) GetTaskQueuesByBuildId(ctx context.Context, request *sqlplugin.GetTaskQueuesByBuildIdRequest) ([]string, error) { //nolint:staticcheck // sqlplugin names this method
	var rows []struct {
		TaskQueueName string
	}

	namespaceIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", request.NamespaceID[0:4], request.NamespaceID[4:6], request.NamespaceID[6:8], request.NamespaceID[8:10], request.NamespaceID[10:16])
	err := pdb.SelectContext(ctx, &rows, listTaskQueuesByBuildIDQry, namespaceIDStr, request.BuildID)
	taskQueues := make([]string, len(rows))
	for i, row := range rows {
		taskQueues[i] = row.TaskQueueName
	}
	return taskQueues, err
}

func (pdb *db) CountTaskQueuesByBuildId(ctx context.Context, request *sqlplugin.CountTaskQueuesByBuildIdRequest) (int, error) { //nolint:staticcheck // sqlplugin names this method
	var count int
	namespaceIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", request.NamespaceID[0:4], request.NamespaceID[4:6], request.NamespaceID[6:8], request.NamespaceID[8:10], request.NamespaceID[10:16])
	err := pdb.GetContext(ctx, &count, countTaskQueuesByBuildIDQry, namespaceIDStr, request.BuildID)
	return count, err
}
