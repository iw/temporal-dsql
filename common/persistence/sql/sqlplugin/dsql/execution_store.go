package dsql

import (
	"context"
	"database/sql"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

// DSQLExecutionStore wraps the common SQL execution store with DSQL-specific optimizations.
//
// # Why This Optimization Exists
//
// The common SQL execution store's GetWorkflowExecution performs 9 separate database reads
// (executions, activity_info_maps, timer_info_maps, etc.), each as an implicit auto-committed
// transaction. For traditional databases (MySQL, PostgreSQL), this has negligible overhead.
//
// However, Aurora DSQL uses optimistic concurrency control (OCC) where each transaction
// boundary incurs validation overhead. The 9 separate transactions were identified as a
// significant contributor to GetWorkflowExecution latency (P95: 1.66s observed in benchmarks).
//
// # Design Decision: DSQL-Only, Non-Invasive
//
// Rather than modifying the common SQL persistence layer (which would affect all SQL backends
// and require extensive testing), we chose to:
//
//  1. Create a DSQL-specific wrapper that overrides only GetWorkflowExecution
//  2. Batch all 9 reads into a single transaction, reducing OCC overhead by ~9x
//  3. Preserve identical error messages and behavior for debugging
//  4. Delegate all other ExecutionStore methods to the common implementation
//
// This approach keeps the common code unchanged, isolates DSQL-specific optimizations,
// and allows independent evolution of the DSQL persistence layer.
type DSQLExecutionStore struct {
	p.ExecutionStore
	db     *db
	logger log.Logger
}

// NewDSQLExecutionStore creates a DSQL-optimized execution store that wraps the common SQL store.
func NewDSQLExecutionStore(base p.ExecutionStore, dsqlDB *db, logger log.Logger) *DSQLExecutionStore {
	return &DSQLExecutionStore{
		ExecutionStore: base,
		db:             dsqlDB,
		logger:         logger,
	}
}

// GetWorkflowExecution overrides the common implementation to batch all reads in a single transaction.
// This reduces 9 separate read-only transactions to 1, significantly improving performance for DSQL
// which has overhead per transaction due to its optimistic concurrency control model.
func (s *DSQLExecutionStore) GetWorkflowExecution(
	ctx context.Context,
	request *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	namespaceID := primitives.MustParseUUID(request.NamespaceID)
	workflowID := request.WorkflowID
	runID := primitives.MustParseUUID(request.RunID)

	var state *p.InternalWorkflowMutableState
	var dbRecordVersion int64

	// Execute all reads in a single transaction
	err := s.executeReadTx(ctx, func(tx sqlplugin.Tx) error {
		// 1. Read execution row
		executionsRow, err := tx.SelectFromExecutions(ctx, sqlplugin.ExecutionsFilter{
			ShardID:     request.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
		})
		if err != nil {
			if err == sql.ErrNoRows {
				return serviceerror.NewNotFoundf("Workflow executionsRow not found. WorkflowId: %v, RunId: %v", workflowID, runID)
			}
			return serviceerror.NewUnavailablef("GetWorkflowExecution: failed. Error: %v", err)
		}

		state = &p.InternalWorkflowMutableState{
			ExecutionInfo:   p.NewDataBlob(executionsRow.Data, executionsRow.DataEncoding),
			ExecutionState:  p.NewDataBlob(executionsRow.State, executionsRow.StateEncoding),
			NextEventID:     executionsRow.NextEventID,
			DBRecordVersion: executionsRow.DBRecordVersion,
		}
		dbRecordVersion = executionsRow.DBRecordVersion

		// 2. Read activity info map
		state.ActivityInfos, err = s.getActivityInfoMapTx(ctx, tx, request.ShardID, namespaceID, workflowID, runID)
		if err != nil {
			return serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get activity info. Error: %v", err)
		}

		// 3. Read timer info map
		state.TimerInfos, err = s.getTimerInfoMapTx(ctx, tx, request.ShardID, namespaceID, workflowID, runID)
		if err != nil {
			return serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get timer info. Error: %v", err)
		}

		// 4. Read child execution info map
		state.ChildExecutionInfos, err = s.getChildExecutionInfoMapTx(ctx, tx, request.ShardID, namespaceID, workflowID, runID)
		if err != nil {
			return serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get child execution info. Error: %v", err)
		}

		// 5. Read request cancel info map
		state.RequestCancelInfos, err = s.getRequestCancelInfoMapTx(ctx, tx, request.ShardID, namespaceID, workflowID, runID)
		if err != nil {
			return serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get request cancel info. Error: %v", err)
		}

		// 6. Read signal info map
		state.SignalInfos, err = s.getSignalInfoMapTx(ctx, tx, request.ShardID, namespaceID, workflowID, runID)
		if err != nil {
			return serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get signal info. Error: %v", err)
		}

		// 7. Read buffered events
		state.BufferedEvents, err = s.getBufferedEventsTx(ctx, tx, request.ShardID, namespaceID, workflowID, runID)
		if err != nil {
			return serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get buffered events. Error: %v", err)
		}

		// 8. Read CHASM nodes
		state.ChasmNodes, err = s.getChasmNodeMapTx(ctx, tx, request.ShardID, namespaceID, workflowID, runID)
		if err != nil {
			return serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get CHASM nodes. Error: %v", err)
		}

		// 9. Read signals requested
		state.SignalRequestedIDs, err = s.getSignalsRequestedTx(ctx, tx, request.ShardID, namespaceID, workflowID, runID)
		if err != nil {
			return serviceerror.NewUnavailablef("GetWorkflowExecution: failed to get signals requested. Error: %v", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &p.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: dbRecordVersion,
	}, nil
}

// executeReadTx executes a function within a read-only transaction.
// For DSQL, this batches multiple reads into a single transaction, reducing overhead.
func (s *DSQLExecutionStore) executeReadTx(ctx context.Context, fn func(tx sqlplugin.Tx) error) error {
	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		return serviceerror.NewUnavailablef("GetWorkflowExecution: failed to start transaction. Error: %v", err)
	}

	err = fn(tx)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	// For read-only transactions, commit is essentially a no-op but ensures proper cleanup
	if err := tx.Commit(); err != nil {
		return serviceerror.NewUnavailablef("GetWorkflowExecution: failed to commit transaction. Error: %v", err)
	}

	return nil
}

// Helper functions that use transaction instead of DB connection

func (s *DSQLExecutionStore) getActivityInfoMapTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*commonpb.DataBlob, error) {
	rows, err := tx.SelectAllFromActivityInfoMaps(ctx, sqlplugin.ActivityInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	ret := make(map[int64]*commonpb.DataBlob)
	for _, row := range rows {
		ret[row.ScheduleID] = p.NewDataBlob(row.Data, row.DataEncoding)
	}
	return ret, nil
}

func (s *DSQLExecutionStore) getTimerInfoMapTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[string]*commonpb.DataBlob, error) {
	rows, err := tx.SelectAllFromTimerInfoMaps(ctx, sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	ret := make(map[string]*commonpb.DataBlob)
	for _, row := range rows {
		ret[row.TimerID] = p.NewDataBlob(row.Data, row.DataEncoding)
	}
	return ret, nil
}

func (s *DSQLExecutionStore) getChildExecutionInfoMapTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*commonpb.DataBlob, error) {
	rows, err := tx.SelectAllFromChildExecutionInfoMaps(ctx, sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	ret := make(map[int64]*commonpb.DataBlob)
	for _, row := range rows {
		ret[row.InitiatedID] = p.NewDataBlob(row.Data, row.DataEncoding)
	}
	return ret, nil
}

func (s *DSQLExecutionStore) getRequestCancelInfoMapTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*commonpb.DataBlob, error) {
	rows, err := tx.SelectAllFromRequestCancelInfoMaps(ctx, sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	ret := make(map[int64]*commonpb.DataBlob)
	for _, row := range rows {
		ret[row.InitiatedID] = p.NewDataBlob(row.Data, row.DataEncoding)
	}
	return ret, nil
}

func (s *DSQLExecutionStore) getSignalInfoMapTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[int64]*commonpb.DataBlob, error) {
	rows, err := tx.SelectAllFromSignalInfoMaps(ctx, sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	ret := make(map[int64]*commonpb.DataBlob)
	for _, row := range rows {
		ret[row.InitiatedID] = p.NewDataBlob(row.Data, row.DataEncoding)
	}
	return ret, nil
}

func (s *DSQLExecutionStore) getBufferedEventsTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) ([]*commonpb.DataBlob, error) {
	rows, err := tx.SelectFromBufferedEvents(ctx, sqlplugin.BufferedEventsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	var result []*commonpb.DataBlob
	for _, row := range rows {
		result = append(result, p.NewDataBlob(row.Data, row.DataEncoding))
	}
	return result, nil
}

func (s *DSQLExecutionStore) getChasmNodeMapTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (map[string]p.InternalChasmNode, error) {
	rows, err := tx.SelectAllFromChasmNodeMaps(ctx, sqlplugin.ChasmNodeMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	ret := make(map[string]p.InternalChasmNode)
	for _, row := range rows {
		ret[row.ChasmPath] = p.InternalChasmNode{
			Metadata: p.NewDataBlob(row.Metadata, row.MetadataEncoding),
			Data:     p.NewDataBlob(row.Data, row.DataEncoding),
		}
	}
	return ret, nil
}

func (s *DSQLExecutionStore) getSignalsRequestedTx(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) ([]string, error) {
	rows, err := tx.SelectAllFromSignalsRequestedSets(ctx, sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	ret := make([]string, len(rows))
	for i, row := range rows {
		ret[i] = row.SignalID
	}
	return ret, nil
}
