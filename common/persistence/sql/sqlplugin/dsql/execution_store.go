package dsql

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
)

// DSQLExecutionStore wraps the common SQL execution store, and reads a workflow's mutable
// state on one connection in one round trip once the connection has the statements
// prepared.
//
// The common store's GetWorkflowExecution makes 9 queries, each taking a pool connection
// in turn. When the pool is short, every read waits for 9 acquires, so the reads progress
// together and run out of time together. This store sends the 9 queries in one read-only
// transaction, pipelined as one pgx batch, which also reads them from one snapshot.
type DSQLExecutionStore struct {
	p.ExecutionStore
	db     *db
	logger log.Logger
}

// NewDSQLExecutionStore creates a DSQL execution store that wraps the common SQL store.
func NewDSQLExecutionStore(base p.ExecutionStore, dsqlDB *db, logger log.Logger) *DSQLExecutionStore {
	return &DSQLExecutionStore{
		ExecutionStore: base,
		db:             dsqlDB,
		logger:         logger,
	}
}

// mutableStateQueries are the mutable state read's statements, in the order their results
// are decoded.
var mutableStateQueries = []string{
	getExecutionQuery,
	getActivityInfoMapQry,
	getTimerInfoMapSQLQuery,
	getChildExecutionInfoMapQry,
	getRequestCancelInfoMapQry,
	getSignalInfoMapQry,
	getBufferedEventsQuery,
	getChasmNodeMapSQLQuery,
	getSignalsRequestedSetQuery,
}

// pipelineConn is the part of *pgx.Conn the read uses.
type pipelineConn interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	IsClosed() bool
}

// errNotPGX means the database's connections aren't pgx connections, as with a test's
// Connect hook.
var errNotPGX = errors.New("driver connection is not a pgx connection")

const readFailed = "GetWorkflowExecution: failed. Error: %v"

// GetWorkflowExecution reads the execution row and its maps in one read-only transaction,
// sent as one batch on one connection.
func (s *DSQLExecutionStore) GetWorkflowExecution(
	ctx context.Context,
	request *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	state, err := s.readMutableState(ctx, request)
	if errors.Is(err, errNotPGX) {
		return s.ExecutionStore.GetWorkflowExecution(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	return &p.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: state.DBRecordVersion,
	}, nil
}

// readMutableState takes one connection for the read, and gives it back before returning.
func (s *DSQLExecutionStore) readMutableState(
	ctx context.Context,
	request *p.GetWorkflowExecutionRequest,
) (*p.InternalWorkflowMutableState, error) {
	namespaceID := primitives.MustParseUUID(request.NamespaceID)
	runID := primitives.MustParseUUID(request.RunID)
	sqlDB, err := s.db.handle.DB()
	if err != nil {
		return nil, s.readError(readFailed, err)
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, s.readError(readFailed, err)
	}
	defer func() { _ = conn.Close() }()

	var state *p.InternalWorkflowMutableState
	err = conn.Raw(func(driverConn any) error {
		pc, ok := pgxConnOf(driverConn)
		if !ok {
			return errNotPGX
		}
		var readErr error
		state, readErr = s.pipelineRead(ctx, pc, request.ShardID, namespaceID, request.WorkflowID, runID)
		return readErr
	})
	return state, err
}

// pgxConnOf returns the pgx connection behind a database/sql driver connection. A
// gatedConn's embedded field, Conn, hides the Conn method of the *stdlib.Conn it embeds.
func pgxConnOf(driverConn any) (*pgx.Conn, bool) {
	switch c := driverConn.(type) {
	case *gatedConn:
		return c.Conn.Conn(), true
	case interface{ Conn() *pgx.Conn }:
		return c.Conn(), true
	}
	return nil, false
}

func (s *DSQLExecutionStore) pipelineRead(
	ctx context.Context,
	conn pipelineConn,
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) (*p.InternalWorkflowMutableState, error) {
	batch := &pgx.Batch{}
	batch.Queue("BEGIN READ ONLY")
	for _, query := range mutableStateQueries {
		batch.Queue(query, shardID, namespaceID.String(), workflowID, runID.String())
	}
	batch.Queue("COMMIT")
	results := conn.SendBatch(ctx, batch)
	state, err := s.decodeMutableState(results, workflowID, runID)
	// Close reads what's left of the batch, and returns the first statement's error.
	closeErr := results.Close()
	if err == nil && closeErr != nil {
		err = s.readError(readFailed, closeErr)
	}
	// A statement the server failed leaves the transaction aborted. Ending it keeps the
	// connection, which pgxpool would otherwise replace with a new one.
	var pgErr *pgconn.PgError
	if errors.As(closeErr, &pgErr) && !conn.IsClosed() {
		_, _ = conn.Exec(ctx, "ROLLBACK")
	}
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (s *DSQLExecutionStore) decodeMutableState(
	results pgx.BatchResults,
	workflowID string,
	runID primitives.UUID,
) (*p.InternalWorkflowMutableState, error) {
	if _, err := results.Exec(); err != nil {
		return nil, s.readError("GetWorkflowExecution: failed to start transaction. Error: %v", err)
	}

	var (
		found                       bool
		nextEventID, recordVersion  int64
		info, executionState        []byte
		infoEncoding, stateEncoding string
	)
	err := readRows(results, func(rows pgx.Rows) error {
		found = true
		// Of executionsColumns, the key columns and last_write_version aren't needed.
		return rows.Scan(nil, nil, nil, nil, &nextEventID, nil, &info, &infoEncoding, &executionState, &stateEncoding, &recordVersion)
	})
	if err != nil {
		return nil, s.readError(readFailed, err)
	}
	if !found {
		return nil, serviceerror.NewNotFoundf("Workflow executionsRow not found. WorkflowId: %v, RunId: %v", workflowID, runID)
	}
	state := &p.InternalWorkflowMutableState{
		ExecutionInfo:   p.NewDataBlob(info, infoEncoding),
		ExecutionState:  p.NewDataBlob(executionState, stateEncoding),
		NextEventID:     nextEventID,
		DBRecordVersion: recordVersion,
	}

	if state.ActivityInfos, err = readBlobMap[int64](results); err != nil {
		return nil, s.readError("GetWorkflowExecution: failed to get activity info. Error: %v", err)
	}
	if state.TimerInfos, err = readBlobMap[string](results); err != nil {
		return nil, s.readError("GetWorkflowExecution: failed to get timer info. Error: %v", err)
	}
	if state.ChildExecutionInfos, err = readBlobMap[int64](results); err != nil {
		return nil, s.readError("GetWorkflowExecution: failed to get child execution info. Error: %v", err)
	}
	if state.RequestCancelInfos, err = readBlobMap[int64](results); err != nil {
		return nil, s.readError("GetWorkflowExecution: failed to get request cancel info. Error: %v", err)
	}
	if state.SignalInfos, err = readBlobMap[int64](results); err != nil {
		return nil, s.readError("GetWorkflowExecution: failed to get signal info. Error: %v", err)
	}
	if state.BufferedEvents, err = readBufferedEvents(results); err != nil {
		return nil, s.readError("GetWorkflowExecution: failed to get buffered events. Error: %v", err)
	}
	if state.ChasmNodes, err = readChasmNodes(results); err != nil {
		return nil, s.readError("GetWorkflowExecution: failed to get CHASM nodes. Error: %v", err)
	}
	if state.SignalRequestedIDs, err = readSignalsRequested(results); err != nil {
		return nil, s.readError("GetWorkflowExecution: failed to get signals requested. Error: %v", err)
	}

	if _, err := results.Exec(); err != nil {
		return nil, s.readError("GetWorkflowExecution: failed to commit transaction. Error: %v", err)
	}
	return state, nil
}

// readError keeps the ResourceExhausted of a call the plugin didn't attempt, so history
// backs off rather than retrying at once, and reports other failures as the common store
// does.
func (s *DSQLExecutionStore) readError(format string, err error) error {
	err = s.db.maybeConvertError(err)
	var exhausted *serviceerror.ResourceExhausted
	if errors.As(err, &exhausted) {
		return err
	}
	return serviceerror.NewUnavailablef(format, err)
}

// readRows reads the next statement's rows from the batch.
func readRows(results pgx.BatchResults, scan func(pgx.Rows) error) error {
	rows, err := results.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// readBlobMap reads a map statement's rows of a key, data and encoding.
func readBlobMap[K int64 | string](results pgx.BatchResults) (map[K]*commonpb.DataBlob, error) {
	blobs := make(map[K]*commonpb.DataBlob)
	err := readRows(results, func(rows pgx.Rows) error {
		var (
			key      K
			data     []byte
			encoding string
		)
		if err := rows.Scan(&key, &data, &encoding); err != nil {
			return err
		}
		blobs[key] = p.NewDataBlob(data, encoding)
		return nil
	})
	return blobs, err
}

func readBufferedEvents(results pgx.BatchResults) ([]*commonpb.DataBlob, error) {
	var events []*commonpb.DataBlob
	err := readRows(results, func(rows pgx.Rows) error {
		var (
			data     []byte
			encoding string
		)
		if err := rows.Scan(&data, &encoding); err != nil {
			return err
		}
		events = append(events, p.NewDataBlob(data, encoding))
		return nil
	})
	return events, err
}

func readChasmNodes(results pgx.BatchResults) (map[string]p.InternalChasmNode, error) {
	nodes := make(map[string]p.InternalChasmNode)
	err := readRows(results, func(rows pgx.Rows) error {
		var (
			stored                         string
			metadata, data                 []byte
			metadataEncoding, dataEncoding string
		)
		if err := rows.Scan(&stored, &metadata, &metadataEncoding, &data, &dataEncoding); err != nil {
			return err
		}
		path, err := decodeChasmPath(stored)
		if err != nil {
			return err
		}
		nodes[path] = p.InternalChasmNode{
			Metadata: p.NewDataBlob(metadata, metadataEncoding),
			Data:     p.NewDataBlob(data, dataEncoding),
		}
		return nil
	})
	return nodes, err
}

func readSignalsRequested(results pgx.BatchResults) ([]string, error) {
	ids := make([]string, 0)
	err := readRows(results, func(rows pgx.Rows) error {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
		return nil
	})
	return ids, err
}
