package dsql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

// fakeRows serves scripted rows. Its embedded Rows is nil: the read uses none of its other
// methods.
type fakeRows struct {
	pgx.Rows
	rows [][]any
	next int
	err  error
}

func (r *fakeRows) Next() bool {
	if r.err != nil || r.next == len(r.rows) {
		return false
	}
	r.next++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.next-1]
	if len(dest) != len(row) {
		return fmt.Errorf("%d destinations for %d columns", len(dest), len(row))
	}
	for i, d := range dest {
		switch d := d.(type) {
		case nil:
		case *int64:
			*d = row[i].(int64)
		case *string:
			*d = row[i].(string)
		case *[]byte:
			*d = row[i].([]byte)
		default:
			return fmt.Errorf("unexpected destination %T", d)
		}
	}
	return nil
}

func (r *fakeRows) Err() error { return r.err }

func (r *fakeRows) Close() {}

// fakeResult is one statement's outcome: rows, or the error the server returned.
type fakeResult struct {
	rows [][]any
	err  error
}

// fakeBatchResults serves a batch's results in order. As pgx does, it keeps the first
// statement's error, returns it for every statement after, and returns it from Close.
type fakeBatchResults struct {
	results []fakeResult
	next    int
	failed  error
	closed  bool
}

func (b *fakeBatchResults) take() (fakeResult, error) {
	if b.failed != nil {
		return fakeResult{}, b.failed
	}
	r := b.results[b.next]
	b.next++
	b.failed = r.err
	return r, nil
}

func (b *fakeBatchResults) Exec() (pgconn.CommandTag, error) {
	r, err := b.take()
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return pgconn.CommandTag{}, r.err
}

func (b *fakeBatchResults) Query() (pgx.Rows, error) {
	r, err := b.take()
	if err != nil {
		return nil, err
	}
	return &fakeRows{rows: r.rows, err: r.err}, nil
}

func (b *fakeBatchResults) QueryRow() pgx.Row { panic("unused") }

func (b *fakeBatchResults) Close() error {
	b.closed = true
	return b.failed
}

type fakePipelineConn struct {
	results *fakeBatchResults
	batch   *pgx.Batch
	execs   []string
	closed  bool
}

func (c *fakePipelineConn) SendBatch(_ context.Context, b *pgx.Batch) pgx.BatchResults {
	c.batch = b
	return c.results
}

func (c *fakePipelineConn) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	c.execs = append(c.execs, query)
	return pgconn.CommandTag{}, nil
}

func (c *fakePipelineConn) IsClosed() bool { return c.closed }

const testEncoding = "Proto3"

// mutableStateResults scripts a batch whose statements all succeed.
func mutableStateResults() []fakeResult {
	return []fakeResult{
		{}, // BEGIN READ ONLY
		{rows: [][]any{{nil, nil, nil, nil, int64(12), nil, []byte("info"), testEncoding, []byte("state"), testEncoding, int64(3)}}},
		{rows: [][]any{{int64(5), []byte("activity"), testEncoding}}},
		{rows: [][]any{{"timer-1", []byte("timer"), testEncoding}}},
		{rows: [][]any{{int64(6), []byte("child"), testEncoding}}},
		{rows: [][]any{{int64(7), []byte("cancel"), testEncoding}}},
		{rows: [][]any{{int64(8), []byte("signal"), testEncoding}}},
		{rows: [][]any{{[]byte("event-1"), testEncoding}, {[]byte("event-2"), testEncoding}}},
		{rows: [][]any{{encodeChasmPath("node\x00path"), []byte("metadata"), testEncoding, []byte("data"), testEncoding}}},
		{rows: [][]any{{"signal-1"}}},
		{}, // COMMIT
	}
}

func blob(data string) *commonpb.DataBlob {
	return p.NewDataBlob([]byte(data), testEncoding)
}

func newReadTestStore(t *testing.T) (*DSQLExecutionStore, *recordedFacts) {
	t.Helper()
	pdb, facts := newFactsTestDB(t)
	return NewDSQLExecutionStore(nil, pdb, log.NewNoopLogger()), facts
}

func TestPipelineReadSendsOneReadOnlyTransaction(t *testing.T) {
	store, _ := newReadTestStore(t)
	conn := &fakePipelineConn{results: &fakeBatchResults{results: mutableStateResults()}}
	namespaceID, runID := primitives.NewUUID(), primitives.NewUUID()
	state, err := store.pipelineRead(t.Context(), conn, 11, namespaceID, "workflow", runID)
	require.NoError(t, err)

	require.Equal(t, &p.InternalWorkflowMutableState{
		ExecutionInfo:       blob("info"),
		ExecutionState:      blob("state"),
		NextEventID:         12,
		DBRecordVersion:     3,
		ActivityInfos:       map[int64]*commonpb.DataBlob{5: blob("activity")},
		TimerInfos:          map[string]*commonpb.DataBlob{"timer-1": blob("timer")},
		ChildExecutionInfos: map[int64]*commonpb.DataBlob{6: blob("child")},
		RequestCancelInfos:  map[int64]*commonpb.DataBlob{7: blob("cancel")},
		SignalInfos:         map[int64]*commonpb.DataBlob{8: blob("signal")},
		BufferedEvents:      []*commonpb.DataBlob{blob("event-1"), blob("event-2")},
		ChasmNodes:          map[string]p.InternalChasmNode{"node\x00path": {Metadata: blob("metadata"), Data: blob("data")}},
		SignalRequestedIDs:  []string{"signal-1"},
	}, state)

	queued := conn.batch.QueuedQueries
	require.Len(t, queued, len(mutableStateQueries)+2)
	require.Equal(t, "BEGIN READ ONLY", queued[0].SQL)
	for i, query := range mutableStateQueries {
		require.Equal(t, query, queued[i+1].SQL)
		require.Equal(t, []any{int32(11), namespaceID.String(), "workflow", runID.String()}, queued[i+1].Arguments)
	}
	require.Equal(t, "COMMIT", queued[len(queued)-1].SQL)
	require.True(t, conn.results.closed)
	require.Empty(t, conn.execs)
}

func TestPipelineReadOfAnEmptyWorkflow(t *testing.T) {
	store, _ := newReadTestStore(t)
	results := mutableStateResults()
	for i := 2; i < len(results)-1; i++ {
		results[i] = fakeResult{}
	}
	conn := &fakePipelineConn{results: &fakeBatchResults{results: results}}
	state, err := store.pipelineRead(t.Context(), conn, 1, primitives.NewUUID(), "workflow", primitives.NewUUID())
	require.NoError(t, err)
	// As the common store returns them: empty maps and signals, and no buffered events.
	require.Empty(t, state.ActivityInfos)
	require.NotNil(t, state.ActivityInfos)
	require.NotNil(t, state.TimerInfos)
	require.NotNil(t, state.ChildExecutionInfos)
	require.NotNil(t, state.RequestCancelInfos)
	require.NotNil(t, state.SignalInfos)
	require.NotNil(t, state.ChasmNodes)
	require.Equal(t, []string{}, state.SignalRequestedIDs)
	require.Nil(t, state.BufferedEvents)
}

func TestPipelineReadOfAMissingExecution(t *testing.T) {
	store, _ := newReadTestStore(t)
	results := mutableStateResults()
	results[1] = fakeResult{}
	conn := &fakePipelineConn{results: &fakeBatchResults{results: results}}
	runID := primitives.NewUUID()
	_, err := store.pipelineRead(t.Context(), conn, 1, primitives.NewUUID(), "workflow", runID)
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, fmt.Sprintf("Workflow executionsRow not found. WorkflowId: workflow, RunId: %v", runID), notFound.Message)
	require.True(t, conn.results.closed)
	require.Empty(t, conn.execs)
}

func TestPipelineReadRollsBackAfterAFailedStatement(t *testing.T) {
	for _, tc := range []struct {
		statement int
		message   string
	}{
		{0, "GetWorkflowExecution: failed to start transaction. Error: "},
		{1, "GetWorkflowExecution: failed. Error: "},
		{2, "GetWorkflowExecution: failed to get activity info. Error: "},
		{3, "GetWorkflowExecution: failed to get timer info. Error: "},
		{4, "GetWorkflowExecution: failed to get child execution info. Error: "},
		{5, "GetWorkflowExecution: failed to get request cancel info. Error: "},
		{6, "GetWorkflowExecution: failed to get signal info. Error: "},
		{7, "GetWorkflowExecution: failed to get buffered events. Error: "},
		{8, "GetWorkflowExecution: failed to get CHASM nodes. Error: "},
		{9, "GetWorkflowExecution: failed to get signals requested. Error: "},
		{10, "GetWorkflowExecution: failed to commit transaction. Error: "},
	} {
		t.Run(fmt.Sprint(tc.statement), func(t *testing.T) {
			store, _ := newReadTestStore(t)
			results := mutableStateResults()
			pgErr := &pgconn.PgError{Code: "XX000", Message: "internal error"}
			results[tc.statement] = fakeResult{err: pgErr}
			conn := &fakePipelineConn{results: &fakeBatchResults{results: results}}
			_, err := store.pipelineRead(t.Context(), conn, 1, primitives.NewUUID(), "workflow", primitives.NewUUID())
			var unavailable *serviceerror.Unavailable
			require.ErrorAs(t, err, &unavailable)
			require.Equal(t, tc.message+pgErr.Error(), unavailable.Message)
			require.True(t, conn.results.closed)
			require.Equal(t, []string{"ROLLBACK"}, conn.execs)
		})
	}
}

func TestPipelineReadOnALostConnection(t *testing.T) {
	store, _ := newReadTestStore(t)
	results := mutableStateResults()
	results[2] = fakeResult{err: io.ErrUnexpectedEOF}
	conn := &fakePipelineConn{results: &fakeBatchResults{results: results}, closed: true}
	_, err := store.pipelineRead(t.Context(), conn, 1, primitives.NewUUID(), "workflow", primitives.NewUUID())
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, "GetWorkflowExecution: failed to get activity info. Error: database connection lost: unexpected EOF", unavailable.Message)
	require.Empty(t, conn.execs)
}

func TestPipelineReadOfAnUndecodableChasmPath(t *testing.T) {
	store, _ := newReadTestStore(t)
	results := mutableStateResults()
	results[8] = fakeResult{rows: [][]any{{"bad\x01\x09path", []byte("metadata"), testEncoding, []byte("data"), testEncoding}}}
	conn := &fakePipelineConn{results: &fakeBatchResults{results: results}}
	_, err := store.pipelineRead(t.Context(), conn, 1, primitives.NewUUID(), "workflow", primitives.NewUUID())
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.Contains(t, unavailable.Message, "GetWorkflowExecution: failed to get CHASM nodes. Error: ")
	require.Contains(t, unavailable.Message, "unknown escape")
	// The server ran every statement, so closing the batch ended the transaction.
	require.True(t, conn.results.closed)
	require.Empty(t, conn.execs)
}

// readTestConnector hands database/sql the connection, or the error, it's given.
type readTestConnector struct {
	conn sqldriver.Conn
	err  error
}

func (c readTestConnector) Connect(context.Context) (sqldriver.Conn, error) { return c.conn, c.err }

func (c readTestConnector) Driver() sqldriver.Driver { return nil }

// plainConn is a driver connection that isn't pgx's.
type plainConn struct {
	closes *int
}

func (plainConn) Prepare(string) (sqldriver.Stmt, error) { return nil, errors.New("unused") }

func (plainConn) Begin() (sqldriver.Tx, error) { return nil, errors.New("unused") }

func (c plainConn) Close() error {
	*c.closes++
	return nil
}

// baseReadStore stands in for the common store.
type baseReadStore struct {
	p.ExecutionStore
	read func()
}

func (s baseReadStore) GetWorkflowExecution(context.Context, *p.GetWorkflowExecutionRequest) (*p.InternalGetWorkflowExecutionResponse, error) {
	s.read()
	return &p.InternalGetWorkflowExecutionResponse{DBRecordVersion: 42}, nil
}

func newConnectorTestStore(t *testing.T, base p.ExecutionStore, connector sqldriver.Connector) (*DSQLExecutionStore, *recordedFacts) {
	t.Helper()
	sqlDB := sql.OpenDB(connector)
	sqlDB.SetMaxIdleConns(0)
	t.Cleanup(func() { _ = sqlDB.Close() })
	handle := sqlplugin.NewDatabaseHandle(sqlplugin.DbKindMain, func() (*sqlx.DB, error) {
		return sqlx.NewDb(sqlDB, "pgx"), nil
	}, func(error) bool { return false }, log.NewNoopLogger(), metrics.NoopMetricsHandler, clock.NewRealTimeSource())
	pdb := newDB(sqlplugin.DbKindMain, "", nil, handle, nil)
	facts := &recordedFacts{}
	pdb.SetStoreFacts(facts)
	return NewDSQLExecutionStore(base, pdb, log.NewNoopLogger()), facts
}

func readRequest() *p.GetWorkflowExecutionRequest {
	return &p.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: primitives.NewUUID().String(),
		WorkflowID:  "workflow",
		RunID:       primitives.NewUUID().String(),
	}
}

func TestGetWorkflowExecutionKeepsTheRefusal(t *testing.T) {
	store, facts := newConnectorTestStore(t, nil, readTestConnector{err: errGateRefused})
	_, err := store.GetWorkflowExecution(t.Context(), readRequest())
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, exhausted.Cause)
	require.Equal(t, map[p.StoreFact]int64{p.StoreFactThrottled: 1}, facts.snapshot())
}

func TestGetWorkflowExecutionReportsAFailedAcquireAsTheCommonStoreDoes(t *testing.T) {
	store, _ := newConnectorTestStore(t, nil, readTestConnector{err: errors.New("pool closed")})
	_, err := store.GetWorkflowExecution(t.Context(), readRequest())
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, "GetWorkflowExecution: failed. Error: pool closed", unavailable.Message)
}

func TestPGXConnOfAGatedConnection(t *testing.T) {
	_, ok := pgxConnOf(&gatedConn{Conn: &stdlib.Conn{}})
	require.True(t, ok)
	_, ok = pgxConnOf(&stdlib.Conn{})
	require.True(t, ok)
	_, ok = pgxConnOf(plainConn{})
	require.False(t, ok)
}

func TestGetWorkflowExecutionFallsBackWithoutPGX(t *testing.T) {
	var closes, closesAtRead int
	base := baseReadStore{read: func() { closesAtRead = closes }}
	store, _ := newConnectorTestStore(t, base, readTestConnector{conn: plainConn{closes: &closes}})
	response, err := store.GetWorkflowExecution(t.Context(), readRequest())
	require.NoError(t, err)
	require.Equal(t, int64(42), response.DBRecordVersion)
	require.Equal(t, 1, closesAtRead, "the read's connection should be given back before the common store reads")
}
