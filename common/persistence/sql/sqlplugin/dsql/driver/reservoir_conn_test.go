package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mockConnFull is a full mock connection implementing optional interfaces
type mockConnFull struct {
	prepareCalled bool
	beginCalled   bool
	closeCalled   bool
	pingCalled    bool
	resetCalled   bool
	isValidCalled bool
	execCalled    bool
	queryCalled   bool
	beginTxCalled bool

	prepareErr error
	beginErr   error
	closeErr   error
	pingErr    error
	resetErr   error
	isValid    bool
	execErr    error
	queryErr   error
	beginTxErr error
}

func (m *mockConnFull) Prepare(query string) (driver.Stmt, error) {
	m.prepareCalled = true
	return nil, m.prepareErr
}

func (m *mockConnFull) Close() error {
	m.closeCalled = true
	return m.closeErr
}

func (m *mockConnFull) Begin() (driver.Tx, error) {
	m.beginCalled = true
	return nil, m.beginErr
}

func (m *mockConnFull) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	m.beginTxCalled = true
	return nil, m.beginTxErr
}

func (m *mockConnFull) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	m.execCalled = true
	return nil, m.execErr
}

func (m *mockConnFull) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	m.queryCalled = true
	return nil, m.queryErr
}

func (m *mockConnFull) Ping(ctx context.Context) error {
	m.pingCalled = true
	return m.pingErr
}

func (m *mockConnFull) ResetSession(ctx context.Context) error {
	m.resetCalled = true
	return m.resetErr
}

func (m *mockConnFull) IsValid() bool {
	m.isValidCalled = true
	return m.isValid
}

// TestReservoirConn_Close tests that Close returns connection to reservoir
func TestReservoirConn_Close(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	require.Equal(t, 0, res.Len())

	err := conn.Close()
	require.NoError(t, err)
	require.Equal(t, 1, res.Len())
}

// TestReservoirConn_Close_BadConnection tests that bad connections are discarded
func TestReservoirConn_Close_BadConnection(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	mockConn := &mockConnFull{prepareErr: driver.ErrBadConn}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)

	// Trigger bad connection flag
	_, _ = conn.Prepare("SELECT 1")

	err := conn.Close()
	require.NoError(t, err)

	// When a connection is marked bad, Close() sets Lifetime = 0.
	// The Return() method discards connections with remaining lifetime = 0,
	// so the bad connection should be discarded, not returned to the reservoir.
	require.Equal(t, 0, res.Len())
	require.Equal(t, int64(1), metrics.discards.Load())
	require.Contains(t, metrics.getDiscardReasons(), "expired_on_return")
}

// TestReservoirConn_DoubleClose tests that double close is idempotent
func TestReservoirConn_DoubleClose(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)

	// First close
	err := conn.Close()
	require.NoError(t, err)
	require.Equal(t, 1, res.Len())

	// Second close should be no-op
	err = conn.Close()
	require.NoError(t, err)
	require.Equal(t, 1, res.Len()) // Still 1, not 2
}

// TestReservoirConn_Prepare tests Prepare forwarding
func TestReservoirConn_Prepare(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, _ = conn.Prepare("SELECT 1")

	require.True(t, mockConn.prepareCalled)
}

// TestReservoirConn_Begin tests Begin forwarding
func TestReservoirConn_Begin(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, _ = conn.Begin()

	require.True(t, mockConn.beginCalled)
}

// TestReservoirConn_BeginTx tests BeginTx forwarding
func TestReservoirConn_BeginTx(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, _ = conn.BeginTx(context.Background(), driver.TxOptions{})

	require.True(t, mockConn.beginTxCalled)
}

// TestReservoirConn_ExecContext tests ExecContext forwarding
func TestReservoirConn_ExecContext(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, _ = conn.ExecContext(context.Background(), "INSERT INTO test VALUES (1)", nil)

	require.True(t, mockConn.execCalled)
}

// TestReservoirConn_QueryContext tests QueryContext forwarding
func TestReservoirConn_QueryContext(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, _ = conn.QueryContext(context.Background(), "SELECT 1", nil)

	require.True(t, mockConn.queryCalled)
}

// TestReservoirConn_Ping tests Ping forwarding
func TestReservoirConn_Ping(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_ = conn.Ping(context.Background())

	require.True(t, mockConn.pingCalled)
}

// TestReservoirConn_ResetSession tests ResetSession forwarding
func TestReservoirConn_ResetSession(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_ = conn.ResetSession(context.Background())

	require.True(t, mockConn.resetCalled)
}

// TestReservoirConn_IsValid tests IsValid forwarding
func TestReservoirConn_IsValid(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{isValid: true}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	valid := conn.IsValid()

	require.True(t, mockConn.isValidCalled)
	require.True(t, valid)
}

// TestReservoirConn_IsValid_False tests IsValid returning false marks connection bad
func TestReservoirConn_IsValid_False(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{isValid: false}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	valid := conn.IsValid()

	require.False(t, valid)
	require.True(t, conn.bad.Load())
}

// TestReservoirConn_BadConnError_Prepare tests that ErrBadConn from Prepare marks connection bad
func TestReservoirConn_BadConnError_Prepare(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{prepareErr: driver.ErrBadConn}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, err := conn.Prepare("SELECT 1")

	require.Equal(t, driver.ErrBadConn, err)
	require.True(t, conn.bad.Load())
}

// TestReservoirConn_BadConnError_Begin tests that ErrBadConn from Begin marks connection bad
func TestReservoirConn_BadConnError_Begin(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{beginErr: driver.ErrBadConn}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, err := conn.Begin()

	require.Equal(t, driver.ErrBadConn, err)
	require.True(t, conn.bad.Load())
}

// TestReservoirConn_BadConnError_Ping tests that ErrBadConn from Ping marks connection bad
func TestReservoirConn_BadConnError_Ping(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{pingErr: driver.ErrBadConn}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	err := conn.Ping(context.Background())

	require.Equal(t, driver.ErrBadConn, err)
	require.True(t, conn.bad.Load())
}

// TestReservoirConn_Close_NilReservoir tests Close with nil reservoir
func TestReservoirConn_Close_NilReservoir(t *testing.T) {
	conn := &reservoirConn{
		r:  nil,
		pc: &PhysicalConn{},
	}

	err := conn.Close()
	require.NoError(t, err)
}

// TestReservoirConn_Close_NilPhysicalConn tests Close with nil physical connection
func TestReservoirConn_Close_NilPhysicalConn(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)
	conn := &reservoirConn{
		r:  res,
		pc: nil,
	}

	err := conn.Close()
	require.NoError(t, err)
}

// TestReservoirConn_ExecContext_NoInterface tests ExecContext when underlying doesn't implement it
func TestReservoirConn_ExecContext_NoInterface(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	// Use basic mockConn that doesn't implement ExecerContext
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, err := conn.ExecContext(context.Background(), "INSERT INTO test VALUES (1)", nil)

	require.Equal(t, driver.ErrSkip, err)
}

// TestReservoirConn_QueryContext_NoInterface tests QueryContext when underlying doesn't implement it
func TestReservoirConn_QueryContext_NoInterface(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	// Use basic mockConn that doesn't implement QueryerContext
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, err := conn.QueryContext(context.Background(), "SELECT 1", nil)

	require.Equal(t, driver.ErrSkip, err)
}

// TestReservoirConn_Ping_NoInterface tests Ping when underlying doesn't implement it
func TestReservoirConn_Ping_NoInterface(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	// Use basic mockConn that doesn't implement Pinger
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	err := conn.Ping(context.Background())

	require.NoError(t, err) // Returns nil when not implemented
}

// TestReservoirConn_ResetSession_NoInterface tests ResetSession when underlying doesn't implement it
func TestReservoirConn_ResetSession_NoInterface(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	// Use basic mockConn that doesn't implement SessionResetter
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	err := conn.ResetSession(context.Background())

	require.NoError(t, err) // Returns nil when not implemented
}

// TestReservoirConn_IsValid_NoInterface tests IsValid when underlying doesn't implement it
func TestReservoirConn_IsValid_NoInterface(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	// Use basic mockConn that doesn't implement Validator
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	valid := conn.IsValid()

	require.True(t, valid) // Returns true when not implemented
}

// TestReservoirConn_BeginTx_NoInterface tests BeginTx fallback to Begin
func TestReservoirConn_BeginTx_NoInterface(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	// Use basic mockConn that doesn't implement ConnBeginTx
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, err := conn.BeginTx(context.Background(), driver.TxOptions{})

	// Should fall back to Begin() which returns nil, nil for mockConn
	require.NoError(t, err)
}

// =============================================================================
// Edge Case Tests
// =============================================================================

// TestReservoirConn_BadConnError_ExecContext tests that ErrBadConn from ExecContext marks connection bad
func TestReservoirConn_BadConnError_ExecContext(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{execErr: driver.ErrBadConn}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, err := conn.ExecContext(context.Background(), "INSERT INTO test VALUES (1)", nil)

	require.Equal(t, driver.ErrBadConn, err)
	require.True(t, conn.bad.Load())
}

// TestReservoirConn_BadConnError_QueryContext tests that ErrBadConn from QueryContext marks connection bad
func TestReservoirConn_BadConnError_QueryContext(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{queryErr: driver.ErrBadConn}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, err := conn.QueryContext(context.Background(), "SELECT 1", nil)

	require.Equal(t, driver.ErrBadConn, err)
	require.True(t, conn.bad.Load())
}

// TestReservoirConn_BadConnError_BeginTx tests that ErrBadConn from BeginTx marks connection bad
func TestReservoirConn_BadConnError_BeginTx(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{beginTxErr: driver.ErrBadConn}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, err := conn.BeginTx(context.Background(), driver.TxOptions{})

	require.Equal(t, driver.ErrBadConn, err)
	require.True(t, conn.bad.Load())
}

// TestReservoirConn_BadConnError_ResetSession tests that ErrBadConn from ResetSession marks connection bad
func TestReservoirConn_BadConnError_ResetSession(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{resetErr: driver.ErrBadConn}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	err := conn.ResetSession(context.Background())

	require.Equal(t, driver.ErrBadConn, err)
	require.True(t, conn.bad.Load())
}

// TestReservoirConn_NonBadConnError tests that non-ErrBadConn errors don't mark connection bad
func TestReservoirConn_NonBadConnError(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	otherErr := errors.New("some other error")
	mockConn := &mockConnFull{prepareErr: otherErr}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	_, err := conn.Prepare("SELECT 1")

	require.Equal(t, otherErr, err)
	require.False(t, conn.bad.Load())
}

// TestReservoirConn_BeginTx_WithOptions tests BeginTx with non-default options
func TestReservoirConn_BeginTx_WithOptions(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	opts := driver.TxOptions{
		Isolation: driver.IsolationLevel(2), // Serializable
		ReadOnly:  true,
	}
	_, _ = conn.BeginTx(context.Background(), opts)

	require.True(t, mockConn.beginTxCalled)
}

// TestReservoirConn_MultipleOperations tests multiple operations on same connection
func TestReservoirConn_MultipleOperations(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{isValid: true}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)

	// Multiple operations should all work
	_, _ = conn.Prepare("SELECT 1")
	require.True(t, mockConn.prepareCalled)

	_, _ = conn.Begin()
	require.True(t, mockConn.beginCalled)

	_ = conn.Ping(context.Background())
	require.True(t, mockConn.pingCalled)

	valid := conn.IsValid()
	require.True(t, valid)
	require.True(t, mockConn.isValidCalled)
}

// TestReservoirConn_Close_AfterBadOperation tests Close after a bad operation
func TestReservoirConn_Close_AfterBadOperation(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	mockConn := &mockConnFull{pingErr: driver.ErrBadConn}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)

	// Trigger bad connection
	_ = conn.Ping(context.Background())
	require.True(t, conn.bad.Load())

	// Close should discard the connection
	err := conn.Close()
	require.NoError(t, err)

	// Connection should be discarded, not returned to reservoir
	require.Equal(t, 0, res.Len())
}

// TestReservoirConn_ContextCancellation tests operations with cancelled context
func TestReservoirConn_ContextCancellation(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Operations should still be called (context handling is up to underlying driver)
	_, _ = conn.ExecContext(ctx, "INSERT INTO test VALUES (1)", nil)
	require.True(t, mockConn.execCalled)
}

// TestReservoirConn_Close_ReturnsToReservoir_WithMetrics tests Close with metrics tracking
func TestReservoirConn_Close_ReturnsToReservoir_WithMetrics(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)
	err := conn.Close()
	require.NoError(t, err)

	// Connection should be returned
	require.Equal(t, 1, res.Len())
	// Size metric should be recorded
	require.Equal(t, int64(1), metrics.sizeRecorded.Load())
}

// mockConnFull with errors import
var _ = errors.New // Ensure errors is imported

// TestReservoirConn_IsValid_MarkedBadPreviously tests IsValid when already marked bad
func TestReservoirConn_IsValid_MarkedBadPreviously(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	mockConn := &mockConnFull{isValid: true}
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	conn := newReservoirConn(res, pc)

	// Mark as bad manually
	conn.bad.Store(true)

	// IsValid should still call underlying and return its result
	// but the bad flag is already set
	valid := conn.IsValid()
	require.True(t, valid) // Underlying returns true
	require.True(t, conn.bad.Load())
}
