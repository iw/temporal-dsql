package dsql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

const (
	shardTestLockQuery     = "SELECT range_id FROM shards WHERE shard_id = $1 FOR UPDATE"
	shardTestReadLockQuery = "SELECT range_id FROM shards WHERE shard_id = $1 FOR KEY SHARE"
)

var shardTestDriverID atomic.Int64

type shardTestDriver struct {
	mu           sync.Mutex
	rangeID      int64
	statements   []string
	commits      int
	rollbacks    int
	begins       int
	beginErrors  []error
	commitErrors []error
	commitError  error
	execErrors   []error
	onCommit     func()
	onRollback   func()
}

func newShardTestDB(t *testing.T, rangeID int64) (*sqlx.DB, *shardTestDriver) {
	t.Helper()
	fake := &shardTestDriver{rangeID: rangeID}
	name := fmt.Sprintf("shard-fencing-%d", shardTestDriverID.Add(1))
	sql.Register(name, fake)
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return sqlx.NewDb(db, "postgres"), fake
}

func (d *shardTestDriver) Open(string) (driver.Conn, error) {
	return &shardTestConn{db: d}, nil
}

func (d *shardTestDriver) record(query string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	query = strings.Join(strings.Fields(query), " ")
	d.statements = append(d.statements, query)
	return query
}

func (d *shardTestDriver) snapshot() (statements []string, commits, rollbacks int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.statements...), d.commits, d.rollbacks
}

type shardTestConn struct {
	db *shardTestDriver
}

func (c *shardTestConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected prepare: %s", c.db.record(query))
}

func (c *shardTestConn) Close() error { return nil }

func (c *shardTestConn) Begin() (driver.Tx, error) {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	c.db.begins++
	if len(c.db.beginErrors) > 0 {
		err := c.db.beginErrors[0]
		c.db.beginErrors = c.db.beginErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	return &shardTestTx{db: c.db}, nil
}

func (c *shardTestConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch c.db.record(query) {
	case shardTestLockQuery, shardTestReadLockQuery, "SELECT range_id FROM shards WHERE shard_id = $1":
		return &shardTestRows{rangeID: c.db.rangeID}, nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
}

func (c *shardTestConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if !strings.HasPrefix(c.db.record(query), "UPDATE shards ") {
		return nil, fmt.Errorf("unexpected exec: %s", query)
	}
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	if len(c.db.execErrors) > 0 {
		err := c.db.execErrors[0]
		c.db.execErrors = c.db.execErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(1), nil
}

type shardTestTx struct {
	db *shardTestDriver
}

func (tx *shardTestTx) Commit() error {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()
	tx.db.commits++
	if tx.db.onCommit != nil {
		tx.db.onCommit()
	}
	if len(tx.db.commitErrors) > 0 {
		err := tx.db.commitErrors[0]
		tx.db.commitErrors = tx.db.commitErrors[1:]
		return err
	}
	return tx.db.commitError
}

func (tx *shardTestTx) Rollback() error {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()
	tx.db.rollbacks++
	if tx.db.onRollback != nil {
		tx.db.onRollback()
	}
	return nil
}

type shardTestRows struct {
	rangeID int64
	done    bool
}

func (r *shardTestRows) Columns() []string { return []string{"range_id"} }
func (r *shardTestRows) Close() error      { return nil }

func (r *shardTestRows) Next(values []driver.Value) error {
	if r.done {
		return io.EOF
	}
	values[0] = r.rangeID
	r.done = true
	return nil
}

func (d *shardTestDriver) beginCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.begins
}
