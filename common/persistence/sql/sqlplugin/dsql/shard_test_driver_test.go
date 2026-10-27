package dsql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

const (
	shardTestLockQuery         = "SELECT range_id FROM shards WHERE shard_id = $1 FOR UPDATE"
	shardTestReadLockQuery     = "SELECT range_id FROM shards WHERE shard_id = $1 FOR KEY SHARE"
	bufferedEventsTestMaxQuery = "SELECT COALESCE(MAX(id), 0) FROM buffered_events WHERE shard_id = $1 AND namespace_id = $2 AND workflow_id = $3 AND run_id = $4"
)

var shardTestDriverID atomic.Int64

var notificationVersionCondition = regexp.MustCompile(`\bnotification_version\s*=\s*\$(\d+)`)

type shardTestDriver struct {
	mu                  sync.Mutex
	rangeID             int64
	statements          []string
	commits             int
	rollbacks           int
	begins              int
	beginErrors         []error
	commitErrors        []error
	commitError         error
	execErrors          []error
	onCommit            func()
	onRollback          func()
	arguments           [][]driver.Value
	versions            map[string]int64
	bufferedEventMaxIDs map[bufferedEventsTestRun]int64
	queryErrors         []error
}

type bufferedEventsTestRun struct {
	shardID     int64
	namespaceID string
	workflowID  string
	runID       string
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

func (d *shardTestDriver) record(query string, args ...driver.NamedValue) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	query = strings.Join(strings.Fields(query), " ")
	d.statements = append(d.statements, query)
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	d.arguments = append(d.arguments, values)
	return query
}

func (d *shardTestDriver) statementArgs(index int) []driver.Value {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]driver.Value(nil), d.arguments[index]...)
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

func (c *shardTestConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	query = c.db.record(query, args...)
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	if len(c.db.queryErrors) > 0 {
		err := c.db.queryErrors[0]
		c.db.queryErrors = c.db.queryErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	switch query {
	case bufferedEventsTestMaxQuery:
		run := bufferedEventsTestRun{
			shardID:     args[0].Value.(int64),
			namespaceID: args[1].Value.(string),
			workflowID:  args[2].Value.(string),
			runID:       args[3].Value.(string),
		}
		return &shardTestRows{column: "coalesce", value: c.db.bufferedEventMaxIDs[run]}, nil
	case shardTestLockQuery, shardTestReadLockQuery, "SELECT range_id FROM shards WHERE shard_id = $1":
		return &shardTestRows{column: "range_id", value: c.db.rangeID}, nil
	case "SELECT notification_version FROM namespace_metadata WHERE partition_id=$1 FOR UPDATE":
		return &shardTestRows{column: "notification_version", value: c.db.versions["namespace_metadata"]}, nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
}

func (c *shardTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	query = c.db.record(query, args...)
	if !strings.HasPrefix(query, "UPDATE shards ") &&
		!strings.HasPrefix(query, "UPDATE namespaces ") &&
		!strings.HasPrefix(query, "UPDATE namespace_metadata ") &&
		!strings.HasPrefix(query, "INSERT INTO buffered_events(") {
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
	// Model only version predicates against the fixture's stored table versions.
	table := strings.Fields(query)[1]
	if version, ok := c.db.versions[table]; ok {
		_, where, _ := strings.Cut(query, " WHERE ")
		if match := notificationVersionCondition.FindStringSubmatch(where); match != nil {
			index, err := strconv.Atoi(match[1])
			if err != nil || index < 1 || index > len(args) {
				return nil, fmt.Errorf("invalid notification_version argument: %s", match[1])
			}
			if args[index-1].Value != version {
				return driver.RowsAffected(0), nil
			}
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
	column string
	value  int64
	done   bool
}

func (r *shardTestRows) Columns() []string { return []string{r.column} }
func (r *shardTestRows) Close() error      { return nil }

func (r *shardTestRows) Next(values []driver.Value) error {
	if r.done {
		return io.EOF
	}
	values[0] = r.value
	r.done = true
	return nil
}

func (d *shardTestDriver) beginCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.begins
}
