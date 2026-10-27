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
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/resolver"
)

func TestUpdateShardFencing(t *testing.T) {
	for _, pluginName := range []string{dsql.PluginName, postgresql.PluginName} {
		t.Run(pluginName, func(t *testing.T) {
			for _, tc := range []struct {
				name            string
				previousRangeID int64
				ownershipLost   bool
				updates         int
				commits         int
				rollbacks       int
			}{
				{
					name:            "stale_owner",
					previousRangeID: 5,
					ownershipLost:   true,
					rollbacks:       1,
				},
				{
					name:            "current_owner",
					previousRangeID: 7,
					updates:         1,
					commits:         1,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					db, fake := newShardTestDB(t, 7)
					factory := persistencesql.NewFactory(config.SQL{
						PluginName:   pluginName,
						DatabaseName: "shard_fencing_test",
						Connect: func(*config.SQL) (*sqlx.DB, error) {
							return db, nil
						},
					}, resolver.NewNoopResolver(), "test", log.NewNoopLogger(), metrics.NoopMetricsHandler, serialization.NewSerializer())
					t.Cleanup(factory.Close)
					store, err := factory.NewShardStore()
					require.NoError(t, err)
					t.Cleanup(store.Close)
					require.Equal(t, pluginName, store.GetName())

					err = store.UpdateShard(t.Context(), &persistence.InternalUpdateShardRequest{
						ShardID:         1,
						PreviousRangeID: tc.previousRangeID,
						RangeID:         tc.previousRangeID + 1,
						ShardInfo:       persistence.NewDataBlob([]byte("shard"), "Proto3"),
					})

					statements, commits, rollbacks := fake.snapshot()
					t.Logf("statements=%q commits=%d rollbacks=%d", statements, commits, rollbacks)
					if tc.ownershipLost {
						var ownershipLost *persistence.ShardOwnershipLostError
						require.ErrorAs(t, err, &ownershipLost)
						require.Equal(t, int32(1), ownershipLost.ShardID)
					} else {
						require.NoError(t, err)
					}
					require.NotEmpty(t, statements)
					require.Equal(t, shardTestLockQuery, statements[0])
					var updates int
					for _, statement := range statements {
						if strings.HasPrefix(statement, "UPDATE shards ") {
							updates++
						}
					}
					require.Equal(t, tc.updates, updates)
					require.Equal(t, tc.commits, commits)
					require.Equal(t, tc.rollbacks, rollbacks)
				})
			}
		})
	}
}

const shardTestLockQuery = "SELECT range_id FROM shards WHERE shard_id = $1 FOR UPDATE"

var shardTestDriverID atomic.Int64

type shardTestDriver struct {
	mu         sync.Mutex
	rangeID    int64
	statements []string
	commits    int
	rollbacks  int
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
	return &shardTestTx{db: c.db}, nil
}

func (c *shardTestConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if c.db.record(query) != shardTestLockQuery {
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
	return &shardTestRows{rangeID: c.db.rangeID}, nil
}

func (c *shardTestConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if !strings.HasPrefix(c.db.record(query), "UPDATE shards ") {
		return nil, fmt.Errorf("unexpected exec: %s", query)
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
	return nil
}

func (tx *shardTestTx) Rollback() error {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()
	tx.db.rollbacks++
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
