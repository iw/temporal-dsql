package persistencetests

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

func TestParseDSQLTestSchemas(t *testing.T) {
	schemas, err := parseDSQLTestSchemas(" corpus_0, corpus_1,,corpus_0 ")
	require.NoError(t, err)
	require.Equal(t, []string{"corpus_0", "corpus_1"}, schemas)

	for _, value := range []string{"", " , ", "Corpus_0", "corpus-0", "corpus_0;drop", `"corpus_0"`, "corpus_0 corpus_1"} {
		_, err := parseDSQLTestSchemas(value)
		require.Error(t, err, value)
	}
}

func setDSQLTestEnv(t *testing.T, schemas, visibilitySchema string) {
	t.Setenv(dsqlEndpointEnv, "example.dsql.eu-west-1.on.aws")
	t.Setenv(dsqlUserEnv, "tester")
	t.Setenv(dsqlRegionEnv, "")
	t.Setenv(dsqlSchemasEnv, schemas)
	t.Setenv(dsqlVisibilitySchemaEnv, visibilitySchema)
}

func TestDSQLTestClusterConfig(t *testing.T) {
	setDSQLTestEnv(t, "corpus_0,corpus_1", "")
	options := GetTestClusterOption(config.StoreTypeSQL, dsqlPluginName)
	// The cross-cluster tests name a database per cluster; DSQL serves one, and each
	// cluster has its own schema instead.
	options.DBName += "_standby"
	options.FaultInjection = &config.FaultInjection{}
	c, err := newDSQLTestCluster(options, log.NewNoopLogger())
	require.NoError(t, err)
	require.Panics(t, func() { c.Config() })

	c.lease = &dsqlSchemaLease{schema: "corpus_1", release: func() {}}
	cfg := c.Config()
	require.Equal(t, "test", cfg.DefaultStore)
	require.Equal(t, dsqlVisibilityStoreName, cfg.VisibilityStore)
	require.Equal(t, dsqlTransactionSizeLimit, cfg.TransactionSizeLimit())

	mainStore, visibilityStore := cfg.DataStores[cfg.DefaultStore], cfg.DataStores[cfg.VisibilityStore]
	for _, store := range []config.DataStore{mainStore, visibilityStore} {
		require.Same(t, options.FaultInjection, store.FaultInjection)
		require.Equal(t, dsqlPluginName, store.SQL.PluginName)
		require.Equal(t, "postgres", store.SQL.DatabaseName)
		require.Equal(t, "example.dsql.eu-west-1.on.aws:5432", store.SQL.ConnectAddr)
		require.Equal(t, "tester", store.SQL.User)
		require.Equal(t, dsqlTestMaxConns, store.SQL.MaxConns)
		require.Equal(t, dsqlTestConnectionRate, store.SQL.ConnectAttributes["connection_rate_limit"])
		require.NotContains(t, store.SQL.ConnectAttributes, "region")
	}
	require.Equal(t, "corpus_1", mainStore.SQL.ConnectAttributes["search_path"])
	require.Equal(t, "temporal_visibility", visibilityStore.SQL.ConnectAttributes["search_path"])

	// Each call returns copies.
	mainStore.SQL.ConnectAttributes["search_path"] = "public"
	visibilityStore.SQL.ConnectAttributes["search_path"] = "public"
	cfg = c.Config()
	require.Equal(t, "corpus_1", cfg.DataStores[cfg.DefaultStore].SQL.ConnectAttributes["search_path"])
	require.Equal(t, "temporal_visibility", cfg.DataStores[cfg.VisibilityStore].SQL.ConnectAttributes["search_path"])

	c.TearDownTestDatabase()
	require.Panics(t, func() { c.Config() })
}

func TestDSQLTestClusterSettings(t *testing.T) {
	setDSQLTestEnv(t, "corpus_0", "corpus_visibility")
	t.Setenv(dsqlRegionEnv, "eu-west-1")
	options := GetDSQLTestClusterOption()
	options.ConnectAttributes = map[string]string{"connection_rate_limit": "5"}
	c, err := newDSQLTestCluster(options, log.NewNoopLogger())
	require.NoError(t, err)
	c.lease = &dsqlSchemaLease{schema: "corpus_0", release: func() {}}
	cfg := c.Config()
	for _, store := range cfg.DataStores {
		require.Equal(t, "eu-west-1", store.SQL.ConnectAttributes["region"])
		require.Equal(t, "5", store.SQL.ConnectAttributes["connection_rate_limit"])
	}
	require.Equal(t, "corpus_visibility", cfg.DataStores[cfg.VisibilityStore].SQL.ConnectAttributes["search_path"])
}

func TestNewDSQLTestClusterRejectsSettings(t *testing.T) {
	for name, tc := range map[string]struct {
		endpoint, schemas, visibility, want string
	}{
		"no endpoint":                {"", "corpus_0", "", dsqlEndpointEnv},
		"no schemas":                 {"example.dsql.eu-west-1.on.aws", "", "", dsqlSchemasEnv},
		"visibility schema leased":   {"example.dsql.eu-west-1.on.aws", "corpus_0,temporal_visibility", "", "visibility schema"},
		"visibility schema not name": {"example.dsql.eu-west-1.on.aws", "corpus_0", "Visibility", dsqlVisibilitySchemaEnv},
	} {
		t.Run(name, func(t *testing.T) {
			setDSQLTestEnv(t, tc.schemas, tc.visibility)
			t.Setenv(dsqlEndpointEnv, tc.endpoint)
			_, err := newDSQLTestCluster(GetDSQLTestClusterOption(), log.NewNoopLogger())
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestDSQLSchemaLease(t *testing.T) {
	dir := t.TempDir()
	defaultDir := dsqlLockDir
	dsqlLockDir = func() string { return dir }
	t.Cleanup(func() { dsqlLockDir = defaultDir })
	logger := log.NewNoopLogger()
	schemas := []string{"corpus_0", "corpus_1"}

	first, err := leaseDSQLSchema(t.Context(), "one.example", schemas, logger)
	require.NoError(t, err)
	second, err := leaseDSQLSchema(t.Context(), "one.example", schemas, logger)
	require.NoError(t, err)
	require.Equal(t, "corpus_0", first.schema)
	require.Equal(t, "corpus_1", second.schema)

	done, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = leaseDSQLSchema(done, "one.example", schemas, logger)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, dsqlSchemasEnv)

	// Another cluster's schemas are its own.
	other, err := leaseDSQLSchema(t.Context(), "two.example", schemas, logger)
	require.NoError(t, err)
	require.Equal(t, "corpus_0", other.schema)

	first.release()
	first.release()
	again, err := leaseDSQLSchema(t.Context(), "one.example", schemas, logger)
	require.NoError(t, err)
	require.Equal(t, "corpus_0", again.schema)

	for _, lease := range []*dsqlSchemaLease{second, other, again} {
		lease.release()
	}
	require.Empty(t, dsqlLeases.held)
}

func TestLockDSQLSchemaFile(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("lock files are taken on darwin and linux")
	}
	path := filepath.Join(t.TempDir(), "schema.lock")
	unlock, ok, err := lockDSQLSchemaFile(path)
	require.NoError(t, err)
	require.True(t, ok)

	// A second open file can't take the lock, as another process's couldn't.
	_, ok, err = lockDSQLSchemaFile(path)
	require.NoError(t, err)
	require.False(t, ok)

	unlock()
	unlock, ok, err = lockDSQLSchemaFile(path)
	require.NoError(t, err)
	require.True(t, ok)
	unlock()
}

func TestDSQLSeedStatements(t *testing.T) {
	seeds, err := dsqlSeedStatements()
	require.NoError(t, err)
	require.Len(t, seeds, 1)
	require.Contains(t, seeds[0], "namespace_metadata")
	require.Contains(t, seeds[0], "54321")
}

// fakeDSQLSchemaDB deletes counted rows, failing a delete of more than limit rows as
// DSQL fails a transaction that modifies too many.
type fakeDSQLSchemaDB struct {
	rows       map[string]int
	keys       map[string][]string
	limit      int
	statements []string
}

var fakeDelete = regexp.MustCompile(`^(?:WITH b AS MATERIALIZED \(SELECT .+ LIMIT (\d+)\) )?DELETE FROM "corpus_0"\."(\w+)"`)

func (f *fakeDSQLSchemaDB) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	f.statements = append(f.statements, query)
	m := fakeDelete.FindStringSubmatch(query)
	if m == nil {
		return driver.RowsAffected(1), nil
	}
	n := f.rows[m[2]]
	if m[1] != "" {
		batch, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, err
		}
		n = min(n, batch)
	}
	if n > f.limit {
		return nil, &persistence.TransactionSizeLimitError{Msg: "transaction row limit exceeded"}
	}
	f.rows[m[2]] -= n
	return driver.RowsAffected(n), nil
}

func (f *fakeDSQLSchemaDB) SelectContext(_ context.Context, dest any, query string, args ...any) error {
	out, ok := dest.(*[]string)
	if !ok {
		return fmt.Errorf("unexpected destination %T", dest)
	}
	switch {
	case strings.Contains(query, "information_schema.tables"):
		for table := range f.rows {
			*out = append(*out, table)
		}
		slices.Sort(*out)
	case strings.Contains(query, "PRIMARY KEY"):
		table, _ := args[1].(string)
		*out = slices.Clone(f.keys[table])
	default:
		return fmt.Errorf("unexpected query %q", query)
	}
	return nil
}

func TestClearDSQLSchema(t *testing.T) {
	db := &fakeDSQLSchemaDB{
		rows: map[string]int{
			"schema_version":        1,
			"schema_update_history": 1,
			"namespace_metadata":    1,
			"executions":            20,
			"history_node":          1500,
		},
		keys:  map[string][]string{"history_node": {"shard_id", "tree_id", "branch_id", "node_id", "txn_id"}},
		limit: 600,
	}
	require.NoError(t, clearDSQLSchema(t.Context(), db, "corpus_0"))

	require.Equal(t, map[string]int{
		"schema_version":        1,
		"schema_update_history": 1,
		"namespace_metadata":    0,
		"executions":            0,
		"history_node":          0,
	}, db.rows)
	seeds, err := dsqlSeedStatements()
	require.NoError(t, err)
	require.Equal(t, []string{
		`DELETE FROM "corpus_0"."executions"`,
		`DELETE FROM "corpus_0"."history_node"`,
		historyNodeBatch(1000),
		historyNodeBatch(500),
		historyNodeBatch(500),
		historyNodeBatch(500),
		historyNodeBatch(500),
		`DELETE FROM "corpus_0"."namespace_metadata"`,
		seeds[0],
	}, db.statements)
}

func historyNodeBatch(n int) string {
	return fmt.Sprintf(`WITH b AS MATERIALIZED (SELECT "shard_id", "tree_id", "branch_id", "node_id", "txn_id" `+
		`FROM "corpus_0"."history_node" LIMIT %d) DELETE FROM "corpus_0"."history_node" t USING b `+
		`WHERE t."shard_id" = b."shard_id" AND t."tree_id" = b."tree_id" AND t."branch_id" = b."branch_id" `+
		`AND t."node_id" = b."node_id" AND t."txn_id" = b."txn_id"`, n)
}

func TestClearDSQLSchemaNeedsPrimaryKey(t *testing.T) {
	db := &fakeDSQLSchemaDB{rows: map[string]int{"queue": 5000}, limit: 3000}
	require.ErrorContains(t, clearDSQLSchema(t.Context(), db, "corpus_0"), "no primary key")
}
