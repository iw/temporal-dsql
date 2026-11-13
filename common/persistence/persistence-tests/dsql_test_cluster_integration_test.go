//go:build integration

package persistencetests

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

// TestDSQLTestClusterResetIntegration leases a schema, fills a table with more rows than
// one DSQL transaction may delete, and leases the schema again. Run it with
// DSQL_TEST_ENDPOINT set, credentials from the default AWS chain, and DSQL_TEST_SCHEMAS
// naming schemas set up with dsql/temporal:
//
// go test -tags integration,disable_grpc_modules,test_dep ./common/persistence/persistence-tests -run '^TestDSQLTestClusterResetIntegration$' -count=1 -v
func TestDSQLTestClusterResetIntegration(t *testing.T) {
	if os.Getenv(dsqlEndpointEnv) == "" {
		t.Skip("set DSQL_TEST_ENDPOINT to run against a live DSQL cluster")
	}
	logger := log.NewTestLogger()
	c, err := newDSQLTestCluster(GetDSQLTestClusterOption(), logger)
	require.NoError(t, err)
	c.SetupTestDatabase()
	t.Cleanup(c.TearDownTestDatabase)

	db := openDSQLTestSchema(t, c)
	const rows, perInsert = 3500, 500
	namespaceID := uuid.NewString()
	for start := 0; start < rows; start += perInsert {
		values := make([]string, 0, perInsert)
		for i := start; i < start+perInsert; i++ {
			values = append(values, fmt.Sprintf("('%s', 'build-%d', 'queue')", namespaceID, i))
		}
		_, err := db.ExecContext(t.Context(),
			"INSERT INTO build_id_to_task_queue (namespace_id, build_id, task_queue_name) VALUES "+strings.Join(values, ", "))
		require.NoError(t, err)
	}
	require.Equal(t, rows, countDSQLRows(t, db, "build_id_to_task_queue"))

	// Returning the schema and leasing it again clears it.
	schema := c.lease.schema
	c.TearDownTestDatabase()
	c.schemas = []string{schema}
	c.SetupTestDatabase()
	db = openDSQLTestSchema(t, c)
	require.Zero(t, countDSQLRows(t, db, "build_id_to_task_queue"))
	require.Equal(t, 1, countDSQLRows(t, db, "namespace_metadata"))
}

func openDSQLTestSchema(t *testing.T, c *dsqlTestCluster) dsqlTestSchemaDB {
	t.Helper()
	cfg := c.Config()
	store := *cfg.DataStores[cfg.DefaultStore].SQL
	store.PoolSizeHint = "ephemeral"
	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, &store, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, ok := db.(dsqlTestSchemaDB)
	require.True(t, ok)
	return conn
}

func countDSQLRows(t *testing.T, db dsqlTestSchemaDB, table string) int {
	t.Helper()
	var counts []int
	require.NoError(t, db.SelectContext(t.Context(), &counts, "SELECT count(*) FROM "+table))
	require.Len(t, counts, 1)
	return counts[0]
}
