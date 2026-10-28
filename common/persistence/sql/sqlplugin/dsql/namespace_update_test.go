package dsql_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
)

func TestUpdateNamespaceUsesGlobalVersion(t *testing.T) {
	for _, pluginName := range []string{dsql.PluginName, postgresql.PluginName} {
		t.Run(pluginName, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				version int64
				stale   bool
			}{
				{name: "older_namespace_current_global_version", version: 3},
				{name: "stale_global_version", version: 2, stale: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					factory, fake := newNamespaceTestFactory(t, pluginName)
					store, err := factory.NewMetadataStore()
					require.NoError(t, err)
					t.Cleanup(store.Close)
					require.Equal(t, pluginName, store.GetName())

					const namespaceID = "b99f9e68-5709-4f6f-99f4-d20e613b508b"
					err = store.UpdateNamespace(t.Context(), &persistence.InternalUpdateNamespaceRequest{
						Id:                  namespaceID,
						Name:                "older-namespace",
						Namespace:           persistence.NewDataBlob([]byte("namespace"), "Proto3"),
						IsGlobal:            true,
						NotificationVersion: tc.version,
					})

					statements, commits, rollbacks := fake.snapshot()
					t.Logf("statements=%q commits=%d rollbacks=%d", statements, commits, rollbacks)
					require.NotEmpty(t, statements)
					require.Equal(t, "SELECT notification_version FROM namespace_metadata WHERE partition_id=$1 FOR UPDATE", statements[0])
					require.Equal(t, []driver.Value{int64(54321)}, fake.statementArgs(0))
					require.Equal(t, 1, fake.beginCount())
					if tc.stale {
						require.ErrorContains(t, err, "conditional update error: expect: 2, actual: 3")
						require.Len(t, statements, 1)
						require.Zero(t, commits)
						require.Equal(t, 1, rollbacks)
						return
					}

					require.NoError(t, err)
					require.Equal(t, 1, commits)
					require.Zero(t, rollbacks)
					require.Len(t, statements, 3)
					require.Equal(t, "UPDATE namespaces SET name = $1, data = $2, data_encoding = $3, is_global = $4, notification_version = $5 WHERE partition_id=54321 AND id = $6", statements[1])
					_, where, found := strings.Cut(statements[1], " WHERE ")
					require.True(t, found)
					require.NotContains(t, where, "notification_version")
					var id driver.Value = namespaceID
					if pluginName == postgresql.PluginName {
						id = []byte(primitives.MustParseUUID(namespaceID))
					}
					require.Equal(t, []driver.Value{"older-namespace", []byte("namespace"), "Proto3", true, int64(3), id}, fake.statementArgs(1))
					require.Equal(t, "UPDATE namespace_metadata SET notification_version = $1 WHERE notification_version = $2 AND partition_id=$3", statements[2])
					require.Equal(t, []driver.Value{int64(4), int64(3), int64(54321)}, fake.statementArgs(2))
				})
			}
		})
	}
}

func TestUpdateNamespaceMetadataRowsAffected(t *testing.T) {
	for _, pluginName := range []string{dsql.PluginName, postgresql.PluginName} {
		t.Run(pluginName, func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				version      int64
				rowsAffected int64
			}{
				{name: "current_version", version: 3, rowsAffected: 1},
				{name: "stale_version", version: 2, rowsAffected: 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					factory, fake := newNamespaceTestFactory(t, pluginName)
					db, err := factory.GetDB()
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, db.Close()) })
					tx, err := db.BeginTx(t.Context())
					require.NoError(t, err)
					defer func() { require.NoError(t, tx.Rollback()) }()

					result, err := tx.UpdateNamespaceMetadata(t.Context(), &sqlplugin.NamespaceMetadataRow{NotificationVersion: tc.version})
					require.NoError(t, err)
					rowsAffected, err := result.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, tc.rowsAffected, rowsAffected)
					statements, _, _ := fake.snapshot()
					require.Equal(t, []string{"UPDATE namespace_metadata SET notification_version = $1 WHERE notification_version = $2 AND partition_id=$3"}, statements)
					require.Equal(t, []driver.Value{tc.version + 1, tc.version, int64(54321)}, fake.statementArgs(0))
				})
			}
		})
	}
}

func newNamespaceTestFactory(t *testing.T, pluginName string) (*persistencesql.Factory, *shardTestDriver) {
	t.Helper()
	db, fake := newShardTestDB(t, 0)
	// Other namespaces have advanced the global version since this namespace's last write.
	fake.versions = map[string]int64{"namespaces": 1, "namespace_metadata": 3}
	factory := persistencesql.NewFactory(config.SQL{
		PluginName:   pluginName,
		DatabaseName: "namespace_update_test",
		Connect:      func(*config.SQL) (*sqlx.DB, error) { return db, nil },
	}, resolver.NewNoopResolver(), "test", log.NewNoopLogger(), metrics.NoopMetricsHandler, serialization.NewSerializer())
	t.Cleanup(factory.Close)
	return factory, fake
}
