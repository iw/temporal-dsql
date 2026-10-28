package dsql_test

import (
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
