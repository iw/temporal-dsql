package dsql_test

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/resolver"
)

func TestReadLockShardsForKeyShare(t *testing.T) {
	fakeDB, fake := newShardTestDB(t, 7)
	cfg := &config.SQL{
		PluginName:   dsql.PluginName,
		DatabaseName: "shard_read_lock_test",
		Connect: func(*config.SQL) (*sqlx.DB, error) {
			return fakeDB, nil
		},
	}
	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, cfg,
		resolver.NewNoopResolver(), log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	tx, err := db.BeginTx(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback()) }()
	rangeID, err := tx.ReadLockShards(t.Context(), sqlplugin.ShardsFilter{ShardID: 1})
	require.NoError(t, err)

	require.Equal(t, int64(7), rangeID)
	statements, _, _ := fake.snapshot()
	require.Equal(t, []string{shardTestReadLockQuery}, statements)
}
