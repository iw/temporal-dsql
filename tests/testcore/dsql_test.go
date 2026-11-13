package testcore

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
)

func persistenceOn(plugin string) config.Persistence {
	return config.Persistence{
		DefaultStore: "test",
		DataStores:   map[string]config.DataStore{"test": {SQL: &config.SQL{PluginName: plugin}}},
	}
}

func TestClusterDynamicConfigOverrides(t *testing.T) {
	defaults := maps.Clone(defaultDynamicConfigOverrides)
	require.Equal(t, defaults, clusterDynamicConfigOverrides(persistenceOn(sqlite.PluginName)))
	require.Equal(t, defaults, clusterDynamicConfigOverrides(config.Persistence{}))

	got := clusterDynamicConfigOverrides(persistenceOn(dsqlPluginName))
	want := maps.Clone(defaults)
	want[dynamicconfig.TransactionSizeLimit.Key()] = 1 << 20
	want[dynamicconfig.BlobSizeLimitError.Key()] = 1_000_000
	require.Equal(t, want, got)
	require.Equal(t, defaults, defaultDynamicConfigOverrides)
}

func TestUseSQLVisibilityOnDSQL(t *testing.T) {
	driver := cliFlags.persistenceDriver
	t.Cleanup(func() { cliFlags.persistenceDriver = driver })
	cliFlags.persistenceDriver = dsqlPluginName
	require.True(t, UseSQLVisibility())
}
