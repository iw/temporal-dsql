package testcore

import (
	"maps"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
)

const dsqlPluginName = dsql.PluginName

// dsqlDynamicConfigOverrides are the limits config/dynamicconfig/development-dsql.yaml
// sets for a DSQL deployment: DSQL holds at most 1 MiB in a column, and a history batch
// is one column value.
var dsqlDynamicConfigOverrides = map[dynamicconfig.Key]any{
	dynamicconfig.TransactionSizeLimit.Key(): 1 << 20,
	dynamicconfig.BlobSizeLimitError.Key():   1_000_000,
}

// clusterDynamicConfigOverrides returns the global overrides for a cluster on the given
// persistence: the defaults, plus DSQL's limits when the cluster runs on DSQL.
func clusterDynamicConfigOverrides(cfg config.Persistence) map[dynamicconfig.Key]any {
	if !runsOnDSQL(cfg) {
		return defaultDynamicConfigOverrides
	}
	overrides := maps.Clone(defaultDynamicConfigOverrides)
	maps.Copy(overrides, dsqlDynamicConfigOverrides)
	return overrides
}

// runsOnDSQL reports whether a cluster's persistence is DSQL.
func runsOnDSQL(cfg config.Persistence) bool {
	store, ok := cfg.DataStores[cfg.DefaultStore]
	return ok && store.SQL != nil && store.SQL.PluginName == dsqlPluginName
}
