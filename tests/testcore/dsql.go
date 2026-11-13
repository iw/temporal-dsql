package testcore

import (
	"fmt"
	"maps"
	"sync"
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
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

// UseDSQLPersistence reports whether the functional tests run on DSQL.
func UseDSQLPersistence() bool {
	return cliFlags.persistenceDriver == dsqlPluginName
}

// dsqlSuitePlaces holds a place for each suite-scoped cluster that may lease a DSQL test
// schema at once. It is nil when the tests don't run on DSQL.
var dsqlSuitePlaces = sync.OnceValues(func() (chan struct{}, error) {
	if !UseDSQLPersistence() {
		return nil, nil
	}
	schemas, err := persistencetests.DSQLTestSchemaCount()
	if err != nil {
		return nil, err
	}
	places, err := dsqlSuitePlaceCount(schemas, len(testClusterRouter.shared.allSlots), len(testClusterRouter.dedicated.allSlots))
	if err != nil {
		return nil, err
	}
	return make(chan struct{}, places), nil
})

// dsqlSuitePlaceCount returns how many suite-scoped clusters may hold a DSQL test schema
// at once. The shared clusters keep a schema each, and so does each dedicated slot's
// pooled cluster. Suite-scoped clusters share the rest but one, which a dedicated slot's
// fresh cluster can always lease. A suite keeps its schema while its tests wait for a
// dedicated slot, so if suites could hold every schema left, the slots' holders could
// wait for a schema that no test gives back.
func dsqlSuitePlaceCount(schemas, shared, dedicated int) (int, error) {
	places := schemas - shared - dedicated - 1
	if places < 1 {
		return 0, fmt.Errorf("DSQL_TEST_SCHEMAS lists %d schemas, but %d shared and %d dedicated test clusters need at least %d",
			schemas, shared, dedicated, shared+dedicated+2)
	}
	return places, nil
}

// dsqlSuitePlacesFor returns the places for suite-scoped clusters on DSQL, and fails the
// test if DSQL_TEST_SCHEMAS can't hold the router's clusters.
func dsqlSuitePlacesFor(t *testing.T) chan struct{} {
	places, err := dsqlSuitePlaces()
	if err != nil {
		t.Fatal(err)
	}
	return places
}

// takeDSQLSuitePlace waits for a place for a suite-scoped cluster, and returns the
// function that gives it back. Without places, it doesn't wait.
func takeDSQLSuitePlace(t *testing.T, places chan struct{}) func() {
	if places == nil {
		return func() {}
	}
	select {
	case places <- struct{}{}:
	default:
		t.Logf("Waiting for one of the %d places for suite-scoped clusters on DSQL", cap(places))
		places <- struct{}{}
	}
	var once sync.Once
	return func() { once.Do(func() { <-places }) }
}
