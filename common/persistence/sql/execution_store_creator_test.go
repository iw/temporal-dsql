package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

// pluginExecutionStore stands in for a plugin's execution store, which wraps the base.
type pluginExecutionStore struct {
	p.ExecutionStore
}

// storeCreatorDB is a plugin database that provides an execution store.
type storeCreatorDB struct {
	sqlplugin.DB
}

func (storeCreatorDB) NewExecutionStore(baseStore p.ExecutionStore, _ log.Logger) p.ExecutionStore {
	return &pluginExecutionStore{ExecutionStore: baseStore}
}

func TestDbConnForwardsTheExecutionStoreHook(t *testing.T) {
	conn := &DbConn{DB: storeCreatorDB{}}
	base := &sqlExecutionStore{}
	store := conn.NewExecutionStore(base, log.NewNoopLogger())
	require.Equal(t, &pluginExecutionStore{ExecutionStore: base}, store)
}

func TestDbConnKeepsTheBaseStoreForADBWithoutTheHook(t *testing.T) {
	conn := &DbConn{DB: struct{ sqlplugin.DB }{}}
	base := &sqlExecutionStore{}
	require.Same(t, base, conn.NewExecutionStore(base, log.NewNoopLogger()))
}

func TestFactoryUsesThePluginsExecutionStore(t *testing.T) {
	// A reference count above zero stands for a database the plugin has already created.
	f := &Factory{logger: log.NewNoopLogger(), mainDBConn: DbConn{DB: storeCreatorDB{}, refCnt: 1}}
	store, err := f.NewExecutionStore()
	require.NoError(t, err)
	pluginStore, ok := store.(*pluginExecutionStore)
	require.True(t, ok, "got %T", store)
	require.IsType(t, &sqlExecutionStore{}, pluginStore.ExecutionStore)
}
