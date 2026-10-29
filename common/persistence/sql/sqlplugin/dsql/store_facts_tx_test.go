package dsql_test

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/resolver"
)

type countedFacts map[persistence.StoreFact]int64

func (c countedFacts) ReportStoreFact(fact persistence.StoreFact, count int64) { c[fact] += count }

func TestTransactionsReportThroughTheirDatabasesSink(t *testing.T) {
	fakeDB, _ := newShardTestDB(t, 0)
	cfg := &config.SQL{
		PluginName:   dsql.PluginName,
		DatabaseName: "store_facts_test",
		Connect:      func(*config.SQL) (*sqlx.DB, error) { return fakeDB, nil },
	}
	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, cfg,
		resolver.NewNoopResolver(), log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	facts := countedFacts{}
	receiver, ok := db.(persistence.StoreFactsReceiver)
	require.True(t, ok)
	receiver.SetStoreFacts(facts)

	tx, err := db.BeginTx(context.WithoutCancel(t.Context()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	dsql.ReportStoreFact(db, persistence.StoreFactThrottled)
	dsql.ReportStoreFact(tx, persistence.StoreFactLimit)
	require.Equal(t, countedFacts{persistence.StoreFactThrottled: 1, persistence.StoreFactLimit: 1}, facts)
}
