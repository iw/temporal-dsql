package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type factSink struct{}

func (factSink) DeclareStoreFacts() {}

func (factSink) ReportStoreFact(p.StoreFact, int64) {}

// factsDB is a plugin database that reports store facts.
type factsDB struct {
	sqlplugin.DB
	facts p.StoreFacts
}

func (d *factsDB) SetStoreFacts(facts p.StoreFacts) { d.facts = facts }

func TestDbConnPassesStoreFactsToAnExistingDB(t *testing.T) {
	db := &factsDB{}
	conn := &DbConn{DB: db}
	conn.SetStoreFacts(factSink{})
	require.Equal(t, factSink{}, db.facts)
}

func TestDbConnPassesStoreFactsToADBCreatedLater(t *testing.T) {
	conn := &DbConn{}
	conn.SetStoreFacts(factSink{})

	// What Get does once the plugin has created the database.
	db := &factsDB{}
	conn.DB = db
	conn.applyStoreFacts()
	require.Equal(t, factSink{}, db.facts)
}

func TestDbConnIgnoresADBWithoutFacts(t *testing.T) {
	conn := &DbConn{DB: struct{ sqlplugin.DB }{}}
	conn.SetStoreFacts(factSink{})
	require.Equal(t, factSink{}, conn.storeFacts)
}

func TestFactorySetsStoreFactsOnItsConnection(t *testing.T) {
	f := &Factory{}
	f.SetStoreFacts(factSink{})
	require.Equal(t, factSink{}, f.mainDBConn.storeFacts)
}
