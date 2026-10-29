package dsql

import (
	"time"

	"go.temporal.io/server/common/persistence"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
)

const FastRetryPluginName = "dsql-fast-retry-test"

const (
	RangeDeleteTaskQuery   = rangeDeleteTaskQry
	RangeDeleteTaskV2Query = rangeDeleteTaskV2Qry
)

// ReportStoreFact reports a fact as the plugin does from d, a database or a transaction.
func ReportStoreFact(d any, fact persistence.StoreFact) {
	d.(*db).facts.report(fact, 1)
}

func init() {
	cfg := DefaultRetryConfig()
	cfg.BaseDelay = time.Millisecond
	cfg.MaxDelay = time.Millisecond
	persistencesql.RegisterPlugin(FastRetryPluginName, &plugin{
		driver:         &driver.PGXDriver{},
		queryConverter: &queryConverter{},
		retryConfig:    cfg,
	})
}
