package dsql

import (
	"time"

	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
)

const FastRetryPluginName = "dsql-fast-retry-test"

const RangeDeleteTaskQuery = rangeDeleteTaskQry

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
