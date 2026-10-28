package main

import (
	"os"

	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/dsql" // needed to load dsql plugin
	"go.temporal.io/server/tools/dsql"
)

func main() {
	if err := dsql.RunTool(os.Args); err != nil {
		os.Exit(1)
	}
}
