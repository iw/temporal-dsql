package dsql

import (
	"bytes"
	"io"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	dbschemas "go.temporal.io/server/schema"
	dsqlschema "go.temporal.io/server/schema/dsql"
	"go.temporal.io/server/tools/common/schema"
)

// Capture the statements and version writes from the real schema tasks without a cluster.
type baselineDB struct {
	statements          []string
	version, compatible string
	versionTables       int
	updates             []string
}

func (*baselineDB) Type() string { return "dsql" }
func (*baselineDB) Close()       {}
func (d *baselineDB) Exec(stmt string, _ ...interface{}) error {
	d.statements = append(d.statements, stmt)
	return nil
}
func (*baselineDB) DropAllTables() error                 { return nil }
func (d *baselineDB) CreateSchemaVersionTables() error   { d.versionTables++; return nil }
func (d *baselineDB) ReadSchemaVersion() (string, error) { return d.version, nil }
func (d *baselineDB) UpdateSchemaVersion(version, compatible string) error {
	d.version, d.compatible = version, compatible
	return nil
}
func (d *baselineDB) WriteSchemaUpdateLog(_, version, _, _ string) error {
	d.updates = append(d.updates, version)
	return nil
}

func TestSchemaSetupPaths(t *testing.T) {
	full, err := fs.ReadFile(dbschemas.Assets(), "dsql/temporal/schema.sql")
	require.NoError(t, err)
	statements, err := persistence.LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewReader(full)})
	require.NoError(t, err)
	require.NotEmpty(t, statements)
	dirs, err := fs.ReadDir(dbschemas.Assets(), "dsql/temporal/versioned")
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	require.Equal(t, "v"+dsqlschema.Version, dirs[0].Name())
	for _, embedded := range []bool{true, false} {
		name := "filesystem"
		setupSource := []string{"--schema-file", filepath.Join("..", "..", "schema", "dsql", "temporal", "schema.sql")}
		updateSource := []string{"--schema-dir", filepath.Join("..", "..", "schema", "dsql", "temporal", "versioned")}
		if embedded {
			name = "embedded"
			setupSource = []string{"--schema-name", "dsql/temporal"}
			updateSource = setupSource
		}
		for _, direct := range []bool{true, false} {
			path := "setup-then-update"
			if direct {
				path = "setup-at-baseline"
			}
			t.Run(name+"/"+path, func(t *testing.T) {
				db := &baselineDB{version: "0.0"}
				run := func(args ...string) {
					t.Helper()
					app := BuildCLIOptions()
					for i := range app.Commands {
						if app.Commands[i].Name == "setup-schema" {
							app.Commands[i].Action = func(c *cli.Context) error { return schema.Setup(c, db, log.NewNoopLogger()) }
						} else {
							app.Commands[i].Action = func(c *cli.Context) error { return updateSchemaDB(c, db, log.NewNoopLogger()) }
						}
					}
					require.NoError(t, app.Run(append([]string{"temporal-dsql-tool"}, args...)))
				}
				if direct {
					run(append([]string{"setup-schema", "--version", dsqlschema.Version}, setupSource...)...)
					require.Equal(t, []string{dsqlschema.Version}, db.updates)
				} else {
					run("setup-schema", "--version", "0.0")
					require.Empty(t, db.statements)
					require.Equal(t, "0.0", db.version)
					run(append([]string{"update-schema"}, updateSource...)...)
					require.Equal(t, []string{"0.0", dsqlschema.Version}, db.updates)
				}
				require.Equal(t, 1, db.versionTables)
				require.Equal(t, dsqlschema.Version, db.version)
				require.Equal(t, dsqlschema.Version, db.compatible)
				require.Equal(t, statements, db.statements)
				// Updating an initialized baseline does not execute the DDL again.
				run(append([]string{"update-schema"}, updateSource...)...)
				require.Equal(t, statements, db.statements)
			})
		}
	}
}

func TestSchemaUpdateRequiresBaseline(t *testing.T) {
	db := &baselineDB{version: "0.5"}
	app := BuildCLIOptions()
	for i := range app.Commands {
		if app.Commands[i].Name == "update-schema" {
			app.Commands[i].Action = func(c *cli.Context) error { return updateSchemaDB(c, db, log.NewNoopLogger()) }
		}
	}
	err := app.Run([]string{"temporal-dsql-tool", "update-schema", "--schema-name", "dsql/temporal"})
	require.ErrorContains(t, err, "create a fresh schema at version 1.2")
	require.Empty(t, db.statements)
	require.Empty(t, db.updates)
	require.Equal(t, "0.5", db.version)
}
