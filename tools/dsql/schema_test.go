package dsql

import (
	"bytes"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
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
	for _, store := range []struct{ name, version string }{
		{"temporal", dsqlschema.Version},
		{"visibility", dsqlschema.VisibilityVersion},
	} {
		t.Run(store.name, func(t *testing.T) {
			full, err := fs.ReadFile(dbschemas.Assets(), "dsql/"+store.name+"/schema.sql")
			require.NoError(t, err)
			statements, err := persistence.LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewReader(full)})
			require.NoError(t, err)
			require.NotEmpty(t, statements)
			dirs, err := fs.ReadDir(dbschemas.Assets(), "dsql/"+store.name+"/versioned")
			require.NoError(t, err)
			require.Len(t, dirs, 1)
			require.Equal(t, "v"+store.version, dirs[0].Name())
			for _, embedded := range []bool{true, false} {
				name := "filesystem"
				setupSource := []string{"--schema-file", filepath.Join("..", "..", "schema", "dsql", store.name, "schema.sql")}
				updateSource := []string{"--schema-dir", filepath.Join("..", "..", "schema", "dsql", store.name, "versioned")}
				if embedded {
					name = "embedded"
					setupSource = []string{"--schema-name", "dsql/" + store.name}
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
							run(append([]string{"setup-schema", "--version", store.version}, setupSource...)...)
							require.Equal(t, []string{store.version}, db.updates)
						} else {
							run("setup-schema", "--version", "0.0")
							require.Empty(t, db.statements)
							require.Equal(t, "0.0", db.version)
							run(append([]string{"update-schema"}, updateSource...)...)
							require.Equal(t, []string{"0.0", store.version}, db.updates)
						}
						require.Equal(t, 1, db.versionTables)
						require.Equal(t, store.version, db.version)
						require.Equal(t, store.version, db.compatible)
						require.Equal(t, statements, db.statements)
						// Updating an initialized baseline does not execute the DDL again.
						run(append([]string{"update-schema"}, updateSource...)...)
						require.Equal(t, statements, db.statements)
					})
				}
			}
		})
	}
}

func TestSchemaUpdateRequiresBaseline(t *testing.T) {
	for _, tc := range []struct{ store, version, baseline string }{
		{"temporal", "0.5", dsqlschema.Version},
		{"visibility", dsqlschema.Version, dsqlschema.VisibilityVersion},
	} {
		t.Run(tc.store, func(t *testing.T) {
			db := &baselineDB{version: tc.version}
			app := BuildCLIOptions()
			for i := range app.Commands {
				if app.Commands[i].Name == "update-schema" {
					app.Commands[i].Action = func(c *cli.Context) error { return updateSchemaDB(c, db, log.NewNoopLogger()) }
				}
			}
			err := app.Run([]string{"temporal-dsql-tool", "update-schema", "--schema-name", "dsql/" + tc.store})
			require.ErrorContains(t, err, "create a fresh schema at version "+tc.baseline)
			require.Empty(t, db.statements)
			require.Empty(t, db.updates)
			require.Equal(t, tc.version, db.version)
		})
	}
}

func TestSchemaSearchPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"persistence", []string{"setup-schema", "--schema-name", "dsql/temporal"}, ""},
		{"visibility", []string{"setup-schema", "--schema-name", "dsql/visibility"}, dsqlschema.VisibilitySchema},
		{"configured", []string{"--search-path", "custom_visibility", "setup-schema", "--schema-name", "dsql/visibility"}, "custom_visibility"},
		{"file", []string{"setup-schema", "--schema-file", "schema.sql"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := BuildCLIOptions()
			for i := range app.Commands {
				app.Commands[i].Action = func(c *cli.Context) error {
					require.Equal(t, tc.want, schemaSearchPath(c))
					return nil
				}
			}
			require.NoError(t, app.Run(append([]string{"temporal-dsql-tool"}, tc.args...)))
		})
	}
	dsn := schemaDSN("db.example.invalid", 5432, "admin", "fake-token", "postgres", "verify-full", "temporal_visibility")
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	require.Equal(t, "temporal_visibility", cfg.RuntimeParams["search_path"])

	app := BuildCLIOptions()
	app.Commands[0].Action = func(c *cli.Context) error {
		_, err := NewDSQLSchemaDBFromCLI(c, log.NewNoopLogger())
		return err
	}
	err = app.Run([]string{"temporal-dsql-tool", "--endpoint", "db.example.invalid", "--region", "us-east-1",
		"--search-path", "Visibility; DROP", "setup-schema", "--schema-name", "dsql/visibility"})
	require.ErrorContains(t, err, "use one lowercase schema name")
}

// The DSQL visibility table keeps every column of the PostgreSQL one, in the same
// order, so a search attribute that upstream adds shows up when the fork moves to a
// new release.
func TestVisibilityColumnsMatchPostgreSQL(t *testing.T) {
	columns := func(path string) []string {
		t.Helper()
		full, err := fs.ReadFile(dbschemas.Assets(), path)
		require.NoError(t, err)
		statements, err := persistence.LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewReader(full)})
		require.NoError(t, err)
		for _, stmt := range statements {
			lines := strings.Split(stmt, "\n")
			if strings.TrimSpace(lines[0]) != "CREATE TABLE executions_visibility (" {
				continue
			}
			var names []string
			for _, line := range lines[1:] {
				fields := strings.Fields(line)
				if len(fields) < 2 || fields[0] == "PRIMARY" {
					continue
				}
				names = append(names, fields[0])
			}
			return names
		}
		require.Fail(t, "no executions_visibility table", path)
		return nil
	}
	postgres := columns("postgresql/v12/visibility/schema.sql")
	require.Len(t, postgres, 80)
	require.Equal(t, postgres, columns("dsql/visibility/schema.sql"))
}
