package dsql

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/urfave/cli"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	dbschemas "go.temporal.io/server/schema"
	dsqlschema "go.temporal.io/server/schema/dsql"
	"go.temporal.io/server/tools/common/schema"
)

const (
	cliFlagSearchPath = "search-path"

	// visibilitySchemaName is the embedded visibility schema, which defaults to its own PostgreSQL schema.
	visibilitySchemaName = "dsql/visibility"
)

// RunTool runs the temporal-dsql-tool command line tool
func RunTool(args []string) error {
	app := BuildCLIOptions()
	return app.Run(args)
}

func cliHandler(c *cli.Context, handler func(c *cli.Context, logger log.Logger) error, logger log.Logger) {
	quiet := c.GlobalBool(schema.CLIOptQuiet)
	err := handler(c, logger)
	if err != nil {
		if !quiet {
			logger.Error("Schema operation failed", tag.Error(err))
		}
		os.Exit(1) //nolint:revive // the exit status reports failure, as upstream's schema tools do
	}
}

func BuildCLIOptions() *cli.App {
	app := cli.NewApp()
	app.Name = "temporal-dsql-tool"
	app.Usage = "Command line tool for temporal Aurora DSQL schema operations"
	app.Version = headers.ServerVersion

	logger := log.NewCLILogger()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   schema.CLIFlagEndpoint,
			Value:  "",
			Usage:  "DSQL cluster endpoint hostname (no port). Example: <cluster>.dsql.<region>.on.aws",
			EnvVar: "SQL_HOST",
		},
		cli.IntFlag{
			Name:   schema.CLIFlagPort,
			Value:  5432,
			Usage:  "port of DSQL endpoint",
			EnvVar: "SQL_PORT",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagUser,
			Value:  "admin",
			Usage:  "DSQL database user (default: admin)",
			EnvVar: "SQL_USER",
		},
		cli.StringFlag{
			Name:   schema.CLIFlagDatabase,
			Value:  "postgres",
			Usage:  "name of the database to connect to (default: postgres)",
			EnvVar: "SQL_DATABASE",
		},
		cli.StringFlag{
			Name:  "region",
			Value: "",
			Usage: "AWS region (inferred from the endpoint when omitted)",
		},
		cli.StringFlag{
			Name:  "sslmode",
			Value: "verify-full",
			Usage: "PostgreSQL TLS mode (verify-full verifies the server certificate and hostname)",
		},
		cli.StringFlag{
			Name: cliFlagSearchPath,
			Usage: fmt.Sprintf("PostgreSQL schema for the store, created if missing; match the datastore's search_path "+
				"(default: %s for %s, otherwise the cluster's default schema)", dsqlschema.VisibilitySchema, visibilitySchemaName),
		},
		cli.BoolFlag{
			Name:  schema.CLIFlagQuiet,
			Usage: "Don't set exit status to 1 on error",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "setup-schema",
			Aliases: []string{"setup"},
			Usage:   "setup initial version of DSQL schema",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIFlagVersion,
					Usage: "initial version of the schema, cannot be used with disable-versioning",
				},
				cli.StringFlag{
					Name:  schema.CLIFlagSchemaFile,
					Usage: "path to the .sql schema file; if un-specified, will use embedded schema-name",
				},
				cli.StringFlag{
					Name: schema.CLIFlagSchemaName,
					Usage: fmt.Sprintf("name of embedded schema directory with .sql file, one of: %v",
						dbschemas.PathsByDB("dsql")),
				},
				cli.BoolFlag{
					Name:  schema.CLIFlagDisableVersioning,
					Usage: "disable setup of schema versioning tables",
				},
				cli.BoolFlag{
					Name:  schema.CLIFlagOverwrite,
					Usage: "drop all existing tables before setting up new schema",
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, setupSchema, logger)
			},
		},
		{
			Name:    "update-schema",
			Aliases: []string{"update"},
			Usage:   "update DSQL schema to a specific version",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schema.CLIFlagTargetVersion,
					Usage: "target version for the schema update, defaults to latest",
				},
				cli.StringFlag{
					Name:  schema.CLIFlagSchemaDir,
					Usage: "path to directory containing versioned schema",
				},
				cli.StringFlag{
					Name: schema.CLIFlagSchemaName,
					Usage: fmt.Sprintf("name of embedded versioned schema, one of: %v",
						dbschemas.PathsByDB("dsql")),
				},
			},
			Action: func(c *cli.Context) {
				cliHandler(c, updateSchema, logger)
			},
		},
	}

	return app
}

func setupSchema(c *cli.Context, logger log.Logger) error {
	db, err := NewDSQLSchemaDBFromCLI(c, logger)
	if err != nil {
		return err
	}
	defer db.Close()

	return schema.Setup(c, db, logger)
}

func updateSchema(c *cli.Context, logger log.Logger) error {
	db, err := NewDSQLSchemaDBFromCLI(c, logger)
	if err != nil {
		return err
	}
	defer db.Close()

	return updateSchemaDB(c, db, logger)
}

func updateSchemaDB(c *cli.Context, db schema.DB, logger log.Logger) error {
	baseline, err := baselineVersion(c)
	if err != nil {
		return err
	}
	version, err := db.ReadSchemaVersion()
	if err != nil {
		return err
	}
	if version != "0.0" && version != baseline {
		return fmt.Errorf("unsupported DSQL schema version %q; create a fresh schema at version %s", version, baseline)
	}
	return schema.Update(c, db, logger)
}

// baselineVersion returns the oldest version in the versioned schema the command
// names. Each DSQL store starts from a single baseline, so a store at any other
// version predates it.
func baselineVersion(c *cli.Context) (string, error) {
	var fsys fs.FS
	var dir string
	switch {
	case c.String(schema.CLIOptSchemaName) != "":
		fsys, dir = dbschemas.Assets(), path.Join(c.String(schema.CLIOptSchemaName), "versioned")
	case c.String(schema.CLIOptSchemaDir) != "":
		fsys, dir = os.DirFS(c.String(schema.CLIOptSchemaDir)), "."
	default:
		return "", fmt.Errorf("set --%s or --%s", schema.CLIOptSchemaName, schema.CLIOptSchemaDir)
	}
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return "", err
	}
	var baseline string
	var oldest semver.Version
	for _, entry := range entries {
		version, found := strings.CutPrefix(entry.Name(), "v")
		parsed, err := semver.ParseTolerant(version)
		if !entry.IsDir() || !found || err != nil {
			continue
		}
		if baseline == "" || parsed.LT(oldest) {
			baseline, oldest = version, parsed
		}
	}
	if baseline == "" {
		return "", errors.New("no versioned schema found")
	}
	return baseline, nil
}

// schemaSearchPath is the PostgreSQL schema the command works in, or empty for the
// cluster's default.
func schemaSearchPath(c *cli.Context) string {
	if searchPath := strings.TrimSpace(c.GlobalString(cliFlagSearchPath)); searchPath != "" {
		return searchPath
	}
	if path.Clean(c.String(schema.CLIOptSchemaName)) == visibilitySchemaName {
		return dsqlschema.VisibilitySchema
	}
	return ""
}
