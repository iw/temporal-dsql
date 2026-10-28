package dsql

import (
	"fmt"
	"os"

	"github.com/urfave/cli"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	dbschemas "go.temporal.io/server/schema"
	dsqlschema "go.temporal.io/server/schema/dsql"
	"go.temporal.io/server/tools/common/schema"
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
		os.Exit(1)
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
	version, err := db.ReadSchemaVersion()
	if err != nil {
		return err
	}
	if version != "0.0" && version != dsqlschema.Version {
		return fmt.Errorf("unsupported DSQL schema version %q; create a fresh schema at version %s", version, dsqlschema.Version)
	}
	return schema.Update(c, db, logger)
}
