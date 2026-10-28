package dsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/urfave/cli"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tools/common/schema"
)

const (
	dsqlDBType = "dsql"

	defaultTokenTTL = 15 * time.Minute

	// indexBuildTimeout bounds the wait for each asynchronous index build.
	indexBuildTimeout = 30 * time.Minute

	// Versioning tables - must match the structure expected by Temporal server
	// (see common/persistence/sql/sqlplugin/dsql/admin.go).
	// These are DSQL-safe (no SERIAL/BIGSERIAL).
	schemaVersionTableDDL = `
CREATE TABLE IF NOT EXISTS schema_version (
  version_partition       INT NOT NULL,
  db_name                 VARCHAR(255) NOT NULL,
  creation_time           TIMESTAMP,
  curr_version            VARCHAR(64),
  min_compatible_version  VARCHAR(64),
  PRIMARY KEY (version_partition, db_name)
)`

	schemaUpdateHistoryDDL = `
CREATE TABLE IF NOT EXISTS schema_update_history (
  version_partition  INT NOT NULL,
  year               INT NOT NULL,
  month              INT NOT NULL,
  update_time        TIMESTAMP NOT NULL,
  description        VARCHAR(255),
  manifest_md5       VARCHAR(64),
  new_version        VARCHAR(64),
  old_version        VARCHAR(64),
  PRIMARY KEY (version_partition, year, month, update_time)
)`
)

// asyncIndexStatement matches CREATE [UNIQUE] INDEX ASYNC, which returns the job_id of the index build.
var asyncIndexStatement = regexp.MustCompile(`(?i)\bCREATE\s+(UNIQUE\s+)?INDEX\s+ASYNC\b`)

type DSQLSchemaDB struct {
	db     *sql.DB
	logger log.Logger
}

func NewDSQLSchemaDBFromCLI(c *cli.Context, logger log.Logger) (*DSQLSchemaDB, error) {
	ctx := context.Background()

	endpoint := strings.TrimSpace(c.GlobalString(schema.CLIOptEndpoint))
	if endpoint == "" {
		// handler.go uses CLIOptEndpoint; CLIFlagEndpoint maps to "endpoint, ep"
		// In case urfave/cli didn't populate via alias, try the flag name form:
		endpoint = strings.TrimSpace(c.GlobalString("endpoint"))
	}
	if endpoint == "" {
		return nil, fmt.Errorf("missing endpoint; set --%s", schema.CLIOptEndpoint)
	}

	port := c.GlobalInt(schema.CLIOptPort)
	if port == 0 {
		port = 5432
	}

	user := strings.TrimSpace(c.GlobalString(schema.CLIOptUser))
	if user == "" {
		user = "admin"
	}

	dbName := strings.TrimSpace(c.GlobalString(schema.CLIOptDatabase))
	if dbName == "" {
		dbName = "postgres"
	}

	region, err := schemaRegion(endpoint, c.GlobalString("region"))
	if err != nil {
		return nil, err
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	// Token generation: admin vs custom user
	var token string
	if user == "admin" {
		token, err = auth.GenerateDBConnectAdminAuthToken(
			ctx,
			endpoint,
			region,
			awsCfg.Credentials,
			func(o *auth.TokenOptions) { o.ExpiresIn = defaultTokenTTL },
		)
	} else {
		token, err = auth.GenerateDbConnectAuthToken(
			ctx,
			endpoint,
			region,
			awsCfg.Credentials,
			func(o *auth.TokenOptions) { o.ExpiresIn = defaultTokenTTL },
		)
	}
	if err != nil {
		return nil, fmt.Errorf("generate dsql auth token: %w", err)
	}

	sslMode := c.GlobalString("sslmode")
	if sslMode != "verify-full" {
		logger.Warn("DSQL server certificate and hostname are not fully verified; use sslmode=verify-full", tag.NewStringTag("sslmode", sslMode))
	}
	dsn := schemaDSN(endpoint, port, user, token, dbName, sslMode)

	logger.Info("Connecting to DSQL for schema operations",
		tag.NewStringTag("endpoint", endpoint),
		tag.NewStringTag("region", region),
		tag.NewStringTag("database", dbName),
		tag.NewStringTag("user", user),
	)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &DSQLSchemaDB{db: db, logger: logger}, nil
}

func schemaRegion(endpoint, region string) (string, error) {
	if region = strings.TrimSpace(region); region != "" {
		return region, nil
	}
	parts := strings.Split(strings.TrimSuffix(strings.ToLower(endpoint), "."), ".")
	if len(parts) == 5 && parts[0] != "" && parts[1] == "dsql" && parts[2] != "" && parts[3] == "on" && parts[4] == "aws" {
		return parts[2], nil
	}
	return "", errors.New("cannot infer region from endpoint; set --region")
}

func schemaDSN(endpoint string, port int, user, token, dbName, sslMode string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		url.PathEscape(user), url.PathEscape(token), endpoint, port,
		url.PathEscape(dbName), url.QueryEscape(sslMode))
}

func (d *DSQLSchemaDB) Type() string { return dsqlDBType }

func (d *DSQLSchemaDB) Close() { _ = d.db.Close() }

// Exec executes a SQL statement. For DSQL safety, we execute each statement in its own tx.
func (d *DSQLSchemaDB) Exec(stmt string, args ...interface{}) error {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return nil
	}

	ctx := context.Background()

	// one statement per transaction
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var jobID string
	if asyncIndexStatement.MatchString(stmt) {
		if err := tx.QueryRowContext(ctx, stmt, args...).Scan(&jobID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			_ = tx.Rollback()
			return err
		}
	} else if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if jobID == "" {
		return nil
	}
	// When a build finishes, DSQL marks the index active in its catalog, and
	// transactions using the schema at that moment fail with OC001. Waiting for
	// each build keeps the later statements, and the server, clear of that.
	d.logger.Info("Waiting for DSQL index build", tag.NewStringTag("job_id", jobID))
	ctx, cancel := context.WithTimeout(ctx, indexBuildTimeout)
	defer cancel()
	return waitForIndexBuild(ctx, d, jobID)
}

// jobWaiter is the part of DSQL's job API the tool uses.
type jobWaiter interface {
	waitForJob(ctx context.Context, jobID string) (bool, error)
	jobStatus(ctx context.Context, jobID string) (status, details string, err error)
}

// waitForIndexBuild blocks until the build completes. sys.wait_for_job returns false
// when the job hasn't completed, so the job's status decides whether to wait again.
func waitForIndexBuild(ctx context.Context, w jobWaiter, jobID string) error {
	for {
		ok, err := w.waitForJob(ctx, jobID)
		if err != nil {
			return fmt.Errorf("wait for DSQL index build %s: %w", jobID, err)
		}
		if ok {
			return nil
		}
		status, details, err := w.jobStatus(ctx, jobID)
		if err != nil {
			return fmt.Errorf("read status of DSQL index build %s: %w", jobID, err)
		}
		if status != "submitted" && status != "processing" {
			return fmt.Errorf("DSQL index build %s %s: %s", jobID, status, details)
		}
	}
}

func (d *DSQLSchemaDB) waitForJob(ctx context.Context, jobID string) (bool, error) {
	var succeeded bool
	err := d.db.QueryRowContext(ctx, `CALL sys.wait_for_job($1)`, jobID).Scan(&succeeded)
	return succeeded, err
}

func (d *DSQLSchemaDB) jobStatus(ctx context.Context, jobID string) (status, details string, err error) {
	var nullStatus, nullDetails sql.NullString
	err = d.db.QueryRowContext(ctx, `SELECT status, details FROM sys.jobs WHERE job_id = $1`, jobID).Scan(&nullStatus, &nullDetails)
	return nullStatus.String, nullDetails.String, err
}

// DropAllTables drops all tables in public schema (DSQL-compatible, no DO blocks).
func (d *DSQLSchemaDB) DropAllTables() error {
	ctx := context.Background()

	// List tables in public schema
	rows, err := d.db.QueryContext(ctx, `
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
ORDER BY table_name`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return err
		}
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, t := range tables {
		// Drop each table in its own tx
		stmt := fmt.Sprintf(`DROP TABLE IF EXISTS public.%s`, quoteIdent(t))
		if err := d.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func quoteIdent(s string) string {
	// minimal identifier quoting
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// CreateSchemaVersionTables sets up the schema version tables
func (d *DSQLSchemaDB) CreateSchemaVersionTables() error {
	if err := d.Exec(schemaVersionTableDDL); err != nil {
		return err
	}
	return d.Exec(schemaUpdateHistoryDDL)
}

// ReadSchemaVersion returns the current schema version for the database
func (d *DSQLSchemaDB) ReadSchemaVersion() (string, error) {
	ctx := context.Background()

	var ver string
	err := d.db.QueryRowContext(ctx, `
SELECT curr_version
FROM schema_version
WHERE version_partition = 0 AND db_name = 'postgres'`).Scan(&ver)
	if err == sql.ErrNoRows {
		return "0.0", nil
	}
	if ver == "" {
		return "0.0", nil
	}
	return ver, err
}

// UpdateSchemaVersion updates the schema version
func (d *DSQLSchemaDB) UpdateSchemaVersion(newVersion string, minCompatibleVersion string) error {
	ctx := context.Background()

	// Use UPSERT to handle both insert and update cases
	stmt := `
INSERT INTO schema_version (version_partition, db_name, creation_time, curr_version, min_compatible_version)
VALUES (0, 'postgres', NOW(), $1, $2)
ON CONFLICT (version_partition, db_name) DO UPDATE
SET creation_time = NOW(),
    curr_version = EXCLUDED.curr_version,
    min_compatible_version = EXCLUDED.min_compatible_version`

	// one statement per tx
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stmt, newVersion, minCompatibleVersion); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// WriteSchemaUpdateLog adds an entry to schema update history table
func (d *DSQLSchemaDB) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	ctx := context.Background()

	now := time.Now().UTC()

	stmt := `
INSERT INTO schema_update_history
(version_partition, year, month, update_time, old_version, new_version, manifest_md5, description)
VALUES (0, $1, $2, $3, $4, $5, $6, $7)`

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stmt, now.Year(), int(now.Month()), now, oldVersion, newVersion, manifestMD5, desc); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
