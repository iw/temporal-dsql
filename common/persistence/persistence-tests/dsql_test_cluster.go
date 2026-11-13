package persistencetests

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/resolver"
	dbschemas "go.temporal.io/server/schema"
	dsqlschema "go.temporal.io/server/schema/dsql"
)

// Aurora DSQL has one database per cluster and no CREATE DATABASE, and a persistence
// schema takes over a minute to set up. So each DSQL test cluster leases one of a fixed
// set of schemas, set up beforehand, and deletes the rows the last cluster left. Every
// test cluster shares one visibility schema: its rows are keyed by namespace ID, which
// is new for every cluster. docs/dsql/ci.md describes the variables.
const (
	dsqlEndpointEnv         = "DSQL_TEST_ENDPOINT"
	dsqlUserEnv             = "DSQL_TEST_USER"
	dsqlRegionEnv           = "DSQL_TEST_REGION"
	dsqlSchemasEnv          = "DSQL_TEST_SCHEMAS"
	dsqlVisibilitySchemaEnv = "DSQL_TEST_VISIBILITY_SCHEMA"

	dsqlPluginName          = dsql.PluginName
	dsqlDatabaseName        = "postgres"
	dsqlPort                = 5432
	dsqlVisibilityStoreName = "test-visibility"

	// A test cluster has about ten pools, and each opens all its connections at start.
	dsqlTestMaxConns = 5
	// Test clusters start often. Three test processes at this rate stay under DSQL's
	// 100 connections a second.
	dsqlTestConnectionRate = "30"
	// A history batch is one column value, which DSQL caps at 1 MiB.
	dsqlTransactionSizeLimit = 1 << 20

	dsqlResetTimeout  = 5 * time.Minute
	dsqlResetAttempts = 3
	// DSQL modifies at most 3,000 rows in a transaction.
	dsqlDeleteBatch = 1000
)

// dsqlSchemaName accepts one unquoted lowercase schema name, which search_path and
// the statements here resolve the same way.
var dsqlSchemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// dsqlLockDir holds the lease lock files. Tests point it elsewhere.
var dsqlLockDir = os.TempDir

var dsqlLeases = struct {
	sync.Mutex
	held map[string]bool
}{held: make(map[string]bool)}

type (
	dsqlTestCluster struct {
		endpoint       string
		schemas        []string
		main           config.SQL // search_path is added for the leased schema
		visibility     config.SQL
		faultInjection *config.FaultInjection
		logger         log.Logger

		lease *dsqlSchemaLease
	}

	// dsqlSchemaLease holds a schema for one test cluster. In the process a map holds
	// it; across the host's processes a lock file does, which the OS releases however
	// the process ends.
	dsqlSchemaLease struct {
		schema  string
		release func()
	}

	dsqlTestSchemaDB interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		SelectContext(ctx context.Context, dest any, query string, args ...any) error
	}
)

var _ PersistenceTestCluster = (*dsqlTestCluster)(nil)

// GetDSQLTestClusterOption returns test options for Aurora DSQL, read from the
// DSQL_TEST_* environment variables.
func GetDSQLTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName: dsqlPluginName,
		DBName:          dsqlDatabaseName,
		DBUsername:      os.Getenv(dsqlUserEnv),
		DBHost:          os.Getenv(dsqlEndpointEnv),
		DBPort:          dsqlPort,
		StoreType:       config.StoreTypeSQL,
	}
}

func newDSQLTestBase(options *TestBaseOptions, logger log.Logger) *TestBase {
	cluster, err := newDSQLTestCluster(options, logger)
	if err != nil {
		panic(err)
	}
	return NewTestBaseForCluster(cluster, logger)
}

// newDSQLTestCluster ignores options.DBName: DSQL serves one database, and each test
// cluster gets a schema of its own instead.
func newDSQLTestCluster(options *TestBaseOptions, logger log.Logger) (*dsqlTestCluster, error) {
	if options.DBHost == "" {
		return nil, fmt.Errorf("set %s to the endpoint of the DSQL cluster to test against", dsqlEndpointEnv)
	}
	schemas, err := parseDSQLTestSchemas(os.Getenv(dsqlSchemasEnv))
	if err != nil {
		return nil, err
	}
	visibilitySchema := cmp.Or(os.Getenv(dsqlVisibilitySchemaEnv), dsqlschema.VisibilitySchema)
	if !dsqlSchemaName.MatchString(visibilitySchema) {
		return nil, fmt.Errorf("%s: %q is not one lowercase schema name", dsqlVisibilitySchemaEnv, visibilitySchema)
	}
	if slices.Contains(schemas, visibilitySchema) {
		return nil, fmt.Errorf("%s includes the visibility schema %s, whose rows would be deleted", dsqlSchemasEnv, visibilitySchema)
	}

	attributes := map[string]string{"connection_rate_limit": dsqlTestConnectionRate}
	if region := os.Getenv(dsqlRegionEnv); region != "" {
		attributes["region"] = region
	}
	maps.Copy(attributes, options.ConnectAttributes)
	base := config.SQL{
		User:               options.DBUsername,
		PluginName:         dsqlPluginName,
		DatabaseName:       dsqlDatabaseName,
		ConnectAddr:        net.JoinHostPort(options.DBHost, strconv.Itoa(cmp.Or(options.DBPort, dsqlPort))),
		ConnectProtocol:    "tcp",
		MaxConns:           dsqlTestMaxConns,
		TaskScanPartitions: 4,
		ConnectAttributes:  attributes,
	}
	c := &dsqlTestCluster{
		endpoint:       options.DBHost,
		schemas:        schemas,
		main:           base,
		visibility:     base,
		faultInjection: options.FaultInjection,
		logger:         logger,
	}
	c.visibility.ConnectAttributes = withDSQLSearchPath(attributes, visibilitySchema)
	return c, nil
}

func parseDSQLTestSchemas(value string) ([]string, error) {
	var schemas []string
	for name := range strings.SplitSeq(value, ",") {
		name = strings.TrimSpace(name)
		if name == "" || slices.Contains(schemas, name) {
			continue
		}
		if !dsqlSchemaName.MatchString(name) {
			return nil, fmt.Errorf("%s: %q is not one lowercase schema name", dsqlSchemasEnv, name)
		}
		schemas = append(schemas, name)
	}
	if len(schemas) == 0 {
		return nil, fmt.Errorf("set %s to the comma-separated schemas test clusters may use; tests delete their rows", dsqlSchemasEnv)
	}
	return schemas, nil
}

func withDSQLSearchPath(attributes map[string]string, schema string) map[string]string {
	withPath := make(map[string]string, len(attributes)+1)
	maps.Copy(withPath, attributes)
	withPath["search_path"] = schema
	return withPath
}

// SetupTestDatabase leases a schema and deletes the rows its last cluster left. A schema
// stays leased while its test cluster runs, which for a suite can be most of an hour, so
// it waits as long as it takes.
func (c *dsqlTestCluster) SetupTestDatabase() {
	lease, err := leaseDSQLSchema(context.Background(), c.endpoint, c.schemas, c.logger)
	if err != nil {
		panic(err)
	}
	if err := resetDSQLTestSchema(c.mainConfig(lease.schema), c.logger); err != nil {
		lease.release()
		panic(fmt.Errorf("reset DSQL test schema %s: %w", lease.schema, err))
	}
	c.lease = lease
	c.logger.Info("Leased DSQL test schema", tag.NewStringTag("schema", lease.schema))
}

// Config returns the persistence config for the leased schema.
func (c *dsqlTestCluster) Config() config.Persistence {
	if c.lease == nil {
		panic("DSQL test cluster has no schema before SetupTestDatabase")
	}
	mainStore := c.mainConfig(c.lease.schema)
	visibilityStore := c.visibility
	visibilityStore.ConnectAttributes = maps.Clone(c.visibility.ConnectAttributes)
	return config.Persistence{
		DefaultStore:    "test",
		VisibilityStore: dsqlVisibilityStoreName,
		DataStores: map[string]config.DataStore{
			"test":                  {SQL: &mainStore, FaultInjection: c.faultInjection},
			dsqlVisibilityStoreName: {SQL: &visibilityStore, FaultInjection: c.faultInjection},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(dsqlTransactionSizeLimit),
	}
}

// TearDownTestDatabase returns the schema to the pool. The next lease clears it.
func (c *dsqlTestCluster) TearDownTestDatabase() {
	if c.lease == nil {
		return
	}
	c.lease.release()
	c.lease = nil
}

func (c *dsqlTestCluster) mainConfig(schema string) config.SQL {
	cfg := c.main
	cfg.ConnectAttributes = withDSQLSearchPath(c.main.ConnectAttributes, schema)
	return cfg
}

// leaseDSQLSchema returns the first free schema, waiting while every one is in use.
func leaseDSQLSchema(ctx context.Context, endpoint string, schemas []string, logger log.Logger) (*dsqlSchemaLease, error) {
	retry := time.NewTicker(time.Second)
	defer retry.Stop()
	start := time.Now()
	logged := start
	for {
		for _, schema := range schemas {
			lease, err := tryLeaseDSQLSchema(endpoint, schema)
			if err != nil || lease != nil {
				return lease, err
			}
		}
		if time.Since(logged) >= time.Minute {
			logger.Warn("Waiting for a free DSQL test schema; pooled test clusters keep theirs",
				tag.NewStringsTag("schemas", schemas), tag.NewDurationTag("waited", time.Since(start)))
			logged = time.Now()
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("every schema in %s stayed in use: %w", dsqlSchemasEnv, ctx.Err())
		case <-retry.C:
		}
	}
}

func tryLeaseDSQLSchema(endpoint, schema string) (*dsqlSchemaLease, error) {
	key := endpoint + "/" + schema
	dsqlLeases.Lock()
	defer dsqlLeases.Unlock()
	if dsqlLeases.held[key] {
		return nil, nil
	}
	sum := sha256.Sum256([]byte(key))
	path := filepath.Join(dsqlLockDir(), fmt.Sprintf("temporal-dsql-test-%s-%s.lock", hex.EncodeToString(sum[:6]), schema))
	unlock, ok, err := lockDSQLSchemaFile(path)
	if err != nil || !ok {
		return nil, err
	}
	dsqlLeases.held[key] = true
	var once sync.Once
	return &dsqlSchemaLease{schema: schema, release: func() {
		once.Do(func() {
			dsqlLeases.Lock()
			defer dsqlLeases.Unlock()
			delete(dsqlLeases.held, key)
			unlock()
		})
	}}, nil
}

// resetDSQLTestSchema checks the schema's version, then clears it. Clearing can be
// repeated, so a failed attempt, such as one that meets a transaction conflict or a
// dropped connection, starts again: a panic here would end the whole test process.
func resetDSQLTestSchema(cfg config.SQL, logger log.Logger) error {
	cfg.PoolSizeHint = "ephemeral"
	db, err := persistencesql.NewSQLAdminDB(sqlplugin.DbKindMain, &cfg, resolver.NewNoopResolver(), logger, metrics.NoopMetricsHandler)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	schema := cfg.ConnectAttributes["search_path"]
	ctx, cancel := context.WithTimeout(context.Background(), dsqlResetTimeout)
	defer cancel()
	for attempt := 1; ; attempt++ {
		err := resetDSQLTestSchemaOnce(ctx, db, schema)
		if err == nil || attempt == dsqlResetAttempts {
			return err
		}
		logger.Warn("Couldn't reset DSQL test schema, trying again", tag.NewStringTag("schema", schema), tag.Error(err))
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
}

func resetDSQLTestSchemaOnce(ctx context.Context, db sqlplugin.AdminDB, schema string) error {
	if err := db.VerifyVersion(); err != nil {
		return fmt.Errorf("%w: set the schema up with temporal-dsql-tool setup-schema", err)
	}
	conn, ok := db.(dsqlTestSchemaDB)
	if !ok {
		return fmt.Errorf("%T cannot run queries", db)
	}
	return clearDSQLSchema(ctx, conn, schema)
}

// clearDSQLSchema deletes the rows of every table in schema but its version tables, then
// inserts the rows the schema starts with.
func clearDSQLSchema(ctx context.Context, db dsqlTestSchemaDB, schema string) error {
	seeds, err := dsqlSeedStatements()
	if err != nil {
		return err
	}
	var tables []string
	if err := db.SelectContext(ctx, &tables, `SELECT table_name FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name`, schema); err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	for _, table := range tables {
		if table == "schema_version" || table == "schema_update_history" {
			continue
		}
		if err := clearDSQLTable(ctx, db, schema, table); err != nil {
			return fmt.Errorf("clear %s: %w", table, err)
		}
	}
	for _, stmt := range seeds {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("insert starting rows: %w", err)
		}
	}
	return nil
}

// clearDSQLTable deletes a table's rows, in batches by primary key when they are more
// than a transaction may modify.
func clearDSQLTable(ctx context.Context, db dsqlTestSchemaDB, schema, table string) error {
	name := quoteDSQLIdent(schema) + "." + quoteDSQLIdent(table)
	_, err := db.ExecContext(ctx, "DELETE FROM "+name)
	if !isDSQLTransactionTooLarge(err) {
		return err
	}
	var keys []string
	if err := db.SelectContext(ctx, &keys, `SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON kcu.constraint_schema = tc.constraint_schema AND kcu.constraint_name = tc.constraint_name
		WHERE tc.table_schema = $1 AND tc.table_name = $2 AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position`, schema, table); err != nil {
		return fmt.Errorf("read primary key: %w", err)
	}
	if len(keys) == 0 {
		return errors.New("too many rows to delete at once, and no primary key to delete them by")
	}
	matches := make([]string, len(keys))
	for i, key := range keys {
		keys[i] = quoteDSQLIdent(key)
		matches[i] = "t." + keys[i] + " = b." + keys[i]
	}
	// The batch is materialized so it's read once whatever the table's statistics say. As
	// the inner side of a join, a LIMIT subquery can run again for each outer row.
	columns, match := strings.Join(keys, ", "), strings.Join(matches, " AND ")
	for batch := dsqlDeleteBatch; ; {
		result, err := db.ExecContext(ctx, fmt.Sprintf("WITH b AS MATERIALIZED (SELECT %s FROM %s LIMIT %d) DELETE FROM %s t USING b WHERE %s",
			columns, name, batch, name, match))
		if isDSQLTransactionTooLarge(err) && batch > 1 {
			batch /= 2
			continue
		}
		if err != nil {
			return err
		}
		deleted, err := result.RowsAffected()
		if err != nil || deleted == 0 {
			return err
		}
	}
}

// The DSQL plugin reports SQLSTATE 54000 as a TransactionSizeLimitError.
func isDSQLTransactionTooLarge(err error) bool {
	var sizeErr *persistence.TransactionSizeLimitError
	return errors.As(err, &sizeErr)
}

func quoteDSQLIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// dsqlSeedStatements returns the INSERT statements in the persistence schema.
var dsqlSeedStatements = sync.OnceValues(func() ([]string, error) {
	f, err := dbschemas.Assets().Open("dsql/temporal/schema.sql")
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	statements, err := persistence.LoadAndSplitQueryFromReaders([]io.Reader{f})
	if err != nil {
		return nil, err
	}
	var seeds []string
	for _, stmt := range statements {
		if strings.HasPrefix(strings.ToLower(withoutSQLComments(stmt)), "insert ") {
			seeds = append(seeds, stmt)
		}
	}
	return seeds, nil
})

func withoutSQLComments(stmt string) string {
	var lines []string
	for line := range strings.Lines(stmt) {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "--") {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, " ")
}
