package dsql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
	"go.temporal.io/server/common/resolver"
	dsqlschemaversion "go.temporal.io/server/schema/dsql"
)

func (pdb *db) IsDupEntryError(err error) bool {
	return pdb.dbDriver.IsDupEntryError(err)
}

func (pdb *db) IsDupDatabaseError(err error) bool {
	return pdb.dbDriver.IsDupDatabaseError(err)
}

// db represents a logical connection to dsql database
type db struct {
	*dsqlTxRetryPolicy

	dbKind   sqlplugin.DbKind
	dbName   string
	dbDriver driver.Driver

	plugin          *plugin
	cfg             *config.SQL
	resolver        resolver.ServiceResolver
	converter       DataConverter
	retryManager    *RetryManager
	idGenerator     sqlplugin.IDGenerator   // ID generator for tables without BIGSERIAL support
	poolMetrics     DSQLMetrics             // Pool metrics collector (only for main DB, not transactions)
	reservoirHandle *driver.ReservoirHandle // Reservoir handle for lifecycle management (only for main DB) - DEPRECATED, use reservoirHolder
	reservoirHolder *reservoirHandleHolder  // Holder for reservoir handle that survives reconnects
	logger          log.Logger              // Logger for assertions and error reporting

	handle *sqlplugin.DatabaseHandle
	tx     *sqlx.Tx
}

var _ sqlplugin.DB = (*db)(nil)
var _ sqlplugin.TxRetryPolicy = (*db)(nil)

// newDB returns an instance of DB, which is a logical
// connection to the underlying dsql database
func newDB(
	dbKind sqlplugin.DbKind,
	dbName string,
	dbDriver driver.Driver,
	handle *sqlplugin.DatabaseHandle,
	tx *sqlx.Tx,
) *db {
	return newDBWithDependencies(dbKind, dbName, dbDriver, handle, tx, nil, nil, DefaultRetryConfig())
}

// newDBWithDependencies returns an instance of DB with full dependency injection
func newDBWithDependencies(
	dbKind sqlplugin.DbKind,
	dbName string,
	dbDriver driver.Driver,
	handle *sqlplugin.DatabaseHandle,
	tx *sqlx.Tx,
	logger log.Logger,
	metricsHandler metrics.Handler,
	retryConfig RetryConfig,
) *db {
	mdb := &db{
		dbKind:   dbKind,
		dbName:   dbName,
		dbDriver: dbDriver,
		handle:   handle,
		tx:       tx,
		logger:   logger,
	}
	mdb.converter = &converter{}

	// Initialize Snowflake ID generator for DSQL (no BIGSERIAL support)
	// Use hostname-based node ID for distributed uniqueness
	hostname, _ := os.Hostname()
	nodeID := sqlplugin.GetNodeIDFromHostname(hostname)
	idGen, err := sqlplugin.NewSnowflakeIDGenerator(nodeID, clock.NewRealTimeSource())
	if err != nil {
		// Fallback to random ID generator if Snowflake fails
		mdb.idGenerator = sqlplugin.NewRandomIDGenerator()
	} else {
		mdb.idGenerator = idGen
	}

	// Initialize RetryManager if we have logger and metrics
	// CRITICAL FIX: Remove the tx == nil condition to enable retry for transactions
	if logger != nil && metricsHandler != nil && handle != nil {
		db, err := handle.DB()
		if err == nil && db != nil {
			mdb.retryManager = NewRetryManager(db.DB, retryConfig, logger, metricsHandler)

			// Start pool metrics collector for non-transaction DB instances
			// Only start for the main DB, not for transaction-level instances
			if tx == nil {
				dsqlMetrics := NewDSQLMetrics(metricsHandler)
				dsqlMetrics.StartPoolCollector(db.DB, 15*time.Second)
				mdb.poolMetrics = dsqlMetrics
			}
		}
	}

	rm := mdb.retryManager
	if rm == nil {
		rm = &RetryManager{config: retryConfig}
	}
	mdb.dsqlTxRetryPolicy = NewDSQLTxRetryPolicy(rm)
	return mdb
}

func (pdb *db) conn() sqlplugin.Conn {
	if pdb.tx != nil {
		return pdb.tx
	}
	return pdb.handle.Conn()
}

// maybeConvertError preserves raw pg errors for tx-boundary retry classification.
// Specifically, SQLSTATE 40001 (serialization/OCC conflict) and 0A000 (unsupported feature)
// must remain visible to the SqlStore tx retry policy.
// All other errors are converted using the Temporal sql handle.
//
// IMPORTANT: ErrReservoirEmpty is handled specially - it's a transient backpressure signal
// that should NOT trigger DatabaseHandle.ConvertError (which would recreate the entire pool).
// Instead, report overload: the operation definitely did not commit.
func (pdb *db) maybeConvertError(err error) error {
	if err == nil {
		return nil
	}

	// Handle reservoir empty as transient backpressure - do NOT pass to handle.ConvertError
	// which would trigger pool recreation and cause cascading reservoir creation.
	if errors.Is(err, driver.ErrReservoirEmpty) {
		return &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
			Message: "dsql reservoir empty: transient backpressure, retry",
		}
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.SQLState() {
		case "40001", "0A000":
			return err
		}
	}
	return pdb.handle.ConvertError(err)
}

// BeginTx starts a new transaction and returns a reference to the Tx object
func (pdb *db) BeginTx(ctx context.Context) (sqlplugin.Tx, error) {
	db, err := pdb.handle.DB()
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, pdb.maybeConvertError(err)
	}

	// Create transaction-level db with retry manager inherited from parent
	// This ensures that transaction-level operations can use retry logic
	var logger log.Logger
	var metricsHandler metrics.Handler
	retryConfig := DefaultRetryConfig()

	// If parent has retry manager, inherit its configuration
	if pdb.retryManager != nil {
		var dsqlMetrics DSQLMetrics
		logger, dsqlMetrics, retryConfig = pdb.retryManager.GetDependencies()
		metricsHandler = dsqlMetrics.GetHandler()
	}

	return newDBWithDependencies(pdb.dbKind, pdb.dbName, pdb.dbDriver, pdb.handle, tx, logger, metricsHandler, retryConfig), nil
}

// Close closes the connection to the dsql db
func (pdb *db) Close() error {
	// Stop reservoir refiller if running (prevents goroutine leak)
	// Use the holder if available (preferred), otherwise fall back to direct handle
	if pdb.reservoirHolder != nil {
		pdb.reservoirHolder.Stop()
	} else if pdb.reservoirHandle != nil {
		pdb.reservoirHandle.Stop()
	}
	// Stop pool metrics collector if running
	if pdb.poolMetrics != nil {
		pdb.poolMetrics.StopPoolCollector()
	}
	pdb.handle.Close()
	return nil
}

// PluginName returns the name of the dsql plugin
func (pdb *db) PluginName() string {
	return PluginName
}

// DbName returns the name of the database
func (pdb *db) DbName() string {
	return pdb.dbName
}

// ExpectedVersion returns expected version.
func (pdb *db) ExpectedVersion() string {
	switch pdb.dbKind {
	case sqlplugin.DbKindMain:
		return dsqlschemaversion.Version
	case sqlplugin.DbKindVisibility:
		return dsqlschemaversion.VisibilityVersion
	default:
		panic(fmt.Sprintf("unknown db kind %v", pdb.dbKind))
	}
}

// VerifyVersion verify schema version is up to date
func (pdb *db) VerifyVersion() error {
	expectedVersion := pdb.ExpectedVersion()
	return schema.VerifyCompatibleVersion(pdb, pdb.dbName, expectedVersion)
}

// Commit commits a previously started transaction
func (pdb *db) Commit() error {
	return pdb.tx.Commit()
}

// Rollback triggers rollback of a previously started transaction
func (pdb *db) Rollback() error {
	return pdb.tx.Rollback()
}

// Helper methods to hide common error handling
func (pdb *db) ExecContext(ctx context.Context, stmt string, args ...any) (sql.Result, error) {
	res, err := pdb.conn().ExecContext(ctx, stmt, args...)
	return res, pdb.maybeConvertError(err)
}

func (pdb *db) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	err := pdb.conn().GetContext(ctx, dest, query, args...)
	return pdb.maybeConvertError(err)
}

func (pdb *db) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	err := pdb.conn().SelectContext(ctx, dest, query, args...)
	return pdb.maybeConvertError(err)
}

func (pdb *db) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	res, err := pdb.conn().NamedExecContext(ctx, query, arg)
	return res, pdb.maybeConvertError(err)
}

func (pdb *db) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	stmt, err := pdb.conn().PrepareNamedContext(ctx, query)
	return stmt, pdb.maybeConvertError(err)
}

func (pdb *db) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db, err := pdb.handle.DB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, query, args...)
	return rows, pdb.maybeConvertError(err)
}

func (pdb *db) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if pdb.tx != nil {
		return pdb.tx.QueryRowContext(ctx, query, args...)
	}
	db, err := pdb.handle.DB()
	if err != nil {
		// Return a row that will produce the error when scanned
		return &sql.Row{}
	}
	return db.QueryRowContext(ctx, query, args...)
}

func (pdb *db) Rebind(query string) string {
	return pdb.conn().Rebind(query)
}

// NewExecutionStore implements sql.ExecutionStoreCreator interface.
// This allows DSQL to wrap the common execution store with DSQL-specific optimizations,
// particularly batching reads in GetWorkflowExecution into a single transaction.
func (pdb *db) NewExecutionStore(baseStore persistence.ExecutionStore, logger log.Logger) persistence.ExecutionStore {
	return NewDSQLExecutionStore(baseStore, pdb, logger)
}

// GetRetryManager returns the retry manager for DSQL operations
func (pdb *db) GetRetryManager() *RetryManager {
	return pdb.retryManager
}
