package dsql_test

import (
	"database/sql/driver"
	"errors"
	"io"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
)

func TestConnectionErrorDoesNotReconnect(t *testing.T) {
	for _, cause := range []error{driver.ErrBadConn, io.EOF, io.ErrUnexpectedEOF, syscall.ECONNRESET,
		syscall.ECONNABORTED, syscall.ECONNREFUSED, &pgconn.PgError{Code: "08006"}, errors.New("IAM access denied")} {
		t.Run(cause.Error(), func(t *testing.T) {
			sqlDB, fake := newShardTestDB(t, 7)
			connects := 0
			pluginDB, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, &config.SQL{
				PluginName: dsql.PluginName,
				Connect:    func(*config.SQL) (*sqlx.DB, error) { connects++; return sqlDB, nil },
			}, nil, log.NewNoopLogger(), metrics.NoopMetricsHandler)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, pluginDB.Close()) })
			conn := pluginDB.(sqlplugin.Conn)
			admin := pluginDB.(sqlplugin.AdminDB)
			for _, operation := range []string{"query", "admin_exec", "schema_version"} {
				t.Run(operation, func(t *testing.T) {
					// database/sql retries ErrBadConn up to three times before returning it.
					fake.queryErrors = []error{cause, cause, cause}
					fake.execErrors = []error{cause, cause, cause}
					var rangeID int64
					switch operation {
					case "admin_exec":
						err = admin.Exec("UPDATE shards SET range_id = $1 WHERE shard_id = $2", 8, 1)
					case "schema_version":
						_, err = admin.ReadSchemaVersion("postgres")
					default:
						err = conn.GetContext(t.Context(), &rangeID, "SELECT range_id FROM shards WHERE shard_id = $1", 1)
					}
					var unavailable *serviceerror.Unavailable
					require.ErrorAs(t, err, &unavailable)
					fake.queryErrors = nil
					fake.execErrors = nil
					require.NoError(t, conn.GetContext(t.Context(), &rangeID, "SELECT range_id FROM shards WHERE shard_id = $1", 1))
					require.Equal(t, int64(7), rangeID)
					require.Equal(t, 1, connects)
				})
			}
		})
	}
}
