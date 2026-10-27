package dsql_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	dsqldriver "go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
)

func TestUpdateShardTxRetry(t *testing.T) {
	conflict := &pgconn.PgError{Code: "40001", Message: "serialization failure"}
	for _, pluginName := range []string{dsql.FastRetryPluginName, postgresql.PluginName} {
		t.Run(pluginName, func(t *testing.T) {
			isDSQL := pluginName == dsql.FastRetryPluginName
			for _, tc := range []struct {
				name         string
				beginErrors  []error
				execErrors   []error
				commitErrors []error
				commitError  error
				staleOwner   bool
				cancelCommit bool
				exhausted    bool
				attempts     int
				commits      int
				rollbacks    int
			}{
				{name: "commit_conflicts_then_success", commitErrors: []error{conflict, conflict}, attempts: 3, commits: 3},
				{name: "commit_conflicts_exhausted", commitError: conflict, exhausted: true, attempts: dsql.DefaultRetryConfig().MaxRetries + 1, commits: dsql.DefaultRetryConfig().MaxRetries + 1},
				{name: "empty_reservoir", beginErrors: []error{dsqldriver.ErrReservoirEmpty}, exhausted: true, attempts: 1},
				{name: "typed_error_survives_cancellation", staleOwner: true, attempts: 1, rollbacks: 1},
				{name: "begin_conflicts_then_success", beginErrors: []error{conflict, conflict}, attempts: 3, commits: 1},
				{name: "body_conflicts_then_success", execErrors: []error{conflict, conflict}, attempts: 3, commits: 1, rollbacks: 2},
				{name: "body_empty_reservoir", execErrors: []error{dsqldriver.ErrReservoirEmpty}, exhausted: true, attempts: 1, rollbacks: 1},
				{name: "commit_conflict_with_cancellation", commitError: conflict, cancelCommit: true, exhausted: true, attempts: 1, commits: 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					db, fake := newShardTestDB(t, 7)
					fake.beginErrors = tc.beginErrors
					fake.execErrors = tc.execErrors
					fake.commitErrors = tc.commitErrors
					fake.commitError = tc.commitError
					previousRangeID := int64(7)
					if tc.staleOwner {
						previousRangeID = 5
						fake.onRollback = cancel
					}
					if tc.cancelCommit {
						fake.onCommit = cancel
					}
					factory := persistencesql.NewFactory(config.SQL{
						PluginName:   pluginName,
						DatabaseName: "shard_retry_test",
						Connect:      func(*config.SQL) (*sqlx.DB, error) { return db, nil },
					}, nil, "test", log.NewNoopLogger(), metrics.NoopMetricsHandler, serialization.NewSerializer())
					t.Cleanup(factory.Close)
					store, err := factory.NewShardStore()
					require.NoError(t, err)
					t.Cleanup(store.Close)
					err = store.UpdateShard(ctx, &persistence.InternalUpdateShardRequest{
						ShardID:         1,
						PreviousRangeID: previousRangeID,
						RangeID:         previousRangeID + 1,
						ShardInfo:       persistence.NewDataBlob([]byte("shard"), "Proto3"),
					})

					statements, commits, rollbacks := fake.snapshot()
					t.Logf("attempts=%d commits=%d rollbacks=%d error=%T: %v", fake.beginCount(), commits, rollbacks, err, err)
					switch {
					case tc.staleOwner:
						var ownershipLost *persistence.ShardOwnershipLostError
						require.ErrorAs(t, err, &ownershipLost)
						require.Equal(t, int32(1), ownershipLost.ShardID)
						require.ErrorIs(t, ctx.Err(), context.Canceled)
						require.False(t, persistence.OperationPossiblySucceeded(err))
					case !isDSQL:
						var unavailable *serviceerror.Unavailable
						require.ErrorAs(t, err, &unavailable)
						require.True(t, persistence.OperationPossiblySucceeded(err))
					case tc.exhausted:
						var exhausted *serviceerror.ResourceExhausted
						require.ErrorAs(t, err, &exhausted)
						require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, exhausted.Cause)
						require.False(t, persistence.OperationPossiblySucceeded(err))
						if tc.commitError != nil {
							require.Contains(t, exhausted.Message, "UpdateShard")
							require.Contains(t, exhausted.Message, conflict.Error())
						}
					default:
						require.NoError(t, err)
					}
					if isDSQL {
						require.Equal(t, tc.attempts, fake.beginCount())
						require.Equal(t, tc.commits, commits)
						require.Equal(t, tc.rollbacks, rollbacks)
						if !tc.staleOwner {
							// Every successful Begin reruns both the fence and update.
							require.Len(t, statements, 2*(tc.commits+tc.rollbacks))
						}
					} else {
						require.Equal(t, 1, fake.beginCount())
						require.LessOrEqual(t, commits, 1)
					}
				})
			}
		})
	}
}
