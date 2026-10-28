package sql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type retryTestDB struct {
	sqlplugin.DB
	tx        sqlplugin.Tx
	beginErr  error
	begins    int
	retryable bool
	backoff   func() (time.Duration, bool)
}

func (d *retryTestDB) BeginTx(context.Context) (sqlplugin.Tx, error) {
	d.begins++
	return d.tx, d.beginErr
}
func (d *retryTestDB) IsRetryableTxError(error) bool { return d.retryable }
func (d *retryTestDB) TxRetryBackoff(string, int, error) (time.Duration, bool) {
	if d.backoff != nil {
		return d.backoff()
	}
	return time.Millisecond, true
}

type retryTestTx struct {
	sqlplugin.Tx
	rollback func() error
}

func (tx *retryTestTx) Rollback() error { return tx.rollback() }

func TestTxExecuteTypedErrorSurvivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tx := &retryTestTx{rollback: func() error { cancel(); return nil }}
	store := NewSQLStore(&retryTestDB{tx: tx}, log.NewNoopLogger(), serialization.NewSerializer())
	want := &persistence.ShardOwnershipLostError{ShardID: 1, Msg: "stale owner"}
	err := store.txExecute(ctx, "UpdateShard", func(sqlplugin.Tx) error { return want })
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Same(t, want, err)
}

func TestTxExecuteCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	backoffStarted := make(chan struct{})
	db := &retryTestDB{
		beginErr:  errors.New("safe to retry"),
		retryable: true,
		backoff: func() (time.Duration, bool) {
			close(backoffStarted)
			return time.Hour, true
		},
	}
	store := NewSQLStore(db, log.NewNoopLogger(), serialization.NewSerializer())
	result := make(chan error, 1)
	go func() {
		result <- store.txExecute(ctx, "test operation", func(sqlplugin.Tx) error { return nil })
	}()
	select {
	case <-backoffStarted:
	case <-ctx.Done():
		t.Fatal("transaction did not reach backoff")
	}
	cancel()
	select {
	case err := <-result:
		var exhausted *serviceerror.ResourceExhausted
		require.ErrorAs(t, err, &exhausted)
		require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, exhausted.Cause)
		require.Contains(t, exhausted.Message, "test operation")
		require.Contains(t, exhausted.Message, db.beginErr.Error())
		require.Equal(t, 1, db.begins)
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not stop backoff")
	}
}

func TestTxExecuteResourceExhaustedUnchanged(t *testing.T) {
	for _, stage := range []string{"begin", "body"} {
		t.Run(stage, func(t *testing.T) {
			want := &serviceerror.ResourceExhausted{
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
				Message: "backpressure",
			}
			db := &retryTestDB{tx: &retryTestTx{rollback: func() error { return nil }}}
			if stage == "begin" {
				db.beginErr = want
			}
			store := NewSQLStore(db, log.NewNoopLogger(), serialization.NewSerializer())
			err := store.txExecute(t.Context(), "test", func(sqlplugin.Tx) error { return want })
			require.Same(t, want, err)
			require.Equal(t, 1, db.begins)
		})
	}
}
