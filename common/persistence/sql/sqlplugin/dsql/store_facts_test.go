package dsql

import (
	"context"
	"fmt"
	"io"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/testing/await"
)

type recordedFacts struct {
	mu       sync.Mutex
	counts   map[persistence.StoreFact]int64
	declared bool
}

func (r *recordedFacts) DeclareStoreFacts() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.declared = true
}

func (r *recordedFacts) ReportStoreFact(fact persistence.StoreFact, count int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts == nil {
		r.counts = map[persistence.StoreFact]int64{}
	}
	r.counts[fact] += count
}

func (r *recordedFacts) snapshot() map[persistence.StoreFact]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.counts)
}

func newFactsTestDB(t *testing.T) (*db, *recordedFacts) {
	t.Helper()
	pdb := newDB(sqlplugin.DbKindMain, "", nil, nil, nil)
	facts := &recordedFacts{}
	pdb.SetStoreFacts(facts)
	return pdb, facts
}

func TestSetStoreFactsDeclaresThatDSQLReportsFacts(t *testing.T) {
	_, facts := newFactsTestDB(t)
	facts.mu.Lock()
	defer facts.mu.Unlock()
	require.True(t, facts.declared)
}

func TestMaybeConvertErrorReportsStoreFacts(t *testing.T) {
	throttled := map[persistence.StoreFact]int64{persistence.StoreFactThrottled: 1}
	limit := map[persistence.StoreFact]int64{persistence.StoreFactLimit: 1}
	for name, tc := range map[string]struct {
		err  error
		want map[persistence.StoreFact]int64
	}{
		"connect throttled":    {err: fmt.Errorf("%w: %w", errConnectThrottled, context.DeadlineExceeded), want: throttled},
		"connection rate":      {err: &pgconn.PgError{Code: "53400"}, want: throttled},
		"too many connections": {err: &pgconn.PgError{Code: "53300"}, want: throttled},
		"transaction limits":   {err: &pgconn.PgError{Code: "54000"}, want: limit},
		"transaction memory":   {err: &pgconn.PgError{Code: "53200"}, want: limit},
		"storage":              {err: &pgconn.PgError{Code: "53100"}, want: limit},
		"conflict":             {err: &pgconn.PgError{Code: "40001"}},
		"unique violation":     {err: &pgconn.PgError{Code: "23505"}},
		"connection lost":      {err: io.EOF},
	} {
		t.Run(name, func(t *testing.T) {
			pdb, facts := newFactsTestDB(t)
			_ = pdb.maybeConvertError(tc.err)
			require.Equal(t, tc.want, facts.snapshot())
		})
	}
}

func TestTxRetryBackoffReportsContentionWhenRetriesRunOut(t *testing.T) {
	pdb, facts := newFactsTestDB(t)
	conflict := &pgconn.PgError{Code: "40001"}
	maxRetries := pdb.maxRetries
	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, retry := pdb.TxRetryBackoff("UpdateWorkflowExecution", attempt, conflict)
		require.True(t, retry)
	}
	require.Empty(t, facts.snapshot())

	_, retry := pdb.TxRetryBackoff("UpdateWorkflowExecution", maxRetries+1, conflict)
	require.False(t, retry)
	require.Equal(t, map[persistence.StoreFact]int64{persistence.StoreFactContention: 1}, facts.snapshot())
}

func TestStoreFactsWithoutASink(t *testing.T) {
	var none *storeFacts
	none.report(persistence.StoreFactThrottled, 1)
	(&storeFacts{}).report(persistence.StoreFactThrottled, 1)

	pdb := newDB(sqlplugin.DbKindMain, "", nil, nil, nil)
	require.Error(t, pdb.maybeConvertError(&pgconn.PgError{Code: "53400"}))
	pdb.SetStoreFacts(nil)
	require.Error(t, pdb.maybeConvertError(&pgconn.PgError{Code: "53400"}))
}

func TestReportQueuedFactsReportsNewEmptyAcquires(t *testing.T) {
	facts := &storeFacts{}
	recorded := &recordedFacts{}
	facts.set(recorded)
	var emptyAcquires atomic.Int64
	emptyAcquires.Store(5) // waits before sampling started aren't reported

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		reportQueuedFacts(ctx, facts, time.Millisecond, 5, emptyAcquires.Load)
	}()
	emptyAcquires.Add(3)
	await.RequireTrue(t, func() bool {
		return recorded.snapshot()[persistence.StoreFactQueued] == 3
	}, 5*time.Second, time.Millisecond)

	cancel()
	<-done
	require.Equal(t, map[persistence.StoreFact]int64{persistence.StoreFactQueued: 3}, recorded.snapshot())
}
