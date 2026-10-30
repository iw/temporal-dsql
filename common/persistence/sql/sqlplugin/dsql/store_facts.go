package dsql

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/persistence"
)

// storeFacts passes the plugin's facts to the sink that the persistence factory sets
// once the database exists. See docs/dsql/adaptive-store-facts.md.
type storeFacts struct {
	sink atomic.Pointer[factSink]
}

type factSink struct {
	persistence.StoreFacts
}

func (f *storeFacts) set(sink persistence.StoreFacts) {
	if sink == nil {
		f.sink.Store(nil)
		return
	}
	f.sink.Store(&factSink{sink})
}

func (f *storeFacts) report(fact persistence.StoreFact, count int64) {
	if f == nil || count <= 0 {
		return
	}
	if sink := f.sink.Load(); sink != nil {
		sink.ReportStoreFact(fact, count)
	}
}

var _ persistence.StoreFactsReceiver = (*db)(nil)

// SetStoreFacts implements persistence.StoreFactsReceiver, and declares that DSQL
// reports facts.
func (pdb *db) SetStoreFacts(sink persistence.StoreFacts) {
	pdb.facts.set(sink)
	if sink != nil {
		sink.DeclareStoreFacts()
	}
}

// TxRetryBackoff reports contention once retries on conflicting writes run out.
func (pdb *db) TxRetryBackoff(operation string, attempt int, err error) (time.Duration, bool) {
	delay, retry := pdb.dsqlTxRetryPolicy.TxRetryBackoff(operation, attempt, err)
	if !retry {
		pdb.facts.report(persistence.StoreFactContention, 1)
	}
	return delay, retry
}

// acquireCounts are the pool's cumulative acquires, and those among them that found no
// idle connection.
type acquireCounts struct {
	acquired, queued int64
}

// reportAcquireFacts reports, every interval until ctx ends, the acquires since the last
// sample as acquired facts and those that found no idle connection as queued facts,
// starting from start. Core reads queued facts as a share of acquired ones, because one
// store request can take several connections in turn. The pool collector's 15 seconds
// is too coarse for the capacity controller's windows.
func reportAcquireFacts(
	ctx context.Context,
	facts *storeFacts,
	interval time.Duration,
	start acquireCounts,
	sample func() acquireCounts,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	last := start
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := sample()
			facts.report(persistence.StoreFactQueued, current.queued-last.queued)
			facts.report(persistence.StoreFactAcquired, current.acquired-last.acquired)
			last = current
		}
	}
}
