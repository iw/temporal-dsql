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

// SetStoreFacts implements persistence.StoreFactsReceiver.
func (pdb *db) SetStoreFacts(sink persistence.StoreFacts) {
	pdb.facts.set(sink)
}

// TxRetryBackoff reports contention once retries on conflicting writes run out.
func (pdb *db) TxRetryBackoff(operation string, attempt int, err error) (time.Duration, bool) {
	delay, retry := pdb.dsqlTxRetryPolicy.TxRetryBackoff(operation, attempt, err)
	if !retry {
		pdb.facts.report(persistence.StoreFactContention, 1)
	}
	return delay, retry
}

// reportQueuedFacts reports, every interval until ctx ends, how many acquires found no
// idle connection since the last sample, starting from start. The pool collector's 15
// seconds is too coarse for the capacity controller's windows.
func reportQueuedFacts(
	ctx context.Context,
	facts *storeFacts,
	interval time.Duration,
	start int64,
	emptyAcquires func() int64,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	last := start
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := emptyAcquires()
			facts.report(persistence.StoreFactQueued, current-last)
			last = current
		}
	}
}
