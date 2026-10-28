package dsql

import (
	"sync"
	"testing"

	"go.temporal.io/server/common/log"
)

func TestRetryBackoffConcurrent(t *testing.T) {
	rm := NewRetryManager(DefaultRetryConfig(), log.NewNoopLogger(), nil)
	policy := NewDSQLTxRetryPolicy(rm)
	for _, tc := range []struct {
		name    string
		backoff func()
	}{
		{"transaction_policy", func() { policy.TxRetryBackoff("test", 1, createPgError("40001", "conflict")) }},
		{"retry_manager", func() { rm.calculateBackoff(1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wg sync.WaitGroup
			start := make(chan struct{})
			for range 32 {
				wg.Go(func() {
					<-start
					for range 100 {
						tc.backoff()
					}
				})
			}
			close(start)
			wg.Wait()
		})
	}
}
