package shard

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
)

type recordedShardWaits []time.Duration

func (r *recordedShardWaits) RecordShardWait(wait time.Duration) {
	*r = append(*r, wait)
}

func TestIOSemaphoreAcquireRecordsShardWaits(t *testing.T) {
	waits := &recordedShardWaits{}
	s := &ContextImpl{
		ioSemaphore:    locks.NewPrioritySemaphore(1),
		metricsHandler: metrics.NoopMetricsHandler,
		shardWaits:     waits,
	}

	require.NoError(t, s.ioSemaphoreAcquire(context.Background()))
	require.Len(t, *waits, 1)

	// The semaphore is held, so this acquire waits until its context ends, and that wait
	// is recorded too.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.Error(t, s.ioSemaphoreAcquire(ctx))
	require.Len(t, *waits, 2)
	require.GreaterOrEqual(t, (*waits)[1], 20*time.Millisecond)

	s.ioSemaphoreRelease()
}

func TestIOSemaphoreAcquireWithoutShardWaits(t *testing.T) {
	s := &ContextImpl{
		ioSemaphore:    locks.NewPrioritySemaphore(1),
		metricsHandler: metrics.NoopMetricsHandler,
	}
	require.NoError(t, s.ioSemaphoreAcquire(context.Background()))
	s.ioSemaphoreRelease()
}
