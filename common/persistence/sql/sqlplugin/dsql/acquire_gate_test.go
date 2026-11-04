package dsql

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/gate"
	"go.temporal.io/server/common/testing/await"
)

// The gate's queues: writes and callers without a deadline, then reads.
const (
	priorityQueue = 0
	readQueue     = 1
)

type gateResult struct {
	ticket *gate.Ticket
	err    error
}

// acquireAsync acquires in a goroutine, and sends the outcome.
func acquireAsync(ctx context.Context, g *acquireGate) <-chan gateResult {
	result := make(chan gateResult, 1)
	go func() {
		ticket, err := g.acquire(ctx)
		result <- gateResult{ticket, err}
	}()
	return result
}

func recordHolds(g *acquireGate, kind acquireKind, hold time.Duration, n int) {
	for range n {
		g.policy.RecordHold(kind, hold)
	}
}

func queuedAt(g *acquireGate, queue int) int {
	priority, reads := g.policy.Queued()
	if queue == readQueue {
		return reads
	}
	return priority
}

func heldAt(g *acquireGate) int {
	return g.policy.Held()
}

func awaitQueued(t *testing.T, g *acquireGate, queue, n int) {
	t.Helper()
	await.RequireTrue(t, func() bool { return queuedAt(g, queue) == n }, 5*time.Second, time.Millisecond)
}

func TestGateAdmitsUpToItsSizeAtOnce(t *testing.T) {
	g := newAcquireGate(2, nil)
	for range 2 {
		_, err := g.acquire(t.Context())
		require.NoError(t, err)
	}
	require.Equal(t, 2, heldAt(g))
	require.Zero(t, g.waited.Load())
}

func TestGateServesWritesBeforeReads(t *testing.T) {
	g := newAcquireGate(1, nil)
	held, err := g.acquire(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	read := acquireAsync(ctx, g)
	awaitQueued(t, g, readQueue, 1)
	write := acquireAsync(withAcquireKind(ctx, acquireWrite), g)
	awaitQueued(t, g, priorityQueue, 1)

	held.Release(true)
	first := <-write
	require.NoError(t, first.err)
	require.Equal(t, 1, queuedAt(g, readQueue))
	first.ticket.Release(true)
	second := <-read
	require.NoError(t, second.err)
	second.ticket.Release(true)
	require.Zero(t, heldAt(g))
	require.Equal(t, int64(2), g.waited.Load())
}

func TestGateQueuesCallsWithoutADeadlineWithWrites(t *testing.T) {
	g := newAcquireGate(1, nil)
	held, err := g.acquire(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	withDeadline := acquireAsync(ctx, g)
	awaitQueued(t, g, readQueue, 1)
	withoutDeadline := acquireAsync(t.Context(), g)
	awaitQueued(t, g, priorityQueue, 1)

	held.Release(true)
	first := <-withoutDeadline
	require.NoError(t, first.err)
	first.ticket.Release(true)
	second := <-withDeadline
	require.NoError(t, second.err)
	second.ticket.Release(true)
}

func TestGateRefusesAtOnceACallerItCantServeInTime(t *testing.T) {
	g := newAcquireGate(1, nil)
	// Reads have no holds yet, so they're judged by the writes': a 1 s wait for the slot,
	// and 1.5 s reserved for the read.
	recordHolds(g, acquireWrite, time.Second, 10)
	held, err := g.acquire(t.Context())
	require.NoError(t, err)
	defer held.Release(false)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = g.acquire(ctx)
	require.ErrorIs(t, err, errGateRefused)
	require.NoError(t, ctx.Err(), "the refusal should come before the deadline")
	require.Zero(t, queuedAt(g, readQueue))

	pdb, facts := newFactsTestDB(t)
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, pdb.maybeConvertError(err), &exhausted)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, exhausted.Cause)
	require.Equal(t, map[persistence.StoreFact]int64{persistence.StoreFactThrottled: 1}, facts.snapshot())
}

func TestGateRefusesAWaitingCallerWhileTimeIsLeft(t *testing.T) {
	g := newAcquireGate(1, nil)
	recordHolds(g, acquireRead, 150*time.Millisecond, 10)
	held, err := g.acquire(t.Context())
	require.NoError(t, err)
	defer held.Release(false)

	// A 150 ms wait and 225 ms reserved fit in the 600 ms left, so the caller waits, and is
	// refused when 225 ms are left.
	ctx, cancel := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer cancel()
	result := acquireAsync(ctx, g)
	awaitQueued(t, g, readQueue, 1)
	refused := <-result
	require.ErrorIs(t, refused.err, errGateRefused)
	require.NoError(t, ctx.Err(), "the refusal should come before the deadline")
	require.Zero(t, queuedAt(g, readQueue))
	require.Zero(t, g.waited.Load())
}

func TestGateCanceledWaiterLeavesTheQueue(t *testing.T) {
	g := newAcquireGate(1, nil)
	held, err := g.acquire(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	result := acquireAsync(ctx, g)
	awaitQueued(t, g, priorityQueue, 1)
	cancel()
	canceled := <-result
	require.ErrorIs(t, canceled.err, context.Canceled)
	require.NotErrorIs(t, canceled.err, errConnectThrottled)
	require.Zero(t, queuedAt(g, priorityQueue))

	held.Release(true)
	require.Zero(t, heldAt(g))
}

func TestGateTicketRecordsTheHoldsOfUsedConnections(t *testing.T) {
	g := newAcquireGate(2, nil)
	used, err := g.acquire(withAcquireKind(t.Context(), acquireWrite))
	require.NoError(t, err)
	unused, err := g.acquire(t.Context())
	require.NoError(t, err)

	used.Release(true)
	used.Release(true)
	unused.Release(false)
	require.Zero(t, heldAt(g))
	require.Equal(t, 1, g.policy.Holds(acquireWrite))
	require.Zero(t, g.policy.Holds(acquireRead))
}

func TestGateSeedsAnEmptyEstimateFromTheOtherKind(t *testing.T) {
	g := newAcquireGate(1, nil)
	recordHolds(g, acquireWrite, 40*time.Millisecond, 3)
	p90, mean := g.policy.Estimate(acquireRead)
	require.Equal(t, 40*time.Millisecond, p90)
	require.Equal(t, 40*time.Millisecond, mean)

	recordHolds(g, acquireRead, 5*time.Millisecond, 3)
	p90, mean = g.policy.Estimate(acquireRead)
	require.Equal(t, 5*time.Millisecond, p90)
	require.Equal(t, 5*time.Millisecond, mean)
}

func TestGateMetrics(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	g := newAcquireGate(1, handler)
	recordHolds(g, acquireWrite, time.Second, 10)
	held, err := g.acquire(t.Context())
	require.NoError(t, err)

	refusedCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = g.acquire(withAcquireKind(refusedCtx, acquireWrite))
	require.ErrorIs(t, err, errGateRefused)
	waiting := acquireAsync(t.Context(), g)
	awaitQueued(t, g, priorityQueue, 1)
	held.Release(true)
	admitted := <-waiting
	require.NoError(t, admitted.err)
	admitted.ticket.Release(true)

	snapshot := capture.Snapshot()
	require.Len(t, snapshot["dsql_pool_gate_refusals_total"], 1)
	require.Equal(t, map[string]string{"kind": "write"}, snapshot["dsql_pool_gate_refusals_total"][0].Tags)
	require.Len(t, snapshot["dsql_pool_gate_waits_total"], 1)
	require.Equal(t, map[string]string{"kind": "read"}, snapshot["dsql_pool_gate_waits_total"][0].Tags)
	require.Len(t, snapshot["dsql_pool_gate_wait"], 1)
}

func TestPoolConnectorRefusesThroughTheGate(t *testing.T) {
	var dials atomic.Int64
	p := &plugin{rateLimiter: poolTestLimiter(func(context.Context) error { return nil })}
	c := testPoolConfig(t, p, &config.SQL{PoolSizeHint: "ephemeral"})
	c.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("no server")
	}
	sqlDB, connector, err := openPoolDB(c, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()

	// Fill the gate, and give it holds that a caller with 100 ms left can't wait for.
	var tickets []*gate.Ticket
	defer func() {
		for _, ticket := range tickets {
			ticket.Release(false)
		}
	}()
	for range EphemeralPoolSize {
		ticket, err := connector.gate.acquire(t.Context())
		require.NoError(t, err)
		tickets = append(tickets, ticket)
	}
	recordHolds(connector.gate, acquireRead, time.Second, 10)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = sqlDB.ExecContext(ctx, "SELECT 1")
	require.ErrorIs(t, err, errGateRefused)
	require.Zero(t, dials.Load())
}

func TestPoolConnectorReleasesTheSlotWhenTheConnectFails(t *testing.T) {
	p := &plugin{rateLimiter: poolTestLimiter(func(context.Context) error { return nil })}
	c := testPoolConfig(t, p, &config.SQL{PoolSizeHint: "ephemeral"})
	cause := errors.New("no server")
	c.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) { return nil, cause }
	sqlDB, connector, err := openPoolDB(c, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()

	require.ErrorIs(t, sqlDB.PingContext(t.Context()), cause)
	require.Zero(t, heldAt(connector.gate))
	require.Zero(t, connector.gate.policy.Holds(acquireRead), "a failed connect isn't a hold")
}

func TestPoolConnectorCountsGateWaitsAsQueued(t *testing.T) {
	c := testPoolConfig(t, &plugin{}, &config.SQL{PoolSizeHint: "ephemeral"})
	sqlDB, connector, err := openPoolDB(c, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()

	connector.gate.waited.Add(3)
	require.Equal(t, acquireCounts{queued: 3}, connector.acquireCounts())
}
