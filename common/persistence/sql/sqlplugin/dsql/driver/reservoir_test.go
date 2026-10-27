package driver

import (
	"context"
	"database/sql/driver"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mockLeaseReleaser is a test implementation of LeaseReleaser
type mockLeaseReleaser struct {
	mu           sync.Mutex
	releaseCalls atomic.Int64
	releaseErr   error
	releasedIDs  []string
}

func (m *mockLeaseReleaser) Release(ctx context.Context, leaseID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCalls.Add(1)
	m.releasedIDs = append(m.releasedIDs, leaseID)
	return m.releaseErr
}

func (m *mockLeaseReleaser) getReleasedIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.releasedIDs)
}

// mockReservoirMetrics is a test implementation of ReservoirMetrics
type mockReservoirMetrics struct {
	mu                sync.Mutex
	sizeRecorded      atomic.Int64
	targetRecorded    atomic.Int64
	checkouts         atomic.Int64
	emptyHits         atomic.Int64
	discards          atomic.Int64
	refills           atomic.Int64
	refillFailures    atomic.Int64
	discardReasons    []string
	refillFailReasons []string
	checkoutLatencies []time.Duration
}

func (m *mockReservoirMetrics) RecordReservoirSize(size int) { m.sizeRecorded.Store(int64(size)) }
func (m *mockReservoirMetrics) RecordReservoirTarget(target int) {
	m.targetRecorded.Store(int64(target))
}
func (m *mockReservoirMetrics) IncReservoirCheckouts() { m.checkouts.Add(1) }
func (m *mockReservoirMetrics) IncReservoirEmpty()     { m.emptyHits.Add(1) }
func (m *mockReservoirMetrics) IncReservoirDiscards(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.discards.Add(1)
	m.discardReasons = append(m.discardReasons, reason)
}
func (m *mockReservoirMetrics) IncReservoirRefills() { m.refills.Add(1) }
func (m *mockReservoirMetrics) IncReservoirRefillFailures(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refillFailures.Add(1)
	m.refillFailReasons = append(m.refillFailReasons, reason)
}
func (m *mockReservoirMetrics) RecordCheckoutLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkoutLatencies = append(m.checkoutLatencies, d)
}
func (m *mockReservoirMetrics) RecordRefillerInflight(count int) {
	// No-op for tests
}

func (m *mockReservoirMetrics) getDiscardReasons() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.discardReasons)
}

func (m *mockReservoirMetrics) getRefillFailReasons() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.refillFailReasons)
}

// TestPhysicalConn_ExpiresAt tests the ExpiresAt method
func TestPhysicalConn_ExpiresAt(t *testing.T) {
	now := time.Now().UTC()
	lifetime := 10 * time.Minute

	pc := &PhysicalConn{
		CreatedAt: now,
		Lifetime:  lifetime,
	}

	expected := now.Add(lifetime)
	require.Equal(t, expected, pc.ExpiresAt())
}

// TestPhysicalConn_ExpiresAt_ZeroCreatedAt tests ExpiresAt with zero CreatedAt
func TestPhysicalConn_ExpiresAt_ZeroCreatedAt(t *testing.T) {
	pc := &PhysicalConn{
		Lifetime: 10 * time.Minute,
	}
	require.True(t, pc.ExpiresAt().IsZero())
}

// TestPhysicalConn_RemainingLifetime tests the RemainingLifetime method
func TestPhysicalConn_RemainingLifetime(t *testing.T) {
	createdAt := time.Now().UTC().Add(-5 * time.Minute)
	lifetime := 10 * time.Minute

	pc := &PhysicalConn{
		CreatedAt: createdAt,
		Lifetime:  lifetime,
	}

	now := time.Now().UTC()
	remaining := pc.RemainingLifetime(now)
	// Should be approximately 5 minutes remaining
	require.True(t, remaining > 4*time.Minute && remaining < 6*time.Minute)
}

// TestPhysicalConn_RemainingLifetime_Expired tests RemainingLifetime for expired connection
func TestPhysicalConn_RemainingLifetime_Expired(t *testing.T) {
	createdAt := time.Now().UTC().Add(-15 * time.Minute)
	lifetime := 10 * time.Minute

	pc := &PhysicalConn{
		CreatedAt: createdAt,
		Lifetime:  lifetime,
	}

	now := time.Now().UTC()
	remaining := pc.RemainingLifetime(now)
	require.Equal(t, time.Duration(0), remaining)
}

// TestPhysicalConn_Age tests the Age method
func TestPhysicalConn_Age(t *testing.T) {
	createdAt := time.Now().UTC().Add(-5 * time.Minute)

	pc := &PhysicalConn{
		CreatedAt: createdAt,
	}

	now := time.Now().UTC()
	age := pc.Age(now)
	// Should be approximately 5 minutes
	require.True(t, age > 4*time.Minute && age < 6*time.Minute)
}

// TestReservoir_TryCheckout_NonEmpty tests checkout from non-empty reservoir
func TestReservoir_TryCheckout_NonEmpty(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	// Add a connection to the reservoir
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}
	res.Return(pc, time.Now().UTC())

	// Checkout should succeed
	now := time.Now().UTC()
	checkedOut, ok := res.TryCheckout(now)
	require.True(t, ok)
	require.NotNil(t, checkedOut)
	require.Equal(t, int64(1), metrics.checkouts.Load())
}

// TestReservoir_TryCheckout_Empty tests checkout from empty reservoir
func TestReservoir_TryCheckout_Empty(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	// Checkout should fail
	now := time.Now().UTC()
	checkedOut, ok := res.TryCheckout(now)
	require.False(t, ok)
	require.Nil(t, checkedOut)
	require.Equal(t, int64(1), metrics.emptyHits.Load())
}

// TestReservoir_Return_NonFull tests return to non-full reservoir
func TestReservoir_Return_NonFull(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}

	res.Return(pc, time.Now().UTC())
	require.Equal(t, 1, res.Len())
}

// TestReservoir_Return_Full tests return to full reservoir (discard)
func TestReservoir_Return_Full(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	leaseRel := &mockLeaseReleaser{}
	res := NewReservoir(1, 30*time.Second, leaseRel, nil, metrics)

	// Fill the reservoir
	pc1 := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
		LeaseID:   "lease1",
	}
	res.Return(pc1, time.Now().UTC())
	require.Equal(t, 1, res.Len())

	// Try to return another - should be discarded
	pc2 := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
		LeaseID:   "lease2",
	}
	res.Return(pc2, time.Now().UTC())

	// Reservoir should still be at capacity
	require.Equal(t, 1, res.Len())
	require.Equal(t, int64(1), metrics.discards.Load())
	require.Contains(t, metrics.getDiscardReasons(), "reservoir_full")
	require.Equal(t, int64(1), leaseRel.releaseCalls.Load())
}

// TestReservoir_GuardWindow_Checkout tests guard window enforcement on checkout
func TestReservoir_GuardWindow_Checkout(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	guardWindow := 1 * time.Minute
	res := NewReservoir(10, guardWindow, nil, nil, metrics)

	// Add a connection that's about to expire (within guard window)
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC().Add(-9*time.Minute - 30*time.Second),
		Lifetime:  10 * time.Minute, // 30 seconds remaining, less than 1 min guard window
	}
	res.Return(pc, time.Now().UTC().Add(-9*time.Minute-30*time.Second))

	// Checkout should fail due to guard window
	now := time.Now().UTC()
	checkedOut, ok := res.TryCheckout(now)
	require.False(t, ok)
	require.Nil(t, checkedOut)
	require.Equal(t, int64(1), metrics.discards.Load())
	require.Contains(t, metrics.getDiscardReasons(), "insufficient_remaining_lifetime")
}

// TestReservoir_GuardWindow_Return tests guard window enforcement on return
func TestReservoir_GuardWindow_Return(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	guardWindow := 1 * time.Minute
	res := NewReservoir(10, guardWindow, nil, nil, metrics)

	// Try to return a connection that's about to expire
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC().Add(-9*time.Minute - 30*time.Second),
		Lifetime:  10 * time.Minute, // 30 seconds remaining
	}
	res.Return(pc, time.Now().UTC())

	// Connection should be discarded, not returned
	require.Equal(t, 0, res.Len())
	require.Equal(t, int64(1), metrics.discards.Load())
	require.Contains(t, metrics.getDiscardReasons(), "insufficient_remaining_lifetime_on_return")
}

// TestReservoir_WaitCheckout tests the WaitCheckout method
func TestReservoir_WaitCheckout(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	// Start a goroutine that adds a connection after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		pc := &PhysicalConn{
			Conn:      &mockConn{},
			CreatedAt: time.Now().UTC(),
			Lifetime:  10 * time.Minute,
		}
		res.Return(pc, time.Now().UTC())
	}()

	// WaitCheckout should succeed after the connection is added
	checkedOut, ok := res.WaitCheckout(200 * time.Millisecond)
	require.True(t, ok)
	require.NotNil(t, checkedOut)
}

// TestReservoir_WaitCheckout_Timeout tests WaitCheckout timeout
func TestReservoir_WaitCheckout_Timeout(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	// WaitCheckout should timeout
	checkedOut, ok := res.WaitCheckout(50 * time.Millisecond)
	require.False(t, ok)
	require.Nil(t, checkedOut)
}

// TestReservoir_ExpiredOnCheckout tests that expired connections are discarded on checkout
func TestReservoir_ExpiredOnCheckout(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	// Add an already expired connection
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC().Add(-15 * time.Minute),
		Lifetime:  10 * time.Minute, // Expired 5 minutes ago
	}
	// Return it with a past timestamp so it gets added
	res.ready <- pc

	// Checkout should fail and discard the expired connection
	now := time.Now().UTC()
	checkedOut, ok := res.TryCheckout(now)
	require.False(t, ok)
	require.Nil(t, checkedOut)
	require.Equal(t, int64(1), metrics.discards.Load())
	require.Contains(t, metrics.getDiscardReasons(), "expired_on_checkout")
}

// TestReservoir_NilConnection tests handling of nil connection
func TestReservoir_NilConnection(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	// Return nil should be a no-op
	res.Return(nil, time.Now().UTC())
	require.Equal(t, 0, res.Len())
}

// TestNewReservoir_ZeroCapacity tests that zero capacity defaults to 1
func TestNewReservoir_ZeroCapacity(t *testing.T) {
	res := NewReservoir(0, 30*time.Second, nil, nil, nil)
	require.NotNil(t, res)
	// Should be able to add at least one connection
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}
	res.Return(pc, time.Now().UTC())
	require.Equal(t, 1, res.Len())
}

// TestReservoir_LeaseRelease tests that lease is released when connection is discarded
func TestReservoir_LeaseRelease(t *testing.T) {
	leaseRel := &mockLeaseReleaser{}
	res := NewReservoir(10, 30*time.Second, leaseRel, nil, nil)

	// Add an expired connection with a lease
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC().Add(-15 * time.Minute),
		Lifetime:  10 * time.Minute,
		LeaseID:   "test-lease-123",
	}
	res.ready <- pc

	// Checkout should discard and release the lease
	now := time.Now().UTC()
	res.TryCheckout(now)

	require.Equal(t, int64(1), leaseRel.releaseCalls.Load())
	require.Contains(t, leaseRel.getReleasedIDs(), "test-lease-123")
}

// TestReservoir_Discard_ClosesConnection tests that discarded connections are closed
func TestReservoir_Discard_ClosesConnection(t *testing.T) {
	closeCalled := false
	mockConn := &mockConnWithClose{
		closeFunc: func() error {
			closeCalled = true
			return nil
		},
	}

	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	// Add an expired connection
	pc := &PhysicalConn{
		Conn:      mockConn,
		CreatedAt: time.Now().UTC().Add(-15 * time.Minute),
		Lifetime:  10 * time.Minute,
	}
	res.ready <- pc

	// Checkout should discard and close the connection
	now := time.Now().UTC()
	res.TryCheckout(now)

	require.True(t, closeCalled)
}

// mockConnWithClose is a mock connection that tracks Close calls
type mockConnWithClose struct {
	closeFunc func() error
}

func (m *mockConnWithClose) Prepare(query string) (driver.Stmt, error) { return nil, nil }
func (m *mockConnWithClose) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}
func (m *mockConnWithClose) Begin() (driver.Tx, error) { return nil, nil }

// =============================================================================
// Edge Case Tests
// =============================================================================

// TestReservoir_ConcurrentCheckoutReturn tests concurrent checkout and return operations
func TestReservoir_ConcurrentCheckoutReturn(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(50, 30*time.Second, nil, nil, metrics)

	// Pre-fill reservoir with connections
	for i := 0; i < 50; i++ {
		pc := &PhysicalConn{
			Conn:      &mockConn{},
			CreatedAt: time.Now().UTC(),
			Lifetime:  10 * time.Minute,
		}
		res.Return(pc, time.Now().UTC())
	}
	require.Equal(t, 50, res.Len())

	// Run concurrent checkouts and returns
	done := make(chan struct{})
	var checkoutCount, returnCount atomic.Int64

	// Start checkout goroutines
	for i := 0; i < 10; i++ {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					if pc, ok := res.TryCheckout(time.Now().UTC()); ok {
						checkoutCount.Add(1)
						// Return it back after a brief delay
						time.Sleep(time.Millisecond)
						res.Return(pc, time.Now().UTC())
						returnCount.Add(1)
					}
				}
			}
		}()
	}

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)
	close(done)

	// Verify no panics occurred and operations completed
	require.Greater(t, checkoutCount.Load(), int64(0))
	require.Greater(t, returnCount.Load(), int64(0))
}

// TestReservoir_MultipleExpiredInSequence tests handling multiple expired connections
func TestReservoir_MultipleExpiredInSequence(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	// Add multiple expired connections directly to channel
	for i := 0; i < 5; i++ {
		pc := &PhysicalConn{
			Conn:      &mockConn{},
			CreatedAt: time.Now().UTC().Add(-15 * time.Minute),
			Lifetime:  10 * time.Minute, // All expired
		}
		res.ready <- pc
	}

	// Add one valid connection
	validPC := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}
	res.ready <- validPC

	// First 5 checkouts should fail (expired), 6th should succeed
	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		pc, ok := res.TryCheckout(now)
		require.False(t, ok, "checkout %d should fail (expired)", i)
		require.Nil(t, pc)
	}

	// 6th checkout should succeed
	pc, ok := res.TryCheckout(now)
	require.True(t, ok)
	require.NotNil(t, pc)

	require.Equal(t, int64(5), metrics.discards.Load())
}

// TestReservoir_GuardWindowExactBoundary tests guard window at exact boundary
func TestReservoir_GuardWindowExactBoundary(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	guardWindow := 1 * time.Minute
	res := NewReservoir(10, guardWindow, nil, nil, metrics)

	// Connection with more than guard window remaining should be returned
	// Use a connection that has 2 minutes remaining (well above guard window)
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC().Add(-8 * time.Minute), // 2 min remaining
		Lifetime:  10 * time.Minute,
	}
	res.ready <- pc

	now := time.Now().UTC()
	checkedOut, ok := res.TryCheckout(now)

	// With 2 minutes remaining (> 1 min guard window), connection should be returned
	require.True(t, ok)
	require.NotNil(t, checkedOut)
}

// TestReservoir_GuardWindowJustBelowBoundary tests guard window just below boundary
func TestReservoir_GuardWindowJustBelowBoundary(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	guardWindow := 1 * time.Minute
	res := NewReservoir(10, guardWindow, nil, nil, metrics)

	// Connection with just under guard window remaining should be discarded
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC().Add(-9*time.Minute - 1*time.Second), // 59 seconds remaining
		Lifetime:  10 * time.Minute,
	}
	res.ready <- pc

	now := time.Now().UTC()
	checkedOut, ok := res.TryCheckout(now)

	require.False(t, ok)
	require.Nil(t, checkedOut)
	require.Equal(t, int64(1), metrics.discards.Load())
}

// TestReservoir_ZeroGuardWindow tests reservoir with zero guard window
func TestReservoir_ZeroGuardWindow(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 0, nil, nil, metrics)

	// Connection with very little remaining lifetime should still be returned
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC().Add(-9*time.Minute - 59*time.Second),
		Lifetime:  10 * time.Minute, // 1 second remaining
	}
	res.Return(pc, time.Now().UTC())

	now := time.Now().UTC()
	checkedOut, ok := res.TryCheckout(now)

	// With zero guard window, any positive remaining lifetime is acceptable
	require.True(t, ok)
	require.NotNil(t, checkedOut)
}

// TestReservoir_NegativeCapacity tests that negative capacity defaults to 1
func TestReservoir_NegativeCapacity(t *testing.T) {
	res := NewReservoir(-5, 30*time.Second, nil, nil, nil)
	require.NotNil(t, res)

	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}
	res.Return(pc, time.Now().UTC())
	require.Equal(t, 1, res.Len())
}

// TestReservoir_LargeCapacity tests reservoir with large capacity
func TestReservoir_LargeCapacity(t *testing.T) {
	res := NewReservoir(1000, 30*time.Second, nil, nil, nil)
	require.NotNil(t, res)

	// Add many connections
	for i := 0; i < 100; i++ {
		pc := &PhysicalConn{
			Conn:      &mockConn{},
			CreatedAt: time.Now().UTC(),
			Lifetime:  10 * time.Minute,
		}
		res.Return(pc, time.Now().UTC())
	}
	require.Equal(t, 100, res.Len())
}

// TestReservoir_WaitCheckout_ExpiredDuringWait tests WaitCheckout receiving expired connection
func TestReservoir_WaitCheckout_ExpiredDuringWait(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	// Start a goroutine that adds an expired connection after a short delay
	go func() {
		time.Sleep(20 * time.Millisecond)
		pc := &PhysicalConn{
			Conn:      &mockConn{},
			CreatedAt: time.Now().UTC().Add(-15 * time.Minute),
			Lifetime:  10 * time.Minute, // Expired
		}
		res.ready <- pc
	}()

	// WaitCheckout should receive the expired connection and discard it
	checkedOut, ok := res.WaitCheckout(100 * time.Millisecond)
	require.False(t, ok)
	require.Nil(t, checkedOut)
	require.Equal(t, int64(1), metrics.discards.Load())
}

// TestReservoir_WaitCheckout_NilConnection tests WaitCheckout receiving nil
func TestReservoir_WaitCheckout_NilConnection(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	// Add nil to channel
	go func() {
		time.Sleep(20 * time.Millisecond)
		res.ready <- nil
	}()

	checkedOut, ok := res.WaitCheckout(100 * time.Millisecond)
	require.False(t, ok)
	require.Nil(t, checkedOut)
}

// TestPhysicalConn_RemainingLifetime_ZeroLifetime tests RemainingLifetime with zero lifetime
func TestPhysicalConn_RemainingLifetime_ZeroLifetime(t *testing.T) {
	pc := &PhysicalConn{
		CreatedAt: time.Now().UTC(),
		Lifetime:  0,
	}

	remaining := pc.RemainingLifetime(time.Now().UTC())
	require.Equal(t, time.Duration(0), remaining)
}

// TestPhysicalConn_RemainingLifetime_NegativeLifetime tests RemainingLifetime with negative lifetime
func TestPhysicalConn_RemainingLifetime_NegativeLifetime(t *testing.T) {
	pc := &PhysicalConn{
		CreatedAt: time.Now().UTC(),
		Lifetime:  -5 * time.Minute,
	}

	remaining := pc.RemainingLifetime(time.Now().UTC())
	require.Equal(t, time.Duration(0), remaining)
}

// TestPhysicalConn_Age_ZeroCreatedAt tests Age with zero CreatedAt
func TestPhysicalConn_Age_ZeroCreatedAt(t *testing.T) {
	pc := &PhysicalConn{}

	age := pc.Age(time.Now().UTC())
	require.Equal(t, time.Duration(0), age)
}

// TestReservoir_LeaseRelease_EmptyLeaseID tests that empty lease ID doesn't call releaser
func TestReservoir_LeaseRelease_EmptyLeaseID(t *testing.T) {
	leaseRel := &mockLeaseReleaser{}
	res := NewReservoir(10, 30*time.Second, leaseRel, nil, nil)

	// Add an expired connection without a lease ID
	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC().Add(-15 * time.Minute),
		Lifetime:  10 * time.Minute,
		LeaseID:   "", // Empty lease ID
	}
	res.ready <- pc

	// Checkout should discard but not call lease releaser
	res.TryCheckout(time.Now().UTC())

	require.Equal(t, int64(0), leaseRel.releaseCalls.Load())
}

// TestReservoir_Discard_NilConn tests discard with nil underlying connection
func TestReservoir_Discard_NilConn(t *testing.T) {
	res := NewReservoir(10, 30*time.Second, nil, nil, nil)

	// Add a PhysicalConn with nil Conn
	pc := &PhysicalConn{
		Conn:      nil,
		CreatedAt: time.Now().UTC().Add(-15 * time.Minute),
		Lifetime:  10 * time.Minute,
	}
	res.ready <- pc

	// Should not panic
	_, _ = res.TryCheckout(time.Now().UTC())
}

// TestReservoir_LogFunc tests that log function is called
func TestReservoir_LogFunc(t *testing.T) {
	var logCalled atomic.Bool
	logFunc := func(msg string, keysAndValues ...interface{}) {
		logCalled.Store(true)
	}

	res := NewReservoir(10, 30*time.Second, nil, logFunc, nil)

	// Trigger empty reservoir log
	res.TryCheckout(time.Now().UTC())

	require.True(t, logCalled.Load())
}

// TestReservoir_MetricsRecordedOnReturn tests that metrics are recorded on successful return
func TestReservoir_MetricsRecordedOnReturn(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	pc := &PhysicalConn{
		Conn:      &mockConn{},
		CreatedAt: time.Now().UTC(),
		Lifetime:  10 * time.Minute,
	}
	res.Return(pc, time.Now().UTC())

	// Size should be recorded
	require.Equal(t, int64(1), metrics.sizeRecorded.Load())
}
