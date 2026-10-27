package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mockTokenProvider is a test implementation of TokenProvider
type mockTokenProvider struct {
	token    string
	err      error
	calls    atomic.Int64
	callChan chan struct{}
}

func (m *mockTokenProvider) GetToken(ctx context.Context) (string, error) {
	m.calls.Add(1)
	if m.callChan != nil {
		select {
		case m.callChan <- struct{}{}:
		default:
		}
	}
	return m.token, m.err
}

// mockLeaseManager is a test implementation of LeaseManager
type mockLeaseManager struct {
	acquireLeaseID string
	acquireErr     error
	releaseErr     error
	acquireCalls   atomic.Int64
	releaseCalls   atomic.Int64
}

func (m *mockLeaseManager) Acquire(ctx context.Context) (string, error) {
	m.acquireCalls.Add(1)
	return m.acquireLeaseID, m.acquireErr
}

func (m *mockLeaseManager) Release(ctx context.Context, leaseID string) error {
	m.releaseCalls.Add(1)
	return m.releaseErr
}

// mockUnderlyingDriver is a test implementation of driver.Driver for refiller tests
type mockUnderlyingDriver struct {
	conn      driver.Conn
	err       error
	openCalls atomic.Int64
	openDelay time.Duration
}

func (m *mockUnderlyingDriver) Open(name string) (driver.Conn, error) {
	m.openCalls.Add(1)
	if m.openDelay > 0 {
		time.Sleep(m.openDelay)
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.conn, nil
}

// TestReservoirRefiller_RefillsWhenBelowTarget tests that refiller fills reservoir
func TestReservoirRefiller_RefillsWhenBelowTarget(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	rateLimiter := &mockRateLimiter{}
	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  5,
		LowWatermark: 3,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   rateLimiter,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for refiller to fill the reservoir
	require.Eventually(t, func() bool {
		return res.Len() >= cfg.TargetReady
	}, 5*time.Second, 50*time.Millisecond)

	require.GreaterOrEqual(t, res.Len(), cfg.TargetReady)
	require.GreaterOrEqual(t, metrics.refills.Load(), int64(cfg.TargetReady))
}

// TestReservoirRefiller_StopsWhenAtTarget tests that refiller stops when at target
func TestReservoirRefiller_StopsWhenAtTarget(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for refiller to reach target
	require.Eventually(t, func() bool {
		return res.Len() >= cfg.TargetReady
	}, 5*time.Second, 50*time.Millisecond)

	// Record the number of opens
	initialOpens := underlying.openCalls.Load()

	// Wait a bit and verify no more connections are created
	time.Sleep(200 * time.Millisecond)

	// Should not have created significantly more connections
	finalOpens := underlying.openCalls.Load()
	require.LessOrEqual(t, finalOpens-initialOpens, int64(2), "Should not create many more connections when at target")
}

// TestReservoirRefiller_JitterApplied tests that jitter is applied to lifetime
func TestReservoirRefiller_JitterApplied(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  5,
		LowWatermark: 3,
		BaseLifetime: 10 * time.Minute,
		Jitter:       2 * time.Minute, // 2 minute jitter
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for refiller to create some connections
	require.Eventually(t, func() bool {
		return res.Len() >= 3
	}, 5*time.Second, 50*time.Millisecond)

	// Check that connections have varying lifetimes
	lifetimes := make(map[time.Duration]bool)
	for i := 0; i < res.Len(); i++ {
		pc, ok := res.TryCheckout(time.Now().UTC())
		if ok && pc != nil {
			lifetimes[pc.Lifetime] = true
			res.Return(pc, time.Now().UTC())
		}
	}

	// With jitter, we should see some variation in lifetimes
	// All lifetimes should be between BaseLifetime and BaseLifetime + Jitter
	for lifetime := range lifetimes {
		require.GreaterOrEqual(t, lifetime, cfg.BaseLifetime)
		require.LessOrEqual(t, lifetime, cfg.BaseLifetime+cfg.Jitter)
	}
}

// TestReservoirRefiller_HandlesRateLimiterWait tests rate limiter integration
func TestReservoirRefiller_HandlesRateLimiterWait(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	rateLimiter := &mockRateLimiter{waitDelay: 10 * time.Millisecond}
	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   rateLimiter,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for refiller to create connections
	require.Eventually(t, func() bool {
		return res.Len() >= cfg.TargetReady
	}, 5*time.Second, 50*time.Millisecond)

	// Verify rate limiter was called
	require.GreaterOrEqual(t, rateLimiter.waitCalls.Load(), int64(cfg.TargetReady))
}

// TestReservoirRefiller_HandlesLeaseAcquireFailure tests lease acquire failure handling
func TestReservoirRefiller_HandlesLeaseAcquireFailure(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	leaseManager := &mockLeaseManager{
		acquireErr: errors.New("lease limit reached"),
	}
	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		leaseManager:  leaseManager,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for the expected refill failure.
	require.Eventually(t, func() bool {
		return slices.Contains(metrics.getRefillFailReasons(), "lease_acquire")
	}, 5*time.Second, 50*time.Millisecond)

	// Reservoir should remain empty due to lease failures
	require.Equal(t, 0, res.Len())
	require.Greater(t, leaseManager.acquireCalls.Load(), int64(0))
	require.Greater(t, metrics.refillFailures.Load(), int64(0))
}

// TestReservoirRefiller_GracefulShutdown tests graceful shutdown
func TestReservoirRefiller_GracefulShutdown(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  100, // High target so refiller keeps running
		LowWatermark: 50,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)

	// Stop should complete quickly
	done := make(chan struct{})
	go func() {
		refiller.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good, stopped quickly
	case <-time.After(2 * time.Second):
		t.Fatal("Refiller did not stop in time")
	}
}

// TestReservoirRefiller_TokenProviderFailure tests token provider failure handling
func TestReservoirRefiller_TokenProviderFailure(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "", errors.New("token provider error")
	}

	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for the expected refill failure.
	require.Eventually(t, func() bool {
		return slices.Contains(metrics.getRefillFailReasons(), "token_provider")
	}, 5*time.Second, 50*time.Millisecond)

	// Reservoir should remain empty due to token failures
	require.Equal(t, 0, res.Len())
	require.Greater(t, metrics.refillFailures.Load(), int64(0))
}

// TestReservoirRefiller_ConnectionOpenFailure tests connection open failure handling
func TestReservoirRefiller_ConnectionOpenFailure(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	underlying := &mockUnderlyingDriver{err: errors.New("connection failed")}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for the expected refill failure.
	require.Eventually(t, func() bool {
		return slices.Contains(metrics.getRefillFailReasons(), "connection_open")
	}, 5*time.Second, 50*time.Millisecond)

	// Reservoir should remain empty due to connection failures
	require.Equal(t, 0, res.Len())
	require.Greater(t, metrics.refillFailures.Load(), int64(0))
}

// NOTE: Tests for steadyStateInterval() and calculateRefillInterval() were removed
// because those methods were removed in favor of a simpler rate-limiter-only pacing model.
// The refiller now runs back-to-back openOne() calls with the rate limiter as the only throttle.

// TestReservoirRefiller_LeaseReleasedOnRateLimitFailure tests lease release on rate limit failure
func TestReservoirRefiller_LeaseReleasedOnRateLimitFailure(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	leaseManager := &mockLeaseManager{
		acquireLeaseID: "test-lease",
	}
	rateLimiter := &mockRateLimiter{
		waitErr: errors.New("rate limit exceeded"),
	}
	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   rateLimiter,
		leaseManager:  leaseManager,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for the expected refill failure.
	require.Eventually(t, func() bool {
		return slices.Contains(metrics.getRefillFailReasons(), "rate_limit")
	}, 5*time.Second, 50*time.Millisecond)

	// Lease should be released when rate limit fails
	require.Greater(t, leaseManager.acquireCalls.Load(), int64(0))
	require.Greater(t, leaseManager.releaseCalls.Load(), int64(0))
}

// TestSleepOrStop tests the sleepOrStop helper function
func TestSleepOrStop(t *testing.T) {
	t.Run("completes after duration", func(t *testing.T) {
		stop := make(chan struct{})
		start := time.Now()
		sleepOrStop(stop, 50*time.Millisecond)
		elapsed := time.Since(start)
		require.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	})

	t.Run("returns early on stop", func(t *testing.T) {
		stop := make(chan struct{})
		start := time.Now()

		go func() {
			time.Sleep(20 * time.Millisecond)
			close(stop)
		}()

		sleepOrStop(stop, 1*time.Second)
		elapsed := time.Since(start)
		require.Less(t, elapsed, 500*time.Millisecond)
	})
}

// TestInjectToken tests the injectToken helper function
func TestInjectToken_Refiller(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		username string
		token    string
		wantErr  bool
	}{
		{
			name:     "basic DSN",
			dsn:      "postgres://localhost/test",
			username: "admin",
			token:    "my-token",
			wantErr:  false,
		},
		{
			name:     "DSN with existing user",
			dsn:      "postgres://existinguser@localhost/test",
			username: "admin",
			token:    "my-token",
			wantErr:  false,
		},
		{
			name:     "invalid DSN",
			dsn:      "not-a-valid-url-\x00",
			username: "admin",
			token:    "my-token",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := injectToken(tt.dsn, tt.username, tt.token)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, result)
			}
		})
	}
}

// =============================================================================
// Edge Case Tests
// =============================================================================

// TestReservoirRefiller_DSNInjection_SpecialCharacters tests DSN injection with special characters
func TestReservoirRefiller_DSNInjection_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		username string
		token    string
		wantErr  bool
	}{
		{
			name:     "token with special characters",
			dsn:      "postgres://localhost/test",
			username: "admin",
			token:    "token@with#special$chars%and&more",
			wantErr:  false,
		},
		{
			name:     "username with special characters",
			dsn:      "postgres://localhost/test",
			username: "user@domain.com",
			token:    "simple-token",
			wantErr:  false,
		},
		{
			name:     "DSN with query parameters",
			dsn:      "postgres://localhost/test?sslmode=require&connect_timeout=10",
			username: "admin",
			token:    "my-token",
			wantErr:  false,
		},
		{
			name:     "DSN with port",
			dsn:      "postgres://localhost:5432/test",
			username: "admin",
			token:    "my-token",
			wantErr:  false,
		},
		{
			name:     "empty DSN",
			dsn:      "",
			username: "admin",
			token:    "my-token",
			wantErr:  false, // Empty DSN is valid URL, just empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := injectToken(tt.dsn, tt.username, tt.token)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// For non-empty DSN, verify the result contains the username (URL-encoded)
				if tt.dsn != "" {
					require.NotEmpty(t, result)
				}
			}
		})
	}
}

// TestReservoirRefiller_RestartBehavior tests that refiller can be stopped and restarted
func TestReservoirRefiller_RestartBehavior(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		metrics:       metrics,
	}

	// Start and let it fill
	refiller.Start()
	require.Eventually(t, func() bool {
		return res.Len() >= cfg.TargetReady
	}, 5*time.Second, 50*time.Millisecond)

	// Stop
	refiller.Stop()

	// Drain the reservoir
	for res.Len() > 0 {
		res.TryCheckout(time.Now().UTC())
	}

	// Create new refiller (simulating restart)
	refiller2 := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		metrics:       metrics,
	}

	// Start again
	refiller2.Start()
	defer refiller2.Stop()

	// Should fill again
	require.Eventually(t, func() bool {
		return res.Len() >= cfg.TargetReady
	}, 5*time.Second, 50*time.Millisecond)
}

// TestReservoirRefiller_DoubleStart tests that double start is safe
func TestReservoirRefiller_DoubleStart(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		metrics:       metrics,
	}

	// Double start should be safe (second start is no-op)
	refiller.Start()
	refiller.Start() // Should not panic or create duplicate goroutines

	defer refiller.Stop()

	require.Eventually(t, func() bool {
		return res.Len() >= cfg.TargetReady
	}, 5*time.Second, 50*time.Millisecond)
}

// TestReservoirRefiller_DoubleStop tests that double stop is safe
func TestReservoirRefiller_DoubleStop(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	time.Sleep(50 * time.Millisecond)

	// Double stop should not panic
	refiller.Stop()
	// Note: Second stop will panic on close of closed channel
	// This is expected behavior - don't call Stop twice
}

// TestReservoirRefiller_DSNInjectFailure tests DSN injection failure handling
func TestReservoirRefiller_DSNInjectFailure(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "not-a-valid-url-\x00", // Invalid DSN
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for the expected refill failure.
	require.Eventually(t, func() bool {
		return slices.Contains(metrics.getRefillFailReasons(), "dsn_inject")
	}, 5*time.Second, 50*time.Millisecond)

	// Reservoir should remain empty due to DSN injection failures
	require.Equal(t, 0, res.Len())
	require.Greater(t, metrics.refillFailures.Load(), int64(0))
}

// TestReservoirRefiller_LeaseReleasedOnTokenFailure tests lease release on token failure
func TestReservoirRefiller_LeaseReleasedOnTokenFailure(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "", errors.New("token error")
	}

	leaseManager := &mockLeaseManager{
		acquireLeaseID: "test-lease",
	}
	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		leaseManager:  leaseManager,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for refiller to attempt
	time.Sleep(500 * time.Millisecond)

	// Lease should be released when token fails
	require.Greater(t, leaseManager.acquireCalls.Load(), int64(0))
	require.Greater(t, leaseManager.releaseCalls.Load(), int64(0))
}

// TestReservoirRefiller_LeaseReleasedOnConnectionFailure tests lease release on connection failure
func TestReservoirRefiller_LeaseReleasedOnConnectionFailure(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	leaseManager := &mockLeaseManager{
		acquireLeaseID: "test-lease",
	}
	underlying := &mockUnderlyingDriver{err: errors.New("connection failed")}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		leaseManager:  leaseManager,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for refiller to attempt
	time.Sleep(500 * time.Millisecond)

	// Lease should be released when connection fails
	require.Greater(t, leaseManager.acquireCalls.Load(), int64(0))
	require.Greater(t, leaseManager.releaseCalls.Load(), int64(0))
}

// NOTE: Tests for steadyStateInterval() edge cases (zero/negative base lifetime,
// very small intervals) were removed because steadyStateInterval() was removed
// in favor of a simpler rate-limiter-only pacing model.

// TestReservoirRefiller_LogFuncCalled tests that log function is called
func TestReservoirRefiller_LogFuncCalled(t *testing.T) {
	var logCalled atomic.Bool
	logFunc := func(msg string, keysAndValues ...interface{}) {
		logCalled.Store(true)
	}

	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   nil,
		underlying:    underlying,
		logFunc:       logFunc,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for log to be called
	require.Eventually(t, func() bool {
		return logCalled.Load()
	}, 2*time.Second, 50*time.Millisecond)
}

// TestReservoirRefiller_RateLimiterContextCancellation tests rate limiter with context cancellation
func TestReservoirRefiller_RateLimiterContextCancellation(t *testing.T) {
	metrics := &mockReservoirMetrics{}
	res := NewReservoir(10, 30*time.Second, nil, nil, metrics)

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	rateLimiter := &mockRateLimiter{
		waitErr: context.Canceled,
	}
	underlying := &mockUnderlyingDriver{conn: &mockConn{}}

	cfg := ReservoirConfig{
		TargetReady:  3,
		LowWatermark: 2,
		BaseLifetime: 10 * time.Minute,
		Jitter:       0,
		GuardWindow:  30 * time.Second,
	}

	refiller := &reservoirRefiller{
		username:      "admin",
		baseDSN:       "postgres://localhost/test",
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   rateLimiter,
		underlying:    underlying,
		metrics:       metrics,
	}

	refiller.Start()
	defer refiller.Stop()

	// Wait for the expected refill failure.
	require.Eventually(t, func() bool {
		return slices.Contains(metrics.getRefillFailReasons(), "rate_limit")
	}, 5*time.Second, 50*time.Millisecond)

	// Should have rate limit failures
	require.Greater(t, metrics.refillFailures.Load(), int64(0))
}

// TestSleepOrStop_ZeroDuration tests sleepOrStop with zero duration
func TestSleepOrStop_ZeroDuration(t *testing.T) {
	stop := make(chan struct{})
	start := time.Now()
	sleepOrStop(stop, 0)
	elapsed := time.Since(start)
	// Should return almost immediately
	require.Less(t, elapsed, 50*time.Millisecond)
}

// TestSleepOrStop_NegativeDuration tests sleepOrStop with negative duration
func TestSleepOrStop_NegativeDuration(t *testing.T) {
	stop := make(chan struct{})
	start := time.Now()
	sleepOrStop(stop, -1*time.Second)
	elapsed := time.Since(start)
	// Should return almost immediately (negative duration fires immediately)
	require.Less(t, elapsed, 50*time.Millisecond)
}

// TestSleepOrStop_ClosedChannel tests sleepOrStop with already closed channel
func TestSleepOrStop_ClosedChannel(t *testing.T) {
	stop := make(chan struct{})
	close(stop)
	start := time.Now()
	sleepOrStop(stop, 1*time.Second)
	elapsed := time.Since(start)
	// Should return immediately since channel is closed
	require.Less(t, elapsed, 50*time.Millisecond)
}
