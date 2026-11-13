package testcontext

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/debug"
	"google.golang.org/grpc/metadata"
)

const (
	defaultTimeout      = 90 * time.Second
	testNameMetadataKey = "temporal-test-name"
)

type contextStore struct {
	sync.Mutex
	byTest map[testing.TB]*contextState
}

// testContexts is process-global so repeated helpers in the same test share
// one context and one cleanup.
var testContexts = contextStore{
	byTest: make(map[testing.TB]*contextState),
}

type config struct {
	timeout    time.Duration
	timeoutSet bool
}

type contextDecorator struct {
	key      any
	decorate func(context.Context) context.Context
}

// DefaultTimeout returns the effective default timeout for test-scoped contexts.
func DefaultTimeout() time.Duration {
	return effectiveTimeout(0)
}

// For returns the test-scoped context for tb. The context is canceled
// when the test ends or when the configured test timeout expires.
//
// The first call creates the per-test context and fixes its timeout. Later calls
// return the same context, but an explicit different timeout fails instead of
// being silently ignored.
func For(tb testing.TB, opts ...Option) context.Context {
	tb.Helper()

	cfg := config{timeout: DefaultTimeout()}
	for _, opt := range opts {
		opt(&cfg)
	}

	st := getContextState(tb, cfg.timeout)
	st.configure(tb, cfg)
	return st.context()
}

// Option configures the test-scoped context returned by [For].
type Option func(*config)

// WithTimeout sets a custom timeout for the test-scoped context.
func WithTimeout(timeout time.Duration) Option {
	return func(cfg *config) {
		if timeout <= 0 {
			return
		}
		cfg.timeout = effectiveTimeout(timeout)
		cfg.timeoutSet = true
	}
}

// AttachDecorator applies decorator to the test-scoped context once for key.
// Reusing the same key is a no-op. If the test context does not exist yet,
// AttachDecorator creates it with the default timeout. Call [For] with [WithTimeout]
// first when using a custom timeout.
func AttachDecorator[K comparable](tb testing.TB, key K, decorator func(context.Context) context.Context) {
	tb.Helper()

	st := getContextState(tb, DefaultTimeout())
	st.attachDecorator(tb, contextDecorator{
		key:      key,
		decorate: decorator,
	})
}

// HandOffToSubtests records that tb has started parallel subtests. Go starts them once
// tb's function returns and a parallel slot is free, and runs tb's cleanup only after
// they finish, so tb's deadline keeps running while they wait for a slot and run. That
// time isn't tb's own, and the subtests have contexts of their own, so tb fails for its
// timeout only if the deadline had passed by the time it handed off. Without a context
// for tb, it does nothing.
func HandOffToSubtests(tb testing.TB) {
	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	testContexts.Unlock()
	if ok {
		st.handOff()
	}
}

type contextState struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	timeout    time.Duration
	decorators map[any]struct{}
	// handedOff records that the test started parallel subtests, and lateHandOff that
	// its deadline had passed by then.
	handedOff   bool
	lateHandOff bool
}

func getContextState(tb testing.TB, timeout time.Duration) *contextState {
	tb.Helper()

	testContexts.Lock()
	defer testContexts.Unlock()

	if st, ok := testContexts.byTest[tb]; ok {
		return st
	}

	ctx, cancel := context.WithTimeout(tb.Context(), timeout)

	// Annotate gRPC requests with the test name for OTEL tracing.
	ctx = metadata.AppendToOutgoingContext(ctx, testNameMetadataKey, tb.Name())

	st := &contextState{
		ctx:        ctx,
		cancel:     cancel,
		timeout:    timeout,
		decorators: make(map[any]struct{}),
	}
	testContexts.byTest[tb] = st

	tb.Cleanup(func() {
		timedOut := st.timedOut()
		st.cancel()
		testContexts.Lock()
		delete(testContexts.byTest, tb)
		testContexts.Unlock()
		if timedOut {
			tb.Errorf("test exceeded timeout of %v", st.timeout)
		}
		st.release()
	})
	return st
}

func (s *contextState) configure(tb testing.TB, cfg config) {
	tb.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if cfg.timeoutSet && cfg.timeout != s.timeout {
		tb.Fatalf("testcontext: test context already exists with timeout %v; cannot change it to %v", s.timeout, cfg.timeout)
	}

}

func (s *contextState) attachDecorator(tb testing.TB, decorator contextDecorator) {
	tb.Helper()

	// Decorators may be registered by independent helpers, so apply each keyed
	// decorator at most once while preserving call order.
	s.mu.Lock()
	defer s.mu.Unlock()

	if decorator.key == nil {
		tb.Fatal("testcontext: context decorator key must not be nil")
	}
	if decorator.decorate == nil {
		tb.Fatal("testcontext: context decorator must not be nil")
	}
	if _, ok := s.decorators[decorator.key]; ok {
		return
	}
	s.ctx = decorator.decorate(s.ctx)
	s.decorators[decorator.key] = struct{}{}
}

func (s *contextState) context() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctx
}

// timedOut reports whether the test's deadline passed while the test was running
// itself: at all, or by the time it handed off to parallel subtests.
func (s *contextState) timedOut() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !errors.Is(s.ctx.Err(), context.DeadlineExceeded) {
		return false
	}
	return !s.handedOff || s.lateHandOff
}

func (s *contextState) handOff() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx == nil {
		return // the test has ended
	}
	if errors.Is(s.ctx.Err(), context.DeadlineExceeded) {
		s.lateHandOff = true
	}
	s.handedOff = true
}

func (s *contextState) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctx = nil
}

func effectiveTimeout(customTimeout time.Duration) (timeout time.Duration) {
	defer func() {
		// Build flag TEMPORAL_DEBUG applies a timeout multiplier to all test timeouts.
		timeout *= debug.TimeoutMultiplier
	}()

	// 1. Custom timeout (via WithTimeout option).
	if customTimeout > 0 {
		return customTimeout
	}

	// 2. TEMPORAL_TEST_TIMEOUT environment variable.
	if envTimeout := os.Getenv("TEMPORAL_TEST_TIMEOUT"); envTimeout != "" {
		if dur, err := time.ParseDuration(envTimeout); err == nil && dur > 0 {
			return dur
		}
	}

	// 3. Default timeout.
	return defaultTimeout
}
