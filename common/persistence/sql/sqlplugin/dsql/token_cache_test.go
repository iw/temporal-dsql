package dsql

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type mockCredentialsProvider struct {
	retrieveFunc func(context.Context) (aws.Credentials, error)
	callCount    atomic.Int32
}

func (m *mockCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	m.callCount.Add(1)
	if m.retrieveFunc != nil {
		return m.retrieveFunc(ctx)
	}
	return fakeTokenCredentials(), nil
}

func fakeTokenCredentials() aws.Credentials {
	return aws.Credentials{AccessKeyID: "fake-access-key", SecretAccessKey: "fake-secret-key"}
}

func TestTokenExpiry(t *testing.T) {
	signedAt := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	query := url.Values{"X-Amz-Date": {signedAt.Format("20060102T150405Z")}, "X-Amz-Expires": {"100"}}
	for _, field := range []string{"X-Amz-Date", "X-Amz-Expires"} {
		for _, value := range []string{"", "invalid", "-1", strings.Repeat("9", 30), "1.5"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				values, err := url.ParseQuery(query.Encode())
				require.NoError(t, err)
				if value == "" {
					values.Del(field)
				} else {
					values.Set(field, value)
				}
				token := parseToken("db.example.invalid/?" + values.Encode())
				require.True(t, token.expiresAt.IsZero())
				require.False(t, token.isValid(signedAt))
				require.True(t, token.needsRefresh(signedAt))
			})
		}
	}
	for _, raw := range []string{"not-a-url", "db.example.invalid/?X-Amz-Date=%zz", "db.example.invalid/?" + query.Encode() + "&X-Amz-Expires=100"} {
		require.True(t, parseToken(raw).expiresAt.IsZero())
	}
	token := parseToken("db.example.invalid/?" + query.Encode())
	require.Equal(t, signedAt, token.signedAt)
	require.Equal(t, signedAt.Add(100*time.Second), token.expiresAt)
	require.True(t, token.isValid(signedAt.Add(89*time.Second)))
	require.False(t, token.isValid(signedAt.Add(90*time.Second)))
	require.False(t, token.isValid(token.expiresAt))
	require.False(t, token.needsRefresh(signedAt.Add(79*time.Second)))
	require.True(t, token.needsRefresh(signedAt.Add(80*time.Second)))
	query.Set("X-Amz-Expires", "10")
	token = parseToken("https://db.example.invalid/?" + query.Encode())
	require.True(t, token.isValid(signedAt.Add(4*time.Second)))
	require.False(t, token.isValid(signedAt.Add(5*time.Second)))
}

func TestTokenCacheShortLivedCredentials(t *testing.T) {
	timeSource := clock.NewEventTimeSource().Update(time.Now())
	var fail atomic.Bool
	provider := &mockCredentialsProvider{retrieveFunc: func(context.Context) (aws.Credentials, error) {
		if fail.Load() {
			return aws.Credentials{}, errors.New("credentials unavailable")
		}
		creds := fakeTokenCredentials()
		creds.CanExpire, creds.Expires = true, time.Now().Add(5*time.Second)
		return creds, nil
	}}
	// Exercise the real SDK offline, including its credential-lifetime truncation.
	signed, err := auth.GenerateDbConnectAuthToken(t.Context(), "db.example.invalid", "us-east-1", provider)
	require.NoError(t, err)
	lifetime := parseToken(signed).expiresAt.Sub(parseToken(signed).signedAt)
	require.Positive(t, lifetime)
	require.LessOrEqual(t, lifetime, 5*time.Second)

	cache := newTokenCache(provider, log.NewNoopLogger(), nil, timeSource.Now)
	t.Cleanup(cache.Stop)
	token, err := cache.GetToken(t.Context(), "db.example.invalid", "us-east-1", "temporal", 0)
	require.NoError(t, err, "a fresh token inside its margin is still usable")
	parsed := parseToken(token)
	require.False(t, parsed.isValid(timeSource.Now()))
	count := provider.callCount.Load()
	_, err = cache.GetToken(t.Context(), "db.example.invalid", "us-east-1", "temporal", 0)
	require.NoError(t, err)
	require.Equal(t, count+1, provider.callCount.Load(), "inside-margin tokens must not be reused from cache")

	fail.Store(true)
	timeSource.Update(parsed.expiresAt)
	token, err = cache.GetToken(t.Context(), "db.example.invalid", "us-east-1", "temporal", 0)
	require.ErrorContains(t, err, "credentials unavailable")
	require.Empty(t, token, "the expired cached token must not be served when refresh fails")
	fail.Store(false)
	timeSource.Advance(time.Minute)
	token, err = cache.GetToken(t.Context(), "db.example.invalid", "us-east-1", "temporal", 0)
	require.ErrorContains(t, err, "expired")
	require.Empty(t, token, "even freshly generated tokens must not be served past expiry")
}

func TestTokenCacheWarningRateLimit(t *testing.T) {
	timeSource := clock.NewEventTimeSource().Update(time.Now())
	provider := &mockCredentialsProvider{retrieveFunc: func(context.Context) (aws.Credentials, error) {
		creds := fakeTokenCredentials()
		creds.CanExpire, creds.Expires = true, timeSource.Now().Add(4*time.Second)
		return creds, nil
	}}
	core, logs := observer.New(zap.WarnLevel)
	cache := newTokenCache(provider, log.NewZapLogger(zap.New(core)), nil, timeSource.Now)
	t.Cleanup(cache.Stop)
	for i := range 3 {
		_, err := cache.GetToken(t.Context(), fmt.Sprintf("db%d.example.invalid", i), "us-east-1", "temporal", 0)
		require.NoError(t, err)
	}
	require.Equal(t, 1, logs.Len())
	timeSource.Advance(time.Minute)
	_, err := cache.GetToken(t.Context(), "db.example.invalid", "us-east-1", "temporal", 0)
	require.NoError(t, err)
	require.Equal(t, 2, logs.Len())
}

func TestTokenCacheCachedKeyDoesNotWaitForCredentials(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	provider := &mockCredentialsProvider{retrieveFunc: func(ctx context.Context) (aws.Credentials, error) {
		close(entered)
		select {
		case <-release:
			return fakeTokenCredentials(), nil
		case <-ctx.Done():
			return aws.Credentials{}, ctx.Err()
		}
	}}
	cache := NewTokenCache(provider, log.NewNoopLogger(), nil)
	t.Cleanup(cache.Stop)
	now := time.Now()
	key := poolKey{"cached.example.invalid", "us-east-1", adminUser, DefaultTokenDuration}
	cache.mu.Lock()
	cache.tokens[key] = &pooledToken{token: "cached-token", signedAt: now, expiresAt: now.Add(DefaultTokenDuration)}
	cache.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		_, err := cache.GetToken(t.Context(), "blocked.example.invalid", "us-east-1", adminUser, 0)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("generation did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	token, err := cache.GetToken(ctx, key.host, key.region, key.user, 0)
	require.NoError(t, err)
	require.Equal(t, "cached-token", token)
	close(release)
	require.NoError(t, <-done)
}

func TestTokenCacheCoalescesGeneration(t *testing.T) {
	entered, release := make(chan struct{}, 100), make(chan struct{})
	provider := &mockCredentialsProvider{retrieveFunc: func(ctx context.Context) (aws.Credentials, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return fakeTokenCredentials(), nil
		case <-ctx.Done():
			return aws.Credentials{}, ctx.Err()
		}
	}}
	cache := NewTokenCache(provider, log.NewNoopLogger(), nil)
	t.Cleanup(cache.Stop)
	var wg sync.WaitGroup
	tokens, errs := make([]string, 100), make([]error, 100)
	for i := range tokens {
		wg.Go(func() {
			tokens[i], errs[i] = cache.GetToken(t.Context(), "db.example.invalid", "us-east-1", adminUser, 0)
		})
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("generation did not start")
	}
	close(release)
	wg.Wait()
	for i := range tokens {
		require.NoError(t, errs[i])
		require.NotEmpty(t, tokens[i])
		require.Equal(t, tokens[0], tokens[i])
	}
	require.Equal(t, int32(1), provider.callCount.Load())
	require.Equal(t, 1, cache.Size())
	cache.Clear()
	require.Zero(t, cache.Size())
}

func TestTokenCacheRefreshSignedLifetimeAndMetrics(t *testing.T) {
	timeSource := clock.NewEventTimeSource().Update(time.Now())
	var fail atomic.Bool
	provider := &mockCredentialsProvider{retrieveFunc: func(context.Context) (aws.Credentials, error) {
		if fail.Load() {
			return aws.Credentials{}, errors.New("refresh failed")
		}
		creds := fakeTokenCredentials()
		creds.CanExpire, creds.Expires = true, time.Now().Add(100*time.Second)
		return creds, nil
	}}
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	t.Cleanup(func() { handler.StopCapture(capture) })
	cache := newTokenCache(provider, log.NewNoopLogger(), handler, timeSource.Now)
	t.Cleanup(cache.Stop)
	raw, err := cache.GetToken(t.Context(), "db.example.invalid", "us-east-1", adminUser, DefaultTokenDuration)
	require.NoError(t, err)
	token := parseToken(raw)
	lifetime := token.expiresAt.Sub(token.signedAt)
	timeSource.Update(token.signedAt.Add(time.Duration(float64(lifetime)*0.8) - time.Nanosecond))
	cache.refreshPools()
	require.Equal(t, int32(1), provider.callCount.Load())
	timeSource.Advance(time.Nanosecond)
	fail.Store(true)
	cache.refreshPools()
	require.Equal(t, int32(2), provider.callCount.Load(), "refresh uses signed, not requested lifetime")
	cached, err := cache.GetToken(t.Context(), "db.example.invalid", "us-east-1", adminUser, 0)
	require.NoError(t, err)
	require.Equal(t, raw, cached, "failed refresh preserves a token still outside its margin")
	fail.Store(false)
	cache.refreshPools()
	require.Equal(t, int32(3), provider.callCount.Load())
	snapshot := capture.Snapshot()
	require.Len(t, snapshot["dsql_token_cache_hits"], 1)
	require.Len(t, snapshot["dsql_token_cache_misses"], 1)
	require.Len(t, snapshot["dsql_token_refreshes"], 2)
	require.Len(t, snapshot["dsql_token_refresh_failures"], 1)
}

func TestTokenCacheCanceledWaitAndStop(t *testing.T) {
	entered, exited := make(chan struct{}), make(chan struct{})
	provider := &mockCredentialsProvider{retrieveFunc: func(ctx context.Context) (aws.Credentials, error) {
		close(entered)
		defer close(exited)
		<-ctx.Done()
		return aws.Credentials{}, ctx.Err()
	}}
	cache := NewTokenCache(provider, log.NewNoopLogger(), nil)
	t.Cleanup(cache.Stop)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := cache.GetToken(ctx, "db.example.invalid", "us-east-1", adminUser, 0)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("generation did not start")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	cache.Stop()
	cache.Stop()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel generation")
	}
}

func TestTokenCacheBackgroundSchedule(t *testing.T) {
	timeSource := clock.NewEventTimeSource().Update(time.Now())
	cache := newTokenCache(&mockCredentialsProvider{}, log.NewNoopLogger(), nil, timeSource.Now)
	t.Cleanup(cache.Stop)
	key := poolKey{"db.example.invalid", "us-east-1", "temporal", DefaultTokenDuration}
	token := &pooledToken{signedAt: timeSource.Now(), expiresAt: timeSource.Now().Add(5 * time.Second)}
	cache.tokens[key] = token
	require.Equal(t, 4*time.Second, cache.nextRefreshDelay(), "wake at 80% even for a token shorter than the regular interval")
	timeSource.Advance(3 * time.Second)
	require.Equal(t, time.Second, cache.nextRefreshDelay())
	timeSource.Advance(time.Second)
	require.True(t, token.needsRefresh(timeSource.Now()))
	require.Equal(t, backgroundRefreshInterval, cache.nextRefreshDelay(), "back off after a failed refresh instead of spinning")
}
