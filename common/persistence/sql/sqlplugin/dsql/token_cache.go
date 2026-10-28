// Token cache for DSQL IAM authentication tokens.
package dsql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"golang.org/x/sync/singleflight"
)

const (
	// DefaultTokenDuration is the requested token lifetime; credentials can shorten it.
	DefaultTokenDuration      = 15 * time.Minute
	MinTokenDuration          = 15 * time.Second
	RefreshAtLifetimePercent  = 0.8
	adminUser                 = "admin"
	TokenDurationEnvVar       = "DSQL_TOKEN_DURATION"
	backgroundRefreshInterval = 10 * time.Second
)

var (
	dsqlTokenCacheHits       = metrics.NewCounterDef("dsql_token_cache_hits")
	dsqlTokenCacheMisses     = metrics.NewCounterDef("dsql_token_cache_misses")
	dsqlTokenRefreshes       = metrics.NewCounterDef("dsql_token_refreshes")
	dsqlTokenRefreshFailures = metrics.NewCounterDef("dsql_token_refresh_failures")
)

type poolKey struct {
	host          string
	region        string
	user          string
	tokenDuration time.Duration
}

func (key poolKey) flightKey() string {
	return fmt.Sprintf("%q/%q/%q/%d", key.host, key.region, key.user, key.tokenDuration)
}

type pooledToken struct {
	token     string
	signedAt  time.Time
	expiresAt time.Time
}

// parseToken fails closed: missing or malformed expiry fields leave a zero expiry.
func parseToken(token string) *pooledToken {
	pt := &pooledToken{token: token}
	_, query, ok := strings.Cut(token, "?")
	if !ok {
		return pt
	}
	values, err := url.ParseQuery(query)
	if err != nil || len(values["X-Amz-Date"]) != 1 || len(values["X-Amz-Expires"]) != 1 {
		return pt
	}
	signedAt, err := time.Parse("20060102T150405Z", values.Get("X-Amz-Date"))
	if err != nil {
		return pt
	}
	seconds, err := strconv.ParseInt(values.Get("X-Amz-Expires"), 10, 64)
	if err != nil || seconds <= 0 || seconds > math.MaxInt64/int64(time.Second) {
		return pt
	}
	pt.signedAt = signedAt
	pt.expiresAt = signedAt.Add(time.Duration(seconds) * time.Second)
	return pt
}

func (pt *pooledToken) isValid(now time.Time) bool {
	margin := max(pt.expiresAt.Sub(pt.signedAt)/10, 5*time.Second)
	return now.Before(pt.expiresAt.Add(-margin))
}

func (pt *pooledToken) refreshAt() time.Time {
	lifetime := pt.expiresAt.Sub(pt.signedAt)
	return pt.signedAt.Add(time.Duration(float64(lifetime) * RefreshAtLifetimePercent))
}

func (pt *pooledToken) needsRefresh(now time.Time) bool {
	return !now.Before(pt.refreshAt())
}

// TokenCache keeps one reusable token per key. Signing and credential retrieval
// run outside mu, with singleflight sharing generation between concurrent callers.
type TokenCache struct {
	mu                  sync.RWMutex
	tokens              map[poolKey]*pooledToken
	generations         singleflight.Group
	credentialsProvider aws.CredentialsProvider
	logger              log.Logger
	metricsHandler      metrics.Handler
	now                 func() time.Time
	lastWarningAt       time.Time
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	refreshWake         chan struct{}
}

func NewTokenCache(provider aws.CredentialsProvider, logger log.Logger, handler metrics.Handler) *TokenCache {
	tc := newTokenCache(provider, logger, handler, time.Now)
	tc.wg.Add(1)
	go tc.backgroundRefreshLoop()
	return tc
}

func newTokenCache(provider aws.CredentialsProvider, logger log.Logger, handler metrics.Handler, now func() time.Time) *TokenCache {
	ctx, cancel := context.WithCancel(context.Background())
	tc := &TokenCache{
		tokens:              make(map[poolKey]*pooledToken),
		credentialsProvider: provider,
		logger:              logger,
		metricsHandler:      handler,
		now:                 now,
		ctx:                 ctx,
		cancel:              cancel,
		refreshWake:         make(chan struct{}, 1),
	}
	return tc
}

// Stop cancels background refresh, including an in-flight credentials fetch.
func (tc *TokenCache) Stop() {
	tc.cancel()
	tc.wg.Wait()
}

func (tc *TokenCache) backgroundRefreshLoop() {
	defer tc.wg.Done()
	timer := time.NewTimer(tc.nextRefreshDelay())
	defer timer.Stop()
	for {
		select {
		case <-tc.ctx.Done():
			return
		case <-tc.refreshWake:
		case <-timer.C:
			tc.refreshPools()
		}
		timer.Reset(tc.nextRefreshDelay())
	}
}

func (tc *TokenCache) nextRefreshDelay() time.Duration {
	now := tc.now()
	delay := backgroundRefreshInterval
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	for _, token := range tc.tokens {
		untilRefresh := token.refreshAt().Sub(now)
		// Retry overdue tokens on the regular interval after a failed refresh,
		// rather than spinning. Short-lived tokens wake the loop before that interval.
		if untilRefresh > 0 {
			delay = min(delay, untilRefresh)
		}
	}
	return delay
}

func (tc *TokenCache) refreshPools() {
	now := tc.now()
	tc.mu.RLock()
	pending := make(map[poolKey]*pooledToken)
	for key, token := range tc.tokens {
		if token.needsRefresh(now) {
			pending[key] = token
		}
	}
	tc.mu.RUnlock()
	for key, token := range pending {
		if tc.ctx.Err() != nil {
			return
		}
		if _, err := tc.refreshToken(tc.ctx, key, token); err != nil && tc.logger != nil {
			tc.logger.Error("Background token refresh failed", tag.Error(err),
				tag.NewStringTag("host", key.host), tag.NewStringTag("user", key.user))
		}
	}
}

func (tc *TokenCache) cachedToken(key poolKey) *pooledToken {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.tokens[key]
}

// GetToken serves cached tokens only outside their safety margin. A newly signed
// token inside the margin may be returned to its waiters, but never after expiry.
func (tc *TokenCache) GetToken(ctx context.Context, host, region, user string, duration time.Duration) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if duration == 0 {
		duration = DefaultTokenDuration
	}
	if user == "" {
		user = adminUser
	}
	key := poolKey{host: host, region: region, user: user, tokenDuration: duration}
	token := tc.cachedToken(key)
	if token != nil && token.isValid(tc.now()) {
		tc.recordCacheHit()
		return token.token, nil
	}
	tc.recordCacheMiss()
	token, err := tc.refreshToken(ctx, key, token)
	if err != nil {
		return "", err
	}
	// A waiter may resume after the generated token has already expired.
	if !tc.now().Before(token.expiresAt) {
		return "", errors.New("DSQL IAM token has expired")
	}
	return token.token, nil
}

func (tc *TokenCache) refreshToken(ctx context.Context, key poolKey, previous *pooledToken) (*pooledToken, error) {
	result := tc.generations.DoChan(key.flightKey(), func() (interface{}, error) {
		// Another generation may have completed between the cache lookup and joining the flight.
		if current := tc.cachedToken(key); current != nil && current != previous && current.isValid(tc.now()) {
			return current, nil
		}
		token, err := tc.generateToken(key)
		if err != nil {
			tc.recordTokenRefreshFailure()
			return nil, err
		}
		now := tc.now()
		if !now.Before(token.expiresAt) {
			tc.recordTokenRefreshFailure()
			return nil, errors.New("generated DSQL IAM token has expired or has invalid expiry")
		}
		tc.mu.Lock()
		tc.tokens[key] = token
		warn := !token.isValid(now) && (tc.lastWarningAt.IsZero() || now.Sub(tc.lastWarningAt) >= time.Minute)
		if warn {
			tc.lastWarningAt = now
		}
		tc.mu.Unlock()
		select {
		case tc.refreshWake <- struct{}{}:
		default:
		}
		if warn && tc.logger != nil {
			tc.logger.Warn("Generated DSQL IAM token is inside its expiry safety margin",
				tag.NewStringTag("host", key.host), tag.NewStringTag("user", key.user),
				tag.NewDurationTag("remaining_lifetime", token.expiresAt.Sub(now)))
		}
		tc.recordTokenRefresh()
		tc.logTokenRefresh(key.host, key.region, key.user, token.expiresAt.Sub(token.signedAt))
		return token, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-result:
		if res.Err != nil {
			return nil, res.Err
		}
		token, ok := res.Val.(*pooledToken)
		if !ok {
			return nil, errors.New("unexpected DSQL token generation result")
		}
		return token, nil
	}
}

func (tc *TokenCache) generateToken(key poolKey) (*pooledToken, error) {
	// Callers cancel their own wait without canceling generation for other waiters.
	ctx, cancel := context.WithTimeout(tc.ctx, 30*time.Second)
	defer cancel()
	options := func(opts *auth.TokenOptions) { opts.ExpiresIn = key.tokenDuration }
	var token string
	var err error
	if key.user == adminUser {
		token, err = auth.GenerateDBConnectAdminAuthToken(ctx, key.host, key.region, tc.credentialsProvider, options)
	} else {
		token, err = auth.GenerateDbConnectAuthToken(ctx, key.host, key.region, tc.credentialsProvider, options)
	}
	if err != nil {
		return nil, err
	}
	return parseToken(token), nil
}

// Clear removes all cached tokens.
func (tc *TokenCache) Clear() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.tokens = make(map[poolKey]*pooledToken)
}

// Size returns the number of cached tokens.
func (tc *TokenCache) Size() int {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return len(tc.tokens)
}

// Metrics helpers
func (tc *TokenCache) recordCacheHit() {
	if tc.metricsHandler != nil {
		dsqlTokenCacheHits.With(tc.metricsHandler).Record(1)
	}
}

func (tc *TokenCache) recordCacheMiss() {
	if tc.metricsHandler != nil {
		dsqlTokenCacheMisses.With(tc.metricsHandler).Record(1)
	}
}

func (tc *TokenCache) recordTokenRefresh() {
	if tc.metricsHandler != nil {
		dsqlTokenRefreshes.With(tc.metricsHandler).Record(1)
	}
}

func (tc *TokenCache) recordTokenRefreshFailure() {
	if tc.metricsHandler != nil {
		dsqlTokenRefreshFailures.With(tc.metricsHandler).Record(1)
	}
}

func (tc *TokenCache) logTokenRefresh(host, region, user string, duration time.Duration) {
	if tc.logger != nil {
		tc.logger.Debug("DSQL IAM token refreshed",
			tag.NewStringTag("host", host),
			tag.NewStringTag("region", region),
			tag.NewStringTag("user", user),
			tag.NewDurationTag("token_duration", duration))
	}
}

// ResolveCredentialsProvider resolves the AWS credentials provider once.
func ResolveCredentialsProvider(ctx context.Context, region string) (aws.CredentialsProvider, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region),
		awsconfig.WithCredentialsCacheOptions(func(options *aws.CredentialsCacheOptions) {
			options.ExpiryWindow = 5 * time.Minute
			options.ExpiryWindowJitterFrac = 0.2
		}))
	if err != nil {
		return nil, err
	}
	return cfg.Credentials, nil
}
