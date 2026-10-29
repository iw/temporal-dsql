package dsql

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func settingsConfig() *config.SQL {
	return &config.SQL{ConnectAddr: "test-cluster.dsql.us-east-1.on.aws:5432"}
}

func testSettings(t *testing.T, attrs map[string]string) settings {
	t.Helper()
	cfg := settingsConfig()
	cfg.ConnectAttributes = attrs
	resolved, err := resolveSettings(cfg, log.NewNoopLogger())
	require.NoError(t, err)
	return resolved
}

func TestResolveSettingsAttributes(t *testing.T) {
	for _, tc := range []struct {
		attribute, fallback, value string
		get                        func(settings) string
	}{
		{"token_duration", "15m0s", "10m0s", func(s settings) string { return s.TokenDuration.String() }},
		{"connection_rate_limit", "10", "12", func(s settings) string { return strconv.Itoa(s.Limiter.ConnectionRate) }},
		{"connection_burst_limit", "100", "120", func(s settings) string { return strconv.Itoa(s.Limiter.ConnectionBurst) }},
		{"rate_limiter_table", "", "test-table", func(s settings) string { return s.Limiter.Table }},
		{"token_bucket_rate", "100", "80", func(s settings) string { return strconv.Itoa(s.Limiter.TokenBucketRate) }},
		{"token_bucket_capacity", "1000", "800", func(s settings) string { return strconv.Itoa(s.Limiter.TokenBucketCapacity) }},
		{"token_bucket_max_wait", "30s", "10s", func(s settings) string { return s.Limiter.TokenBucketMaxWait.String() }},
	} {
		t.Run(tc.attribute, func(t *testing.T) {
			require.Equal(t, tc.fallback, tc.get(testSettings(t, nil)))
			require.Equal(t, tc.value, tc.get(testSettings(t, map[string]string{" " + tc.attribute + " ": " " + tc.value + " "})))
		})
	}
}

func TestResolveSettingsEndpointAndRegion(t *testing.T) {
	for _, tc := range []struct {
		name, address, endpoint, region, wantEndpoint, wantRegion string
	}{
		{"host", "first.dsql.eu-west-1.on.aws:5432", "", "", "first.dsql.eu-west-1.on.aws", "eu-west-1"},
		{"endpoint attribute", "first.dsql.eu-west-1.on.aws:5432", "second.dsql.us-west-2.on.aws", "", "second.dsql.us-west-2.on.aws", "us-west-2"},
		{"region attribute", "first.dsql.eu-west-1.on.aws:5432", "", "us-east-2", "first.dsql.eu-west-1.on.aws", "us-east-2"},
		{"IPv6 host", "[::1]:5432", "", "us-east-1", "::1", "us-east-1"},
		{"case and trailing dot", "FIRST.DSQL.EU-WEST-1.ON.AWS.:5432", "", "", "FIRST.DSQL.EU-WEST-1.ON.AWS.", "eu-west-1"},
		{"custom signing endpoint", "first.dsql.eu-west-1.on.aws:5432", "proxy.example.invalid", "us-east-1", "proxy.example.invalid", "us-east-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attrs := map[string]string{}
			if tc.endpoint != "" {
				attrs["cluster_endpoint"] = tc.endpoint
			}
			if tc.region != "" {
				attrs["region"] = tc.region
			}
			s, err := resolveSettings(&config.SQL{ConnectAddr: tc.address, ConnectAttributes: attrs}, log.NewNoopLogger())
			require.NoError(t, err)
			require.Equal(t, tc.wantEndpoint, s.ClusterEndpoint)
			require.Equal(t, tc.wantRegion, s.Region)
		})
	}
	for _, address := range []string{"", "db.example.invalid:5432", "[::1]:5432", "first.dsql.eu-west-1.on.aws.example:5432", ".dsql.eu-west-1.on.aws:5432"} {
		t.Run("missing region/"+address, func(t *testing.T) {
			_, err := resolveSettings(&config.SQL{ConnectAddr: address}, log.NewNoopLogger())
			require.ErrorContains(t, err, "set connectAttributes.region")
		})
	}
}

func TestResolveSettingsModes(t *testing.T) {
	require.Equal(t, localMode, testSettings(t, nil).Limiter.Mode)
	for _, mode := range []rateLimiterMode{localMode, tokenBucketMode} {
		t.Run(string(mode), func(t *testing.T) {
			require.Equal(t, mode, testSettings(t, map[string]string{"rate_limiter": string(mode), "rate_limiter_table": "test-table"}).Limiter.Mode)
		})
	}
}

func TestResolveSettingsValidation(t *testing.T) {
	for _, key := range settingAttributes {
		var invalid []string
		switch key {
		case "connection_rate_limit", "connection_burst_limit", "token_bucket_rate", "token_bucket_capacity":
			invalid = []string{"", "invalid", "0", "-1", strings.Repeat("9", 30)}
		case "token_duration":
			invalid = []string{"", "invalid", "0s", "-1s", "14s", "15m1s"}
		case "token_bucket_max_wait":
			invalid = []string{"", "invalid", "0s", "-1s", strings.Repeat("9", 24) + "h"}
		default:
			continue
		}
		for _, value := range invalid {
			t.Run(key+"/"+value, func(t *testing.T) {
				cfg := settingsConfig()
				cfg.ConnectAttributes = map[string]string{key: value}
				_, err := resolveSettings(cfg, log.NewNoopLogger())
				require.ErrorContains(t, err, fmt.Sprintf("%s=%q", key, value))
			})
		}
	}
	for _, tc := range []struct {
		name  string
		attrs map[string]string
		want  string
	}{
		{"empty region", map[string]string{"region": ""}, `region=""`},
		{"invalid mode", map[string]string{"rate_limiter": "typo"}, `rate_limiter="typo"`},
		{"empty mode", map[string]string{"rate_limiter": ""}, `rate_limiter=""`},
		{"missing table", map[string]string{"rate_limiter": "dynamodb_token_bucket"}, `rate_limiter_table=""`},
		{"blank table", map[string]string{"rate_limiter": "dynamodb_token_bucket", "rate_limiter_table": " "}, `rate_limiter_table=""`},
		{"duplicate attribute", map[string]string{"region": "us-east-1", " region ": "us-west-2"}, "duplicate connectAttribute region="},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := settingsConfig()
			cfg.ConnectAttributes = tc.attrs
			_, err := resolveSettings(cfg, log.NewNoopLogger())
			require.ErrorContains(t, err, tc.want)
		})
	}
	for _, value := range []string{"15s", "15m"} {
		t.Run("token boundary "+value, func(t *testing.T) { testSettings(t, map[string]string{"token_duration": value}) })
	}
}

func TestResolveSettingsUnusedWarning(t *testing.T) {
	for _, tc := range []struct {
		key, value string
	}{
		{"token_bucket_rate", "50"}, {"token_bucket_capacity", "500"}, {"token_bucket_max_wait", "10s"}, {"rate_limiter_table", "test-table"},
	} {
		for _, mode := range []rateLimiterMode{localMode, tokenBucketMode} {
			t.Run(string(mode)+"/"+tc.key, func(t *testing.T) {
				cfg := settingsConfig()
				cfg.ConnectAttributes = map[string]string{"rate_limiter": string(mode), tc.key: tc.value}
				if mode == tokenBucketMode {
					cfg.ConnectAttributes["rate_limiter_table"] = "test-table"
				}
				core, logs := observer.New(zap.WarnLevel)
				_, err := resolveSettings(cfg, log.NewZapLogger(zap.New(core)))
				require.NoError(t, err)
				if mode == tokenBucketMode {
					require.Zero(t, logs.Len())
				} else {
					require.Equal(t, 1, logs.Len())
					require.Equal(t, tc.key, logs.All()[0].ContextMap()["setting"])
				}
			})
		}
	}
}

func TestSettingsDSNFiltering(t *testing.T) {
	cfg := settingsConfig()
	cfg.DatabaseName, cfg.User, cfg.Password = "postgres", "test-user", "ignored"
	cfg.ConnectAttributes = map[string]string{"sslmode": "verify-full", "application_name": "test-app", "search_path": "public"}
	for _, key := range settingAttributes {
		cfg.ConnectAttributes[" "+key+" "] = "not-a-server-parameter"
	}
	before := *cfg
	before.ConnectAttributes = maps.Clone(cfg.ConnectAttributes)
	pool, err := (&plugin{}).poolConfig(cfg, resolver.NewNoopResolver(), nil, log.NewNoopLogger())
	require.NoError(t, err)
	dsn, err := url.Parse(pool.ConnConfig.ConnString())
	require.NoError(t, err)
	for _, key := range settingAttributes {
		require.NotContains(t, dsn.Query(), key)
		require.NotContains(t, pool.ConnConfig.RuntimeParams, key)
	}
	require.Equal(t, "verify-full", dsn.Query().Get("sslmode"))
	require.Equal(t, "test-app", pool.ConnConfig.RuntimeParams["application_name"])
	require.Equal(t, "public", pool.ConnConfig.RuntimeParams["search_path"])
	require.Equal(t, before, *cfg)
}

func TestSettingsCreateDBValidation(t *testing.T) {
	for _, attrs := range []map[string]string{
		{"rate_limiter": "dynamodb_token_bucket"},
		{"connection_rate_limit": "typo"}, {"token_duration": "1h"},
	} {
		t.Run(fmt.Sprint(attrs), func(t *testing.T) {
			cfg := settingsConfig()
			cfg.ConnectAttributes = attrs
			p := &plugin{}
			db, err := p.CreateDB(sqlplugin.DbKindMain, cfg, nil, log.NewNoopLogger(), metrics.NoopMetricsHandler)
			require.Error(t, err)
			require.Nil(t, db)
			require.Nil(t, p.rateLimiter)
			require.Nil(t, p.tokenCache)
		})
	}
}

func TestSettingsLimiterFirstPoolWins(t *testing.T) {
	cfg := settingsConfig()
	cfg.ConnectAttributes = map[string]string{"connection_rate_limit": "3", "connection_burst_limit": "4"}
	s, err := resolveSettings(cfg, log.NewNoopLogger())
	require.NoError(t, err)
	p := &plugin{}
	core, logs := observer.New(zap.WarnLevel)
	logger := log.NewZapLogger(zap.New(core))
	p.initRateLimiter(s, logger, nil)
	original := p.rateLimiter
	// Token lifetime, another local endpoint and unused mode settings do not affect pacing.
	unused := s
	unused.TokenDuration, unused.ClusterEndpoint, unused.Region = time.Minute, "another.example.invalid", "us-west-2"
	unused.Limiter.TokenBucketRate++
	p.initRateLimiter(unused, logger, nil)
	require.Zero(t, logs.Len())
	different := s
	different.Limiter.ConnectionRate, different.Limiter.ConnectionBurst = 5, 6
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { p.initRateLimiter(different, logger, nil) })
	}
	wg.Wait()
	require.Same(t, original, p.rateLimiter)
	require.Equal(t, 1, logs.Len())
	fields := logs.All()[0].ContextMap()
	require.Contains(t, fields["settings_in_use"], "connection_rate_limit=3")
	require.Contains(t, fields["settings_in_use"], "connection_burst_limit=4")
	require.Contains(t, fields["requested_settings"], "connection_rate_limit=5")
}

func TestSettingsLimiterConstruction(t *testing.T) {
	cfg := settingsConfig()
	cfg.ConnectAttributes = map[string]string{
		"region": "eu-west-2", "rate_limiter": "dynamodb_token_bucket", "rate_limiter_table": "test-table",
		"connection_rate_limit": "3", "connection_burst_limit": "4", "token_bucket_rate": "50",
		"token_bucket_capacity": "500", "token_bucket_max_wait": "12s",
	}
	s, err := resolveSettings(cfg, log.NewNoopLogger())
	require.NoError(t, err)
	bucket := newTokenBucketLimiter(nil, s)
	require.Equal(t, int64(50), bucket.Rate)
	require.Equal(t, int64(500), bucket.Capacity)
	require.Equal(t, 12*time.Second, bucket.MaxWait)
	require.Equal(t, "test-table", bucket.tableName)
	require.Equal(t, s.ClusterEndpoint, bucket.endpoint)
	local := newConnectionRateLimiter(s)
	require.InDelta(t, 3, float64(local.limiter.Limit()), 0.001)
	require.Equal(t, 4, local.limiter.Burst())
	fallback := newFailOpenRateLimiterWithSettings(nil, s, nil, nil)
	require.Equal(t, 4, fallback.local.(*ConnectionRateLimiter).limiter.Burst())
	// SDK config loading need not contact AWS to construct a client.
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "absent-config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "absent-credentials"))
	client, err := (&plugin{}).createDynamoDBClient(t.Context(), s, log.NewNoopLogger())
	require.NoError(t, err)
	require.Equal(t, "eu-west-2", client.Options().Region)
}

func TestSettingsDynamoDBClientFailureFallback(t *testing.T) {
	t.Setenv("AWS_MAX_ATTEMPTS", "invalid")
	cfg := settingsConfig()
	cfg.ConnectAttributes = map[string]string{"rate_limiter": "dynamodb_token_bucket", "rate_limiter_table": "test-table", "connection_burst_limit": "7"}
	s, err := resolveSettings(cfg, log.NewNoopLogger())
	require.NoError(t, err)
	p := &plugin{}
	core, logs := observer.New(zap.WarnLevel)
	p.initRateLimiter(s, log.NewZapLogger(zap.New(core)), nil)
	local, ok := p.rateLimiter.(*ConnectionRateLimiter)
	require.True(t, ok)
	require.Equal(t, 7, local.limiter.Burst())
	require.Equal(t, 1, logs.Len())
	require.Contains(t, logs.All()[0].Message, "falling back to local rate limiter")
	p.initRateLimiter(s, log.NewZapLogger(zap.New(core)), nil)
	require.Equal(t, 1, logs.Len())
	s.Limiter.TokenBucketRate++
	p.initRateLimiter(s, log.NewZapLogger(zap.New(core)), nil)
	require.Equal(t, 2, logs.Len())
	require.Contains(t, logs.All()[1].ContextMap()["settings_in_use"], "rate_limiter=local")
}

func TestSettingsTokenProviderUsesDatastore(t *testing.T) {
	cache := NewTokenCache(&mockCredentialsProvider{}, log.NewNoopLogger(), nil)
	t.Cleanup(cache.Stop)
	for _, tc := range []struct{ endpoint, region, duration string }{
		{"first.dsql.us-east-1.on.aws", "us-east-1", "1m"},
		{"second.dsql.eu-west-2.on.aws", "eu-west-2", "2m"},
	} {
		cfg := &config.SQL{User: "test-role", ConnectAddr: tc.endpoint + ":5432", ConnectAttributes: map[string]string{"token_duration": tc.duration}}
		s, err := resolveSettings(cfg, log.NewNoopLogger())
		require.NoError(t, err)
		// The resolved settings own the token duration.
		cfg.ConnectAttributes["token_duration"] = "invalid"
		provider := (&plugin{}).tokenProvider(cfg, cache, s, log.NewNoopLogger())
		token, err := provider(t.Context())
		require.NoError(t, err)
		parsed, err := url.Parse("https://" + token)
		require.NoError(t, err)
		require.Equal(t, tc.endpoint, parsed.Host)
		require.Contains(t, parsed.Query().Get("X-Amz-Credential"), "/"+tc.region+"/")
		duration, err := time.ParseDuration(tc.duration)
		require.NoError(t, err)
		require.Equal(t, strconv.Itoa(int(duration.Seconds())), parsed.Query().Get("X-Amz-Expires"))
	}
}

func TestSettingsLimiterEffectiveComparison(t *testing.T) {
	for _, mode := range []rateLimiterMode{localMode, tokenBucketMode} {
		cfg := settingsConfig()
		cfg.ConnectAttributes = map[string]string{"rate_limiter": string(mode), "rate_limiter_table": "test-table"}
		s, err := resolveSettings(cfg, log.NewNoopLogger())
		require.NoError(t, err)
		for _, tc := range []struct {
			key       string
			change    func(*settings)
			different bool
		}{
			{"mode", func(s *settings) { s.Limiter.Mode = "changed" }, true},
			{"region", func(s *settings) { s.Region = "eu-west-2" }, mode != localMode},
			{"endpoint", func(s *settings) { s.ClusterEndpoint = "other.example.invalid" }, mode != localMode},
			{"table", func(s *settings) { s.Limiter.Table = "other-table" }, mode != localMode},
			{"rate", func(s *settings) { s.Limiter.ConnectionRate++ }, true},
			{"burst", func(s *settings) { s.Limiter.ConnectionBurst++ }, true},
			{"bucket rate", func(s *settings) { s.Limiter.TokenBucketRate++ }, mode == tokenBucketMode},
			{"bucket capacity", func(s *settings) { s.Limiter.TokenBucketCapacity++ }, mode == tokenBucketMode},
			{"bucket wait", func(s *settings) { s.Limiter.TokenBucketMaxWait++ }, mode == tokenBucketMode},
		} {
			t.Run(string(mode)+"/"+tc.key, func(t *testing.T) {
				changed := s
				tc.change(&changed)
				require.Equal(t, tc.different, s.effectiveLimiterSettings() != changed.effectiveLimiterSettings())
			})
		}
	}
}

// Ensure a config-driven local limiter still respects cancellation.
func TestSettingsLimiterCanceled(t *testing.T) {
	s, err := resolveSettings(settingsConfig(), log.NewNoopLogger())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, newConnectionRateLimiter(s).Wait(ctx), context.Canceled)
}

// Inspect the caller's config while each default database is being tried.
type settingsTestResolver func(string) []string

func (r settingsTestResolver) Resolve(address string) []string { return r(address) }

func TestSettingsPoolConfigOwnership(t *testing.T) {
	cfg := settingsConfig()
	cfg.MaxConns = -1 // Stop before dialing while still exercising DSN construction.
	cfg.ConnectAttributes = map[string]string{"connection_rate_limit": "3"}
	before := *cfg
	before.ConnectAttributes = maps.Clone(cfg.ConnectAttributes)
	logger := log.NewNoopLogger()
	cache := NewTokenCache(&mockCredentialsProvider{}, logger, nil)
	t.Cleanup(cache.Stop)
	p := &plugin{tokenCache: cache, credentialsInitialized: true}
	var attempts int
	r := settingsTestResolver(func(address string) []string {
		attempts++
		require.Equal(t, before, *cfg)
		return []string{address}
	})
	s, err := resolveSettings(cfg, logger)
	require.NoError(t, err)
	_, err = p.connectWithSettings(cfg, s, r, logger, nil)
	require.ErrorContains(t, err, "invalid DSQL maxConns")
	require.Equal(t, len(defaultDatabaseNames), attempts)
	require.Equal(t, before, *cfg)
	core, logs := observer.New(zap.WarnLevel)
	later := *cfg
	later.DatabaseName = "postgres"
	later.ConnectAttributes = map[string]string{"connection_rate_limit": "5"}
	s, err = resolveSettings(&later, logger)
	require.NoError(t, err)
	_, err = p.connectWithSettings(&later, s, r, log.NewZapLogger(zap.New(core)), nil)
	require.ErrorContains(t, err, "invalid DSQL maxConns")
	require.Equal(t, 1, logs.Len())
	require.Contains(t, logs.All()[0].ContextMap()["settings_in_use"], "connection_rate_limit=3")
}
