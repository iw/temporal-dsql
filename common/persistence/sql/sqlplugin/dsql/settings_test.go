package dsql

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"os"
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

func clearSettingsEnv(t *testing.T) {
	t.Helper()
	for _, key := range settingAttributes {
		t.Setenv(key.env, "")
		require.NoError(t, os.Unsetenv(key.env))
	}
	t.Setenv("AWS_REGION", "")
	require.NoError(t, os.Unsetenv("AWS_REGION"))
}

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

func TestResolveSettingsPrecedence(t *testing.T) {
	for _, tc := range []struct {
		attribute, variable, fallback, envValue, attrValue string
		get                                                func(settings) string
	}{
		{"token_duration", TokenDurationEnvVar, "15m0s", "12m", "10m", func(s settings) string { return s.TokenDuration.String() }},
		{"connection_rate_limit", ConnectionRateLimitEnvVar, "10", "11", "12", func(s settings) string { return strconv.Itoa(s.Limiter.ConnectionRate) }},
		{"connection_burst_limit", ConnectionBurstLimitEnvVar, "100", "110", "120", func(s settings) string { return strconv.Itoa(s.Limiter.ConnectionBurst) }},
		{"rate_limiter_table", DistributedRateLimiterTableEnvVar, "", "env-table", "attr-table", func(s settings) string { return s.Limiter.Table }},
		{"token_bucket_rate", TokenBucketRateEnvVar, "100", "90", "80", func(s settings) string { return strconv.Itoa(s.Limiter.TokenBucketRate) }},
		{"token_bucket_capacity", TokenBucketCapacityEnvVar, "1000", "900", "800", func(s settings) string { return strconv.Itoa(s.Limiter.TokenBucketCapacity) }},
		{"token_bucket_max_wait", TokenBucketMaxWaitEnvVar, "30s", "20s", "10s", func(s settings) string { return s.Limiter.TokenBucketMaxWait.String() }},
		{"counter_limit", DistributedRateLimiterLimitEnvVar, "100", "90", "80", func(s settings) string { return strconv.Itoa(s.Limiter.CounterLimit) }},
		{"counter_max_wait", DistributedRateLimiterMaxWaitEnvVar, "30s", "20s", "10s", func(s settings) string { return s.Limiter.CounterMaxWait.String() }},
	} {
		t.Run(tc.attribute, func(t *testing.T) {
			clearSettingsEnv(t)
			cfg := settingsConfig()
			for _, stage := range []string{"default", "environment", "attribute"} {
				t.Run(stage, func(t *testing.T) {
					want := tc.fallback
					if stage != "default" {
						t.Setenv(tc.variable, tc.envValue)
						want = tc.envValue
					}
					if stage == "attribute" {
						// An invalid lower-precedence value must not be parsed.
						t.Setenv(tc.variable, "invalid")
						cfg.ConnectAttributes = map[string]string{tc.attribute: tc.attrValue}
						want = tc.attrValue
					}
					if strings.HasSuffix(tc.attribute, "duration") || strings.HasSuffix(tc.attribute, "wait") {
						d, err := time.ParseDuration(want)
						require.NoError(t, err)
						want = d.String()
					}
					s, err := resolveSettings(cfg, log.NewNoopLogger())
					require.NoError(t, err)
					require.Equal(t, want, tc.get(s))
				})
			}
		})
	}
}

func TestResolveSettingsEndpointAndRegion(t *testing.T) {
	for _, tc := range []struct {
		name, address, attrEndpoint, envEndpoint, attrRegion, envRegion, awsRegion, wantEndpoint, wantRegion string
	}{
		{"host default", "first.dsql.eu-west-1.on.aws:5432", "", "", "", "", "", "first.dsql.eu-west-1.on.aws", "eu-west-1"},
		{"endpoint env", "first.dsql.eu-west-1.on.aws:5432", "", "second.dsql.us-west-2.on.aws", "", "", "", "second.dsql.us-west-2.on.aws", "us-west-2"},
		{"endpoint attribute", "first.dsql.eu-west-1.on.aws:5432", "third.dsql.eu-west-2.on.aws", "second.dsql.us-west-2.on.aws", "", "", "", "third.dsql.eu-west-2.on.aws", "eu-west-2"},
		{"region attribute", "first.dsql.eu-west-1.on.aws:5432", "", "", "us-east-2", "eu-west-2", "us-east-1", "first.dsql.eu-west-1.on.aws", "us-east-2"},
		{"region host before env", "first.dsql.eu-west-1.on.aws:5432", "", "", "", "eu-west-2", "us-east-1", "first.dsql.eu-west-1.on.aws", "eu-west-1"},
		{"region env", "db.example.invalid:5432", "", "", "", "eu-west-2", "us-east-1", "db.example.invalid", "eu-west-2"},
		{"region SDK env", "db.example.invalid", "", "", "", "", "us-east-1", "db.example.invalid", "us-east-1"},
		{"IPv6 host", "[::1]:5432", "", "", "", "", "us-east-1", "::1", "us-east-1"},
		{"no suffix match", "first.dsql.eu-west-1.on.aws.example:5432", "", "", "", "", "us-east-1", "first.dsql.eu-west-1.on.aws.example", "us-east-1"},
		{"case and trailing dot", "FIRST.DSQL.EU-WEST-1.ON.AWS.:5432", "", "", "", "", "", "FIRST.DSQL.EU-WEST-1.ON.AWS.", "eu-west-1"},
		{"missing cluster", ".dsql.eu-west-1.on.aws:5432", "", "", "", "", "us-east-1", ".dsql.eu-west-1.on.aws", "us-east-1"},
		{"custom signing endpoint", "first.dsql.eu-west-1.on.aws:5432", "proxy.example.invalid", "", "", "", "us-east-1", "proxy.example.invalid", "us-east-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearSettingsEnv(t)
			t.Setenv("REGION", tc.envRegion)
			t.Setenv("AWS_REGION", tc.awsRegion)
			t.Setenv("CLUSTER_ENDPOINT", tc.envEndpoint)
			attrs := map[string]string{}
			if tc.attrEndpoint != "" {
				attrs["cluster_endpoint"] = tc.attrEndpoint
			}
			if tc.attrRegion != "" {
				attrs["region"] = tc.attrRegion
			}
			s, err := resolveSettings(&config.SQL{ConnectAddr: tc.address, ConnectAttributes: attrs}, log.NewNoopLogger())
			require.NoError(t, err)
			require.Equal(t, tc.wantEndpoint, s.ClusterEndpoint)
			require.Equal(t, tc.wantRegion, s.Region)
		})
	}
}

func TestResolveSettingsModes(t *testing.T) {
	for _, tc := range []struct {
		attribute, distributed, bucket string
		want                           rateLimiterMode
	}{
		{"", "", "", localMode}, {"", "false", "true", localMode},
		{"", "true", "", counterMode}, {"", "true", "false", counterMode}, {"", "1", "0", counterMode},
		{"", "true", "true", tokenBucketMode}, {"", "1", "1", tokenBucketMode},
		{"local", "true", "true", localMode},
		{"dynamodb_counter", "false", "true", counterMode},
		{"dynamodb_token_bucket", "invalid", "invalid", tokenBucketMode},
	} {
		t.Run(fmt.Sprintf("%s/%s/%s", tc.attribute, tc.distributed, tc.bucket), func(t *testing.T) {
			clearSettingsEnv(t)
			t.Setenv(DistributedRateLimiterEnabledEnvVar, tc.distributed)
			t.Setenv(TokenBucketEnabledEnvVar, tc.bucket)
			cfg := settingsConfig()
			cfg.ConnectAttributes = map[string]string{"rate_limiter_table": "test-table"}
			if tc.attribute != "" {
				cfg.ConnectAttributes["rate_limiter"] = tc.attribute
			}
			s, err := resolveSettings(cfg, log.NewNoopLogger())
			require.NoError(t, err)
			require.Equal(t, tc.want, s.Limiter.Mode)
		})
	}
}

func TestResolveSettingsValidation(t *testing.T) {
	for _, key := range settingAttributes {
		var invalid []string
		switch key.attribute {
		case "connection_rate_limit", "connection_burst_limit", "token_bucket_rate", "token_bucket_capacity", "counter_limit":
			invalid = []string{"invalid", "0", "-1", strings.Repeat("9", 30)}
		case "token_duration":
			invalid = []string{"invalid", "0s", "-1s", "14s", "15m1s"}
		case "token_bucket_max_wait", "counter_max_wait":
			invalid = []string{"invalid", "0s", "-1s", strings.Repeat("9", 24) + "h"}
		default:
			continue
		}
		for _, value := range invalid {
			for _, fromEnv := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/env=%t", key.attribute, value, fromEnv), func(t *testing.T) {
					clearSettingsEnv(t)
					cfg := settingsConfig()
					source := key.attribute
					if fromEnv {
						source = key.env
						t.Setenv(key.env, value)
					} else {
						cfg.ConnectAttributes = map[string]string{key.attribute: value}
					}
					_, err := resolveSettings(cfg, log.NewNoopLogger())
					require.ErrorContains(t, err, fmt.Sprintf("%s=%q", source, value))
				})
			}
		}
	}
	for _, tc := range []struct {
		name          string
		attrs, env    map[string]string
		address, want string
	}{
		{"missing region", nil, nil, "db.example.invalid:5432", "set connectAttributes.region"},
		{"empty region", map[string]string{"region": ""}, nil, "test.dsql.us-east-1.on.aws:5432", `region=""`},
		{"invalid mode", map[string]string{"rate_limiter": "typo"}, nil, "", `rate_limiter="typo"`},
		{"empty mode", map[string]string{"rate_limiter": ""}, nil, "", `rate_limiter=""`},
		{"invalid distributed boolean", nil, map[string]string{DistributedRateLimiterEnabledEnvVar: "typo"}, "", `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED="typo"`},
		{"invalid bucket boolean", nil, map[string]string{TokenBucketEnabledEnvVar: "typo"}, "", `DSQL_TOKEN_BUCKET_ENABLED="typo"`},
		{"missing table", map[string]string{"rate_limiter": "dynamodb_counter"}, nil, "", `rate_limiter_table=""`},
		{"empty table override", map[string]string{"rate_limiter": "dynamodb_token_bucket", "rate_limiter_table": ""}, map[string]string{DistributedRateLimiterTableEnvVar: "ignored-table"}, "", `rate_limiter_table=""`},
		{"blank legacy table", nil, map[string]string{DistributedRateLimiterEnabledEnvVar: "true", DistributedRateLimiterTableEnvVar: " "}, "", `DSQL_DISTRIBUTED_RATE_LIMITER_TABLE=" "`},
		{"empty integer", map[string]string{"connection_rate_limit": ""}, nil, "", `connection_rate_limit=""`},
		{"empty duration", map[string]string{"token_duration": ""}, nil, "", `token_duration=""`},
		{"duplicate attribute", map[string]string{"region": "us-east-1", " region ": "us-west-2"}, nil, "", "duplicate connectAttribute region="},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearSettingsEnv(t)
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			cfg := settingsConfig()
			cfg.ConnectAttributes = tc.attrs
			if tc.address != "" {
				cfg.ConnectAddr = tc.address
			}
			_, err := resolveSettings(cfg, log.NewNoopLogger())
			require.ErrorContains(t, err, tc.want)
		})
	}
	for _, value := range []string{"15s", "15m"} {
		t.Run("token boundary "+value, func(t *testing.T) {
			clearSettingsEnv(t)
			cfg := settingsConfig()
			cfg.ConnectAttributes = map[string]string{"token_duration": value}
			_, err := resolveSettings(cfg, log.NewNoopLogger())
			require.NoError(t, err)
		})
	}
}

func TestResolveSettingsUnusedWarning(t *testing.T) {
	for _, tc := range []struct {
		mode       rateLimiterMode
		key, value string
		used       bool
	}{
		{localMode, "token_bucket_rate", "50", false},
		{counterMode, "token_bucket_capacity", "500", false},
		{counterMode, "token_bucket_max_wait", "10s", false},
		{tokenBucketMode, "counter_limit", "50", false},
		{localMode, "counter_max_wait", "10s", false},
		{localMode, "rate_limiter_table", "test-table", false},
		{tokenBucketMode, "token_bucket_rate", "50", true},
		{counterMode, "counter_limit", "50", true},
		{counterMode, "connection_rate_limit", "5", true},
		{tokenBucketMode, "connection_burst_limit", "5", true},
	} {
		for _, fromEnv := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%s/env=%t", tc.mode, tc.key, fromEnv), func(t *testing.T) {
				clearSettingsEnv(t)
				cfg := settingsConfig()
				cfg.ConnectAttributes = map[string]string{"rate_limiter": string(tc.mode)}
				if tc.mode != localMode {
					cfg.ConnectAttributes["rate_limiter_table"] = "test-table"
				}
				source := tc.key
				if fromEnv {
					for _, key := range settingAttributes {
						if key.attribute == tc.key {
							source = key.env
						}
					}
					t.Setenv(source, tc.value)
				} else {
					cfg.ConnectAttributes[tc.key] = tc.value
				}
				core, logs := observer.New(zap.WarnLevel)
				_, err := resolveSettings(cfg, log.NewZapLogger(zap.New(core)))
				require.NoError(t, err)
				warnings := logs.FilterMessage("DSQL setting is unused by the selected rate_limiter").All()
				if tc.used {
					require.Empty(t, warnings)
				} else {
					require.Len(t, warnings, 1)
					require.Equal(t, source, warnings[0].ContextMap()["setting"])
					require.Equal(t, string(tc.mode), warnings[0].ContextMap()["rate_limiter"])
				}
			})
		}
	}
}

func TestResolveSettingsDeprecationWarning(t *testing.T) {
	for _, names := range [][]string{
		nil, {"AWS_REGION"}, {"REGION", "CLUSTER_ENDPOINT", TokenDurationEnvVar},
		{ConnectionRateLimitEnvVar, ConnectionBurstLimitEnvVar, DistributedRateLimiterEnabledEnvVar,
			TokenBucketEnabledEnvVar, DistributedRateLimiterTableEnvVar, TokenBucketRateEnvVar,
			TokenBucketCapacityEnvVar, TokenBucketMaxWaitEnvVar, DistributedRateLimiterLimitEnvVar, DistributedRateLimiterMaxWaitEnvVar},
	} {
		t.Run(strings.Join(names, "/"), func(t *testing.T) {
			clearSettingsEnv(t)
			deprecatedSettingsWarning = sync.Once{}
			t.Cleanup(func() { deprecatedSettingsWarning = sync.Once{} })
			values := map[string]string{
				"REGION": "us-west-2", "CLUSTER_ENDPOINT": "legacy.dsql.us-west-2.on.aws",
				TokenDurationEnvVar: "1m", DistributedRateLimiterEnabledEnvVar: "true",
				TokenBucketEnabledEnvVar: "true", DistributedRateLimiterTableEnvVar: "test-table",
				ConnectionRateLimitEnvVar: "5", ConnectionBurstLimitEnvVar: "10",
				TokenBucketRateEnvVar: "50", TokenBucketCapacityEnvVar: "500", TokenBucketMaxWaitEnvVar: "10s",
				// Empty variables are also listed, preserving their default values.
			}
			for _, name := range names {
				t.Setenv(name, values[name])
			}
			core, logs := observer.New(zap.WarnLevel)
			cfg := settingsConfig()
			// Even variables overridden by attributes are included, but not AWS_REGION.
			cfg.ConnectAttributes = map[string]string{"region": "us-east-1", "token_duration": "15s"}
			for range 3 {
				_, err := resolveSettings(cfg, log.NewZapLogger(zap.New(core)))
				require.NoError(t, err)
			}
			var want []string
			for _, key := range settingAttributes {
				for _, name := range names {
					if key.env == name {
						want = append(want, key.env+" -> "+key.attribute)
					}
				}
			}
			if len(want) == 0 {
				require.Zero(t, logs.Len())
			} else {
				require.Equal(t, 1, logs.Len())
				require.Contains(t, logs.All()[0].Message, "future release")
				require.Equal(t, strings.Join(want, ", "), logs.All()[0].ContextMap()["deprecated_settings"])
			}
		})
	}
}

func TestSettingsDSNFiltering(t *testing.T) {
	cfg := settingsConfig()
	cfg.DatabaseName, cfg.User, cfg.Password = "postgres", "test-user", "ignored"
	cfg.ConnectAttributes = map[string]string{"sslmode": "verify-full", "application_name": "test-app", "search_path": "public"}
	for _, key := range settingAttributes {
		cfg.ConnectAttributes[" "+key.attribute+" "] = "not-a-server-parameter"
	}
	before := *cfg
	before.ConnectAttributes = maps.Clone(cfg.ConnectAttributes)
	pool, err := (&plugin{}).poolConfig(cfg, resolver.NewNoopResolver(), nil, log.NewNoopLogger())
	require.NoError(t, err)
	dsn, err := url.Parse(pool.ConnConfig.ConnString())
	require.NoError(t, err)
	for _, key := range settingAttributes {
		require.NotContains(t, dsn.Query(), key.attribute)
		require.NotContains(t, pool.ConnConfig.RuntimeParams, key.attribute)
	}
	require.Equal(t, "verify-full", dsn.Query().Get("sslmode"))
	require.Equal(t, "test-app", pool.ConnConfig.RuntimeParams["application_name"])
	require.Equal(t, "public", pool.ConnConfig.RuntimeParams["search_path"])
	require.Equal(t, before, *cfg)
}

func TestSettingsCreateDBValidation(t *testing.T) {
	for _, attrs := range []map[string]string{
		{"rate_limiter": "dynamodb_token_bucket"}, {"rate_limiter": "dynamodb_counter"},
		{"connection_rate_limit": "typo"}, {"token_duration": "1h"},
	} {
		t.Run(fmt.Sprint(attrs), func(t *testing.T) {
			clearSettingsEnv(t)
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
	t.Run("legacy missing table", func(t *testing.T) {
		clearSettingsEnv(t)
		t.Setenv(DistributedRateLimiterEnabledEnvVar, "true")
		_, err := (&plugin{}).CreateDB(sqlplugin.DbKindMain, settingsConfig(), nil, log.NewNoopLogger(), metrics.NoopMetricsHandler)
		require.ErrorContains(t, err, `rate_limiter_table=""`)
	})
}

func TestSettingsLimiterFirstPoolWins(t *testing.T) {
	clearSettingsEnv(t)
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
	clearSettingsEnv(t)
	cfg := settingsConfig()
	cfg.ConnectAttributes = map[string]string{
		"region": "eu-west-2", "rate_limiter": "dynamodb_token_bucket", "rate_limiter_table": "test-table",
		"connection_rate_limit": "3", "connection_burst_limit": "4", "token_bucket_rate": "50",
		"token_bucket_capacity": "500", "token_bucket_max_wait": "12s", "counter_limit": "25", "counter_max_wait": "8s",
	}
	s, err := resolveSettings(cfg, log.NewNoopLogger())
	require.NoError(t, err)
	bucket := newTokenBucketLimiter(nil, s)
	require.Equal(t, int64(50), bucket.Rate)
	require.Equal(t, int64(500), bucket.Capacity)
	require.Equal(t, 12*time.Second, bucket.MaxWait)
	require.Equal(t, "test-table", bucket.tableName)
	require.Equal(t, s.ClusterEndpoint, bucket.endpoint)
	counter := newDistributedRateLimiter(nil, s)
	require.Equal(t, int64(25), counter.LimitPerSecond)
	require.Equal(t, 8*time.Second, counter.MaxWait)
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
	clearSettingsEnv(t)
	t.Setenv("AWS_MAX_ATTEMPTS", "invalid")
	cfg := settingsConfig()
	cfg.ConnectAttributes = map[string]string{"rate_limiter": "dynamodb_counter", "rate_limiter_table": "test-table", "connection_burst_limit": "7"}
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
	s.Limiter.CounterLimit++
	p.initRateLimiter(s, log.NewZapLogger(zap.New(core)), nil)
	require.Equal(t, 2, logs.Len())
	require.Contains(t, logs.All()[1].ContextMap()["settings_in_use"], "rate_limiter=local")
}

func TestSettingsTokenProviderUsesDatastore(t *testing.T) {
	clearSettingsEnv(t)
	cache := NewTokenCache(&mockCredentialsProvider{}, log.NewNoopLogger(), nil)
	t.Cleanup(cache.Stop)
	for _, tc := range []struct{ endpoint, region, duration string }{
		{"first.dsql.us-east-1.on.aws", "us-east-1", "1m"},
		{"second.dsql.eu-west-2.on.aws", "eu-west-2", "2m"},
	} {
		cfg := &config.SQL{User: "test-role", ConnectAddr: tc.endpoint + ":5432", ConnectAttributes: map[string]string{"token_duration": tc.duration}}
		s, err := resolveSettings(cfg, log.NewNoopLogger())
		require.NoError(t, err)
		// Changing legacy inputs after resolution cannot change this pool's tokens.
		t.Setenv(TokenDurationEnvVar, "invalid")
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
	clearSettingsEnv(t)
	for _, mode := range []rateLimiterMode{localMode, counterMode, tokenBucketMode} {
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
			{"counter limit", func(s *settings) { s.Limiter.CounterLimit++ }, mode == counterMode},
			{"counter wait", func(s *settings) { s.Limiter.CounterMaxWait++ }, mode == counterMode},
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
	clearSettingsEnv(t)
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
	clearSettingsEnv(t)
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
