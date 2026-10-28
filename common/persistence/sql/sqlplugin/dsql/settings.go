package dsql

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type rateLimiterMode string

const (
	localMode       rateLimiterMode = "local"
	tokenBucketMode rateLimiterMode = "dynamodb_token_bucket"
	counterMode     rateLimiterMode = "dynamodb_counter"
)

type settings struct {
	Region          string
	ClusterEndpoint string
	TokenDuration   time.Duration
	Limiter         limiterSettings
}

type limiterSettings struct {
	Mode                rateLimiterMode
	Table               string
	ConnectionRate      int
	ConnectionBurst     int
	TokenBucketRate     int
	TokenBucketCapacity int
	TokenBucketMaxWait  time.Duration
	CounterLimit        int
	CounterMaxWait      time.Duration
}

// Keep this list shared by resolution, deprecation reporting and DSN filtering.
var settingAttributes = []struct{ attribute, env string }{
	{"region", "REGION"},
	{"cluster_endpoint", "CLUSTER_ENDPOINT"},
	{"token_duration", TokenDurationEnvVar},
	{"connection_rate_limit", ConnectionRateLimitEnvVar},
	{"connection_burst_limit", ConnectionBurstLimitEnvVar},
	{"rate_limiter", DistributedRateLimiterEnabledEnvVar},
	{"rate_limiter", TokenBucketEnabledEnvVar},
	{"rate_limiter_table", DistributedRateLimiterTableEnvVar},
	{"token_bucket_rate", TokenBucketRateEnvVar},
	{"token_bucket_capacity", TokenBucketCapacityEnvVar},
	{"token_bucket_max_wait", TokenBucketMaxWaitEnvVar},
	{"counter_limit", DistributedRateLimiterLimitEnvVar},
	{"counter_max_wait", DistributedRateLimiterMaxWaitEnvVar},
}

var deprecatedSettingsWarning sync.Once

// resolveSettings is the only reader of active plugin environment settings.
// Empty legacy variables retain their old meaning of "unset".
func resolveSettings(cfg *config.SQL, logger log.Logger) (settings, error) {
	var s settings
	env := make(map[string]string, len(settingAttributes)+1)
	var deprecated []string
	for _, key := range settingAttributes {
		if value, set := os.LookupEnv(key.env); set {
			env[key.env] = value
			deprecated = append(deprecated, key.env+" -> "+key.attribute)
		}
	}
	env["AWS_REGION"] = os.Getenv("AWS_REGION")
	if len(deprecated) > 0 {
		deprecatedSettingsWarning.Do(func() {
			logger.Warn("DSQL environment settings are deprecated and will be removed in a future release; use connectAttributes",
				tag.NewStringTag("deprecated_settings", strings.Join(deprecated, ", ")))
		})
	}
	attrs, err := normalizeSettingAttributes(cfg.ConnectAttributes)
	if err != nil {
		return s, err
	}
	sources := make(map[string]string)
	valueFor := func(attribute, variable, fallback string) string {
		if value, set := attrs[attribute]; set {
			sources[attribute] = attribute
			return value
		}
		if value := env[variable]; value != "" {
			sources[attribute] = variable
			return value
		}
		return fallback
	}
	s.ClusterEndpoint = valueFor("cluster_endpoint", "CLUSTER_ENDPOINT", endpointHost(cfg.ConnectAddr))
	s.Region = valueFor("region", "", endpointRegion(s.ClusterEndpoint))
	if _, explicit := attrs["region"]; !explicit && s.Region == "" {
		s.Region = valueFor("region", "REGION", env["AWS_REGION"])
	}
	if s.Region == "" {
		return s, fmt.Errorf("region=%q: set connectAttributes.region, use a DSQL endpoint (<cluster-id>.dsql.<region>.on.aws), or set AWS_REGION", s.Region)
	}
	mode, err := parseRateLimiterMode(attrs, env, logger)
	if err != nil {
		return s, err
	}
	s.Limiter.Mode = mode
	s.Limiter.Table = valueFor("rate_limiter_table", DistributedRateLimiterTableEnvVar, "")
	if s.Limiter.Mode != localMode && strings.TrimSpace(s.Limiter.Table) == "" {
		source := sources["rate_limiter_table"]
		if source == "" {
			source = "rate_limiter_table"
		}
		return s, fmt.Errorf("invalid %s=%q: rate_limiter=%q requires rate_limiter_table", source, s.Limiter.Table, s.Limiter.Mode)
	}
	for _, key := range []struct {
		attribute, env string
		fallback       int
		target         *int
	}{
		{"connection_rate_limit", ConnectionRateLimitEnvVar, DefaultConnectionRateLimit, &s.Limiter.ConnectionRate},
		{"connection_burst_limit", ConnectionBurstLimitEnvVar, DefaultConnectionBurstLimit, &s.Limiter.ConnectionBurst},
		{"token_bucket_rate", TokenBucketRateEnvVar, DefaultTokenBucketRate, &s.Limiter.TokenBucketRate},
		{"token_bucket_capacity", TokenBucketCapacityEnvVar, DefaultTokenBucketCapacity, &s.Limiter.TokenBucketCapacity},
		{"counter_limit", DistributedRateLimiterLimitEnvVar, DefaultDistributedRateLimiterLimit, &s.Limiter.CounterLimit},
	} {
		value := valueFor(key.attribute, key.env, strconv.Itoa(key.fallback))
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return s, fmt.Errorf("invalid %s=%q: must be a positive integer", sources[key.attribute], value)
		}
		*key.target = parsed
	}
	for _, key := range []struct {
		attribute, env string
		fallback       time.Duration
		target         *time.Duration
	}{
		{"token_duration", TokenDurationEnvVar, DefaultTokenDuration, &s.TokenDuration},
		{"token_bucket_max_wait", TokenBucketMaxWaitEnvVar, DefaultTokenBucketMaxWait, &s.Limiter.TokenBucketMaxWait},
		{"counter_max_wait", DistributedRateLimiterMaxWaitEnvVar, DefaultDistributedRateLimiterMaxWait, &s.Limiter.CounterMaxWait},
	} {
		value := valueFor(key.attribute, key.env, key.fallback.String())
		parsed, err := parseSettingDuration(key.attribute, sources[key.attribute], value)
		if err != nil {
			return s, err
		}
		*key.target = parsed
	}
	s.warnUnusedSettings(sources, logger)
	return s, nil
}

func endpointHost(address string) string {
	if host, _, err := net.SplitHostPort(address); err == nil {
		return host
	}
	return address
}

func endpointRegion(endpoint string) string {
	parts := strings.Split(strings.TrimSuffix(strings.ToLower(endpointHost(endpoint)), "."), ".")
	if len(parts) == 5 && parts[0] != "" && parts[1] == "dsql" && parts[2] != "" && parts[3] == "on" && parts[4] == "aws" {
		return parts[2]
	}
	return ""
}

func isPluginAttribute(key string) bool {
	for _, setting := range settingAttributes {
		if strings.TrimSpace(key) == setting.attribute {
			return true
		}
	}
	return false
}

// Only effective settings participate in the process-wide limiter comparison.
func (s settings) effectiveLimiterSettings() settings {
	s.TokenDuration = 0
	if s.Limiter.Mode == localMode {
		s.Region, s.ClusterEndpoint, s.Limiter.Table = "", "", ""
	} else {
		s.ClusterEndpoint = normalizeEndpoint(s.ClusterEndpoint)
	}
	if s.Limiter.Mode != tokenBucketMode {
		s.Limiter.TokenBucketRate, s.Limiter.TokenBucketCapacity, s.Limiter.TokenBucketMaxWait = 0, 0, 0
	}
	if s.Limiter.Mode != counterMode {
		s.Limiter.CounterLimit, s.Limiter.CounterMaxWait = 0, 0
	}
	return s
}

func (s settings) limiterDescription() string {
	return fmt.Sprintf("rate_limiter=%s region=%q cluster_endpoint=%q rate_limiter_table=%q connection_rate_limit=%d connection_burst_limit=%d token_bucket_rate=%d token_bucket_capacity=%d token_bucket_max_wait=%s counter_limit=%d counter_max_wait=%s",
		s.Limiter.Mode, s.Region, s.ClusterEndpoint, s.Limiter.Table, s.Limiter.ConnectionRate, s.Limiter.ConnectionBurst,
		s.Limiter.TokenBucketRate, s.Limiter.TokenBucketCapacity, s.Limiter.TokenBucketMaxWait, s.Limiter.CounterLimit, s.Limiter.CounterMaxWait)
}

func parseRateLimiterMode(attrs, env map[string]string, logger log.Logger) (rateLimiterMode, error) {
	mode := localMode
	if value, explicit := attrs["rate_limiter"]; explicit {
		mode = rateLimiterMode(value)
	}
	if _, explicit := attrs["rate_limiter"]; !explicit {
		enabled := make(map[string]bool, 2)
		for _, key := range []string{DistributedRateLimiterEnabledEnvVar, TokenBucketEnabledEnvVar} {
			if value := env[key]; value != "" {
				parsed, err := strconv.ParseBool(value)
				if err != nil {
					return "", fmt.Errorf("invalid %s=%q: must be a boolean", key, value)
				}
				enabled[key] = parsed
			}
		}
		if enabled[DistributedRateLimiterEnabledEnvVar] {
			mode = counterMode
			if enabled[TokenBucketEnabledEnvVar] {
				mode = tokenBucketMode
			}
		} else if env[TokenBucketEnabledEnvVar] != "" {
			logger.Warn("DSQL setting is unused by the selected rate_limiter", tag.NewStringTag("setting", TokenBucketEnabledEnvVar), tag.NewStringTag("rate_limiter", string(mode)))
		}
	}
	switch mode {
	case localMode, tokenBucketMode, counterMode:
	default:
		return "", fmt.Errorf("invalid rate_limiter=%q: must be local, dynamodb_token_bucket or dynamodb_counter", mode)
	}
	return mode, nil
}

func parseSettingDuration(attribute, source, value string) (time.Duration, error) {
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("invalid %s=%q: must be a positive duration", source, value)
	}
	if attribute == "token_duration" && (parsed < MinTokenDuration || parsed > DefaultTokenDuration) {
		return 0, fmt.Errorf("invalid %s=%q: must be between %s and %s", source, value, MinTokenDuration, DefaultTokenDuration)
	}
	return parsed, nil
}

func normalizeSettingAttributes(attributes map[string]string) (map[string]string, error) {
	attrs := make(map[string]string, len(attributes))
	for key, value := range attributes {
		key = strings.TrimSpace(key)
		if _, exists := attrs[key]; exists {
			return nil, fmt.Errorf("duplicate connectAttribute %s=%q", key, value)
		}
		attrs[key] = strings.TrimSpace(value)
	}
	return attrs, nil
}

func (s settings) warnUnusedSettings(sources map[string]string, logger log.Logger) {
	for _, key := range []struct {
		attribute string
		used      bool
	}{
		{"rate_limiter_table", s.Limiter.Mode != localMode},
		{"token_bucket_rate", s.Limiter.Mode == tokenBucketMode},
		{"token_bucket_capacity", s.Limiter.Mode == tokenBucketMode},
		{"token_bucket_max_wait", s.Limiter.Mode == tokenBucketMode},
		{"counter_limit", s.Limiter.Mode == counterMode},
		{"counter_max_wait", s.Limiter.Mode == counterMode},
	} {
		if source := sources[key.attribute]; source != "" && !key.used {
			logger.Warn("DSQL setting is unused by the selected rate_limiter", tag.NewStringTag("setting", source), tag.NewStringTag("rate_limiter", string(s.Limiter.Mode)))
		}
	}
}
