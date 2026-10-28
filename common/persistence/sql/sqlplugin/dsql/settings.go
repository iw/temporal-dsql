package dsql

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type rateLimiterMode string

const (
	localMode       rateLimiterMode = "local"
	tokenBucketMode rateLimiterMode = "dynamodb_token_bucket"
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
}

// settingAttributes is shared by resolution and DSN filtering.
var settingAttributes = []string{
	"region", "cluster_endpoint", "token_duration",
	"connection_rate_limit", "connection_burst_limit", "rate_limiter",
	"rate_limiter_table", "token_bucket_rate", "token_bucket_capacity", "token_bucket_max_wait",
}

// resolveSettings reads datastore attributes and applies defaults.
func resolveSettings(cfg *config.SQL, logger log.Logger) (settings, error) {
	var s settings
	attrs, err := normalizeSettingAttributes(cfg.ConnectAttributes)
	if err != nil {
		return s, err
	}
	sources := make(map[string]string)
	valueFor := func(attribute, fallback string) string {
		if value, set := attrs[attribute]; set {
			sources[attribute] = attribute
			return value
		}
		return fallback
	}
	s.ClusterEndpoint = valueFor("cluster_endpoint", endpointHost(cfg.ConnectAddr))
	s.Region = valueFor("region", endpointRegion(s.ClusterEndpoint))
	if s.Region == "" {
		return s, fmt.Errorf("invalid region=%q: set connectAttributes.region, or use a DSQL endpoint host (<cluster-id>.dsql.<region>.on.aws)", s.Region)
	}
	mode, err := parseRateLimiterMode(attrs)
	if err != nil {
		return s, err
	}
	s.Limiter.Mode = mode
	s.Limiter.Table = valueFor("rate_limiter_table", "")
	if s.Limiter.Mode != localMode && strings.TrimSpace(s.Limiter.Table) == "" {
		source := sources["rate_limiter_table"]
		if source == "" {
			source = "rate_limiter_table"
		}
		return s, fmt.Errorf("invalid %s=%q: rate_limiter=%q requires rate_limiter_table", source, s.Limiter.Table, s.Limiter.Mode)
	}
	for _, key := range []struct {
		attribute string
		fallback  int
		target    *int
	}{
		{"connection_rate_limit", DefaultConnectionRateLimit, &s.Limiter.ConnectionRate},
		{"connection_burst_limit", DefaultConnectionBurstLimit, &s.Limiter.ConnectionBurst},
		{"token_bucket_rate", DefaultTokenBucketRate, &s.Limiter.TokenBucketRate},
		{"token_bucket_capacity", DefaultTokenBucketCapacity, &s.Limiter.TokenBucketCapacity},
	} {
		value := valueFor(key.attribute, strconv.Itoa(key.fallback))
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return s, fmt.Errorf("invalid %s=%q: must be a positive integer", key.attribute, value)
		}
		*key.target = parsed
	}
	for _, key := range []struct {
		attribute string
		fallback  time.Duration
		target    *time.Duration
	}{
		{"token_duration", DefaultTokenDuration, &s.TokenDuration},
		{"token_bucket_max_wait", DefaultTokenBucketMaxWait, &s.Limiter.TokenBucketMaxWait},
	} {
		value := valueFor(key.attribute, key.fallback.String())
		parsed, err := parseSettingDuration(key.attribute, value)
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
		if strings.TrimSpace(key) == setting {
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
	return s
}

func (s settings) limiterDescription() string {
	return fmt.Sprintf("rate_limiter=%s region=%q cluster_endpoint=%q rate_limiter_table=%q connection_rate_limit=%d connection_burst_limit=%d token_bucket_rate=%d token_bucket_capacity=%d token_bucket_max_wait=%s",
		s.Limiter.Mode, s.Region, s.ClusterEndpoint, s.Limiter.Table, s.Limiter.ConnectionRate, s.Limiter.ConnectionBurst,
		s.Limiter.TokenBucketRate, s.Limiter.TokenBucketCapacity, s.Limiter.TokenBucketMaxWait)
}

func parseRateLimiterMode(attrs map[string]string) (rateLimiterMode, error) {
	mode := localMode
	if value, explicit := attrs["rate_limiter"]; explicit {
		mode = rateLimiterMode(value)
	}
	switch mode {
	case localMode, tokenBucketMode:
	default:
		return "", fmt.Errorf("invalid rate_limiter=%q: must be local or dynamodb_token_bucket", mode)
	}
	return mode, nil
}

func parseSettingDuration(attribute, value string) (time.Duration, error) {
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("invalid %s=%q: must be a positive duration", attribute, value)
	}
	if attribute == "token_duration" && (parsed < MinTokenDuration || parsed > DefaultTokenDuration) {
		return 0, fmt.Errorf("invalid %s=%q: must be between %s and %s", attribute, value, MinTokenDuration, DefaultTokenDuration)
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
	} {
		if source := sources[key.attribute]; source != "" && !key.used {
			logger.Warn("DSQL setting is unused by the selected rate_limiter", tag.NewStringTag("setting", source), tag.NewStringTag("rate_limiter", string(s.Limiter.Mode)))
		}
	}
}
