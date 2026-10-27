package main

import (
	"errors"
	"fmt"
	"time"
)

// ComparisonConfig opts into physical connections; absent fields preserve the legacy engine.
type ComparisonConfig struct {
	Model                    string         `json:"model" yaml:"model"`
	Cluster                  ClusterConfig  `json:"cluster" yaml:"cluster"`
	Limiter                  string         `json:"limiter" yaml:"limiter"`
	LimiterLatency           LatencyRange   `json:"limiterLatency" yaml:"limiterLatency"`
	Adaptive                 AdaptiveConfig `json:"adaptive" yaml:"adaptive"`
	SteadyAfter              time.Duration  `json:"steadyAfter" yaml:"steadyAfter"`
	AssertZeroDSQLRejections bool           `json:"assertZeroDSQLRejections" yaml:"assertZeroDSQLRejections"`
	AssertZeroDeadRequests   bool           `json:"assertZeroDeadRequests" yaml:"assertZeroDeadRequests"`
}

type LatencyRange struct {
	Min time.Duration `json:"min" yaml:"min"`
	Max time.Duration `json:"max" yaml:"max"`
}

type ClusterConfig struct {
	MaxConnections int          `json:"maxConnections" yaml:"maxConnections"`
	Handshake      LatencyRange `json:"handshake" yaml:"handshake"`
}

type AdaptiveConfig struct {
	InitialRate float64 `json:"initialRate" yaml:"initialRate"`
	Increase    float64 `json:"increase" yaml:"increase"`
	Capacity    float64 `json:"capacity" yaml:"capacity"`
}

type PoolConfig struct {
	Model                string          `json:"model" yaml:"model"`
	Replicas             int             `json:"replicas" yaml:"replicas"`
	MaxOpen              int             `json:"maxOpen" yaml:"maxOpen"`
	InflightLimit        int             `json:"inflightLimit" yaml:"inflightLimit"`
	EnforceLifetimeInUse bool            `json:"enforceLifetimeInUse" yaml:"enforceLifetimeInUse"`
	ConcurrentRefill     bool            `json:"concurrentRefill" yaml:"concurrentRefill"`
	LeakOnClose          *bool           `json:"leakOnClose" yaml:"leakOnClose"`
	ReconnectOnConnError *bool           `json:"reconnectOnConnError" yaml:"reconnectOnConnError"`
	PGX                  PGXConfig       `json:"pgxpool" yaml:"pgxpool"`
	Ephemeral            EphemeralConfig `json:"ephemeral" yaml:"ephemeral"`
}

type PGXConfig struct {
	MaxConns              int           `json:"maxConns" yaml:"maxConns"`
	MinConns              int           `json:"minConns" yaml:"minConns"`
	MaxConnLifetime       time.Duration `json:"maxConnLifetime" yaml:"maxConnLifetime"`
	MaxConnLifetimeJitter time.Duration `json:"maxConnLifetimeJitter" yaml:"maxConnLifetimeJitter"`
	MaxConnIdleTime       time.Duration `json:"maxConnIdleTime" yaml:"maxConnIdleTime"`
	HealthCheckPeriod     time.Duration `json:"healthCheckPeriod" yaml:"healthCheckPeriod"`
	// AcquireGuard enables the proposed BeforeAcquire guard-window check; nil means true.
	AcquireGuard *bool `json:"acquireGuard" yaml:"acquireGuard"`
}

type EphemeralConfig struct {
	Pools       int           `json:"pools" yaml:"pools"`
	Connections int           `json:"connections" yaml:"connections"`
	Duration    time.Duration `json:"duration" yaml:"duration"`
}

func usesComparison(c *Config) bool {
	if c.Model != "" || c.Limiter != "" || c.Cluster != (ClusterConfig{}) || c.Scenario.Kind == "fleet_start" || c.Scenario.Kind == "rolling_deploy" || c.Scenario.Kind == "ddb_outage" {
		return true
	}
	for _, s := range c.Services {
		if s.PoolConfig != (PoolConfig{}) {
			return true
		}
	}
	return false
}

func comparisonDefaults(c *Config) {
	if c.Model == "" {
		c.Model = "reservoir"
	}
	if c.Limiter == "" {
		c.Limiter = "perSecond"
		if c.Scenario.Kind == "ddb_outage" {
			c.Limiter = "tokenBucket"
		}
	}
	if c.Cluster.MaxConnections == 0 {
		c.Cluster.MaxConnections = 10000
	}
	if c.Cluster.Handshake == (LatencyRange{}) {
		c.Cluster.Handshake = LatencyRange{50 * time.Millisecond, 200 * time.Millisecond}
	}
	if c.LimiterLatency == (LatencyRange{}) {
		c.LimiterLatency = LatencyRange{5 * time.Millisecond, 10 * time.Millisecond}
	}
	if c.Adaptive.InitialRate == 0 {
		c.Adaptive.InitialRate = 10
	}
	if c.Adaptive.Increase == 0 {
		c.Adaptive.Increase = 0.1
	}
	if c.Adaptive.Capacity == 0 {
		c.Adaptive.Capacity = 10
	}
	if c.SteadyAfter == 0 {
		c.SteadyAfter = c.Duration / 2
	}
	if c.Scenario.EventAt == 0 {
		c.Scenario.EventAt = 5 * time.Minute
	}
	for i := range c.Services {
		poolDefaults(&c.Services[i])
	}
}

func poolDefaults(s *ServiceConfig) {
	if s.Replicas == 0 {
		s.Replicas = 1
	}
	if s.MaxOpen == 0 {
		s.MaxOpen = s.TargetReady
	}
	if s.InflightLimit == 0 {
		s.InflightLimit = 8
	}
	if s.PGX.MaxConns == 0 {
		s.PGX.MaxConns = s.MaxOpen
	}
	if s.PGX.MaxConnLifetime == 0 {
		s.PGX.MaxConnLifetime = 50 * time.Minute
	}
	if s.PGX.HealthCheckPeriod == 0 {
		s.PGX.HealthCheckPeriod = time.Minute
	}
	if s.Ephemeral.Connections == 0 {
		s.Ephemeral.Connections = 3
	}
	if s.Ephemeral.Duration == 0 {
		s.Ephemeral.Duration = time.Second
	}
}

func validModel(model string) bool {
	switch model {
	case "", "reservoir", "reservoir-sql", "as-deployed", "option-a", "pgxpool":
		return true
	default:
		return false
	}
}

func validateComparison(original *Config) error {
	c := *original
	c.Services = append([]ServiceConfig(nil), original.Services...)
	comparisonDefaults(&c)
	if !validModel(c.Model) {
		return fmt.Errorf("unknown model %q", c.Model)
	}
	switch c.Limiter {
	case "perSecond", "tokenBucket", "adaptive":
	default:
		return fmt.Errorf("unknown limiter %q", c.Limiter)
	}
	if c.Duration <= 0 || c.SampleEvery <= 0 || c.SteadyAfter < 0 || c.SteadyAfter >= c.Duration {
		return errors.New("duration/sampleEvery must be positive; steadyAfter must be inside the run")
	}
	if c.Cluster.MaxConnections < 1 || c.Cluster.Handshake.Min < 0 || c.Cluster.Handshake.Max <= 0 || c.Cluster.Handshake.Max < c.Cluster.Handshake.Min {
		return errors.New("invalid cluster cap or handshake latency")
	}
	if c.LimiterLatency.Min < 0 || c.LimiterLatency.Max < c.LimiterLatency.Min || c.Adaptive.InitialRate <= 0 || c.Adaptive.Increase <= 0 || c.Adaptive.Capacity < 1 {
		return errors.New("invalid limiter latency or adaptive configuration")
	}
	if c.Scenario.StartWindow < 0 || c.Scenario.EventAt < 0 || c.Scenario.RollEvery < 0 {
		return errors.New("scenario times cannot be negative")
	}
	names := make(map[string]bool)
	for _, s := range c.Services {
		if names[s.Name] {
			return fmt.Errorf("duplicate service %q", s.Name)
		}
		latestStart := s.StartDelay
		if c.Scenario.Kind == "fleet_start" {
			latestStart += c.Scenario.StartWindow
		}
		if latestStart > c.Duration {
			return fmt.Errorf("service %s may start after simulation end", s.Name)
		}
		names[s.Name] = true
		if err := validatePool(s); err != nil {
			return err
		}
	}
	return nil
}

func validatePool(s ServiceConfig) error {
	if !validModel(s.Model) || s.Replicas < 1 || s.MaxOpen < 1 || s.InflightLimit < 1 {
		return fmt.Errorf("invalid service %q", s.Name)
	}
	if s.StartDelay < 0 || s.Jitter < 0 || s.GuardWindow < 0 {
		return fmt.Errorf("invalid reservoir timing for %s", s.Name)
	}
	if err := validatePGX(s.PGX, s.GuardWindow); err != nil {
		return fmt.Errorf("%s: %w", s.Name, err)
	}
	if s.Ephemeral.Pools < 0 || s.Ephemeral.Connections < 1 || s.Ephemeral.Duration <= 0 {
		return fmt.Errorf("invalid ephemeral settings for %s", s.Name)
	}
	w := s.Workload
	if err := w.validatePhases(); err != nil {
		return fmt.Errorf("%s: %w", s.Name, err)
	}
	if w.Enabled && (w.MaxInUse < 1 || w.SpawnEvery <= 0 || w.ArrivalBurst < 1 || w.HoldTime <= 0 || w.HoldJitter < 0 || w.MaxWait <= 0) {
		return fmt.Errorf("invalid workload for %s", s.Name)
	}
	return nil
}

func validatePGX(p PGXConfig, guard time.Duration) error {
	if p.MinConns < 0 || p.MinConns > p.MaxConns || p.MaxConns < 1 {
		return errors.New("invalid pgxpool connection bounds")
	}
	if p.MaxConnLifetime <= guard || p.MaxConnLifetimeJitter < 0 || p.MaxConnIdleTime < 0 || p.HealthCheckPeriod <= 0 {
		return errors.New("invalid pgxpool timing")
	}
	return nil
}

func boolDefault(p *bool, fallback bool) bool {
	if p == nil {
		return fallback
	}
	return *p
}
