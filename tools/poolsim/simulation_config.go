package main

import (
	"errors"
	"fmt"
	"time"
)

// SimulationConfig configures the cluster, limiter and run assertions.
type SimulationConfig struct {
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
	Replicas      int             `json:"replicas" yaml:"replicas"`
	InflightLimit int             `json:"inflightLimit" yaml:"inflightLimit"`
	PGX           PGXConfig       `json:"pgxpool" yaml:"pgxpool"`
	Ephemeral     EphemeralConfig `json:"ephemeral" yaml:"ephemeral"`
}

type PGXConfig struct {
	MaxConns              int           `json:"maxConns" yaml:"maxConns"`
	MinConns              int           `json:"minConns" yaml:"minConns"`
	MaxConnLifetime       time.Duration `json:"maxConnLifetime" yaml:"maxConnLifetime"`
	MaxConnLifetimeJitter time.Duration `json:"maxConnLifetimeJitter" yaml:"maxConnLifetimeJitter"`
	MaxConnIdleTime       time.Duration `json:"maxConnIdleTime" yaml:"maxConnIdleTime"`
	HealthCheckPeriod     time.Duration `json:"healthCheckPeriod" yaml:"healthCheckPeriod"`
}

type EphemeralConfig struct {
	Pools       int           `json:"pools" yaml:"pools"`
	Connections int           `json:"connections" yaml:"connections"`
	Duration    time.Duration `json:"duration" yaml:"duration"`
}

func simulationDefaults(c *Config) {
	if c.Limiter == "" {
		c.Limiter = "tokenBucket"
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
	if s.InflightLimit == 0 {
		s.InflightLimit = 8
	}
	if s.PGX.MaxConns == 0 {
		s.PGX.MaxConns = 100
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

func validateSimulation(original *Config) error {
	c := *original
	c.Services = append([]ServiceConfig(nil), original.Services...)
	simulationDefaults(&c)
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
	if s.Replicas < 1 || s.InflightLimit < 1 {
		return fmt.Errorf("invalid service %q", s.Name)
	}
	if s.StartDelay < 0 {
		return fmt.Errorf("invalid startup timing for %s", s.Name)
	}
	if err := validatePGX(s.PGX); err != nil {
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

func validatePGX(p PGXConfig) error {
	if p.MinConns < 0 || p.MinConns > p.MaxConns || p.MaxConns < 1 {
		return errors.New("invalid pgxpool connection bounds")
	}
	if p.MaxConnLifetime <= 0 || p.MaxConnLifetimeJitter < 0 || p.MaxConnIdleTime < 0 || p.HealthCheckPeriod <= 0 {
		return errors.New("invalid pgxpool timing")
	}
	return nil
}
