package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the simulation configuration.
type Config struct {
	SimulationConfig `yaml:",inline"`
	Seed             int64 `json:"seed" yaml:"seed"`

	Endpoint string `json:"endpoint" yaml:"endpoint"`

	Duration    time.Duration `json:"duration" yaml:"duration"`
	SampleEvery time.Duration `json:"sampleEvery" yaml:"sampleEvery"`

	LimiterPerSec int `json:"limiterPerSec" yaml:"limiterPerSec"`

	// Assertions
	AssertMaxConnectsPerSec int           `json:"assertMaxConnectsPerSec" yaml:"assertMaxConnectsPerSec"`
	AssertConvergeWithin    time.Duration `json:"assertConvergeWithin" yaml:"assertConvergeWithin"`
	AssertStableFor         time.Duration `json:"assertStableFor" yaml:"assertStableFor"`
	AssertMaxClosurePerSec  int           `json:"assertMaxClosurePerSec" yaml:"assertMaxClosurePerSec"`

	Services []ServiceConfig `json:"services" yaml:"services"`
	Scenario ScenarioConfig  `json:"scenario" yaml:"scenario"`
}

// ServiceConfig configures a service's pools and workload.
type ServiceConfig struct {
	PoolConfig `yaml:",inline"`
	Name       string `json:"name" yaml:"name"`

	// Startup delay (staggered service startup)
	StartDelay time.Duration `json:"startDelay" yaml:"startDelay"`

	// Workload simulation
	Workload WorkloadConfig `json:"workload" yaml:"workload"`
}

// ScenarioConfig configures the simulation scenario.
type ScenarioConfig struct {
	StartWindow time.Duration `json:"startWindow" yaml:"startWindow"`
	EventAt     time.Duration `json:"eventAt" yaml:"eventAt"`
	Kind        string        `json:"kind" yaml:"kind"` // cold_start | rolling_restart | mass_drop

	// Rolling restart
	RollEvery time.Duration `json:"rollEvery" yaml:"rollEvery"`

	// Mass drop
	DropAt       time.Duration `json:"dropAt" yaml:"dropAt"`
	DropFraction float64       `json:"dropFraction" yaml:"dropFraction"`
}

// LoadConfig loads configuration from a file.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := new(Config)
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(b, cfg); err != nil {
			return nil, fmt.Errorf("yaml unmarshal: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(b, cfg); err != nil {
			return nil, fmt.Errorf("json unmarshal: %w", err)
		}
	default:
		// Try YAML first, then JSON
		if err := yaml.Unmarshal(b, cfg); err != nil {
			if err := json.Unmarshal(b, cfg); err != nil {
				return nil, fmt.Errorf("unknown config format")
			}
		}
	}

	applyDefaults(cfg)
	return cfg, validate(cfg)
}

func applyDefaults(cfg *Config) {
	if cfg.Seed == 0 {
		cfg.Seed = 42
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = "cluster.dsql.eu-west-1.on.aws"
	}
	if cfg.Duration == 0 {
		cfg.Duration = 20 * time.Minute
	}
	if cfg.SampleEvery == 0 {
		cfg.SampleEvery = 1 * time.Second
	}
	if cfg.LimiterPerSec == 0 {
		cfg.LimiterPerSec = 100 // DSQL cluster rate limit
	}

	if cfg.AssertMaxConnectsPerSec == 0 {
		cfg.AssertMaxConnectsPerSec = cfg.LimiterPerSec
	}
	if cfg.AssertConvergeWithin == 0 {
		cfg.AssertConvergeWithin = 60 * time.Second
	}
	if cfg.AssertStableFor == 0 {
		cfg.AssertStableFor = 5 * time.Second
	}
	if cfg.AssertMaxClosurePerSec == 0 {
		cfg.AssertMaxClosurePerSec = 200
	}

	if cfg.Scenario.Kind == "" {
		cfg.Scenario.Kind = "cold_start"
	}

	for i := range cfg.Services {
		s := &cfg.Services[i]

		// Workload defaults
		if s.Workload.SpawnEvery == 0 {
			s.Workload.SpawnEvery = 200 * time.Millisecond
		}
		if s.Workload.ArrivalBurst == 0 {
			s.Workload.ArrivalBurst = 5
		}
		if s.Workload.MaxWait == 0 {
			s.Workload.MaxWait = 10 * time.Second
		}
		if s.Workload.HoldTime == 0 {
			s.Workload.HoldTime = 10 * time.Millisecond
		}
	}
	simulationDefaults(cfg)
}

func validate(cfg *Config) error {
	if len(cfg.Services) == 0 {
		return fmt.Errorf("config: services must be non-empty")
	}
	if cfg.LimiterPerSec <= 0 {
		return fmt.Errorf("config: limiterPerSec must be > 0")
	}

	for _, s := range cfg.Services {
		if s.Name == "" {
			return fmt.Errorf("config: each service needs a name")
		}

	}

	switch cfg.Scenario.Kind {
	case "cold_start":
	case "rolling_restart", "rolling_deploy":
		if cfg.Scenario.RollEvery == 0 {
			cfg.Scenario.RollEvery = 60 * time.Second
		}
	case "fleet_start", "ddb_outage":
	case "mass_drop":
		if cfg.Scenario.DropAt == 0 {
			cfg.Scenario.DropAt = 5 * time.Minute
		}
		if cfg.Scenario.DropFraction <= 0 || cfg.Scenario.DropFraction > 1 {
			cfg.Scenario.DropFraction = 0.3
		}
	default:
		return fmt.Errorf("config: unknown scenario.kind %q", cfg.Scenario.Kind)
	}

	return validateSimulation(cfg)
}

// DefaultConfig models four Temporal services with a full warm pool.
func DefaultConfig() *Config {
	cfg := &Config{Scenario: ScenarioConfig{Kind: "cold_start"}}
	for j, name := range []string{"history", "matching", "frontend", "worker"} {
		cfg.Services = append(cfg.Services, ServiceConfig{
			Name: name, StartDelay: time.Duration(j) * 100 * time.Millisecond,
			PoolConfig: PoolConfig{PGX: PGXConfig{MaxConns: 100, MinConns: 100, MaxConnLifetimeJitter: 4 * time.Minute}},
		})
	}
	cfg.AssertMaxConnectsPerSec = -1
	applyDefaults(cfg)
	return cfg
}
