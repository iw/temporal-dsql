package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

func main() {
	configPath := flag.String("config", "", "Path to config file (YAML or JSON)")
	outPath := flag.String("out", "", "Path to output CSV file")
	cpuprofile := flag.String("cpuprofile", "", "Write CPU profile to file")
	compare := flag.String("compare", "", "Comma-separated variants: as-deployed,option-a,pgxpool,reservoir")
	flag.Parse()

	// CPU profiling
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "Error starting CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	// Load or create config
	var cfg *Config
	var err error
	if *configPath != "" {
		cfg, err = LoadConfig(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
			os.Exit(1)
		}
	} else {
		cfg = DefaultConfig()
	}

	if *compare != "" || usesComparison(cfg) {
		if err := runComparisons(cfg, *compare, *outPath); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Seed random for reproducibility (using global source)
	_ = cfg.Seed // Seed is used by individual components with rand.NewSource

	// Create simulation
	startTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sim := NewSim(startTime, cfg.Duration)

	// Create rate limiter
	limiter := NewPerSecondLimiter(cfg.LimiterPerSec)

	// Create metrics collector
	metrics := NewMetrics()

	// Create services
	services := make(map[string]*Service)
	for _, scfg := range cfg.Services {
		svc := NewService(scfg, cfg.Endpoint, limiter, metrics, sim)
		services[scfg.Name] = svc

		// Schedule service start with delay
		startAt := sim.Now().Add(scfg.StartDelay)
		svc.Start(startAt)
	}

	// Apply scenario (rolling restart, mass drop, etc.)
	if err := ApplyScenario(cfg, sim, services); err != nil {
		fmt.Fprintf(os.Stderr, "Error applying scenario: %v\n", err)
		os.Exit(1)
	}

	// Schedule periodic sampling
	scheduleSampling(sim, cfg, services, metrics)

	// Run simulation
	fmt.Printf("Running reservoir simulation for %s...\n", cfg.Duration)
	sim.Run()

	// Write CSV if requested
	if *outPath != "" {
		if err := metrics.WriteCSV(*outPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing CSV: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("CSV: %s\n", *outPath)
	}

	// Print summary
	printSummary(cfg, services, metrics)

	// Check assertions
	if err := checkAssertions(cfg, metrics); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("OK")
}

func scheduleSampling(sim *Sim, cfg *Config, services map[string]*Service, metrics *Metrics) {
	var sample func()
	sample = func() {
		now := sim.Now()
		for _, svc := range services {
			// Record snapshot
			metrics.Snapshot(now, svc.Name, svc.Reservoir)
		}
		sim.After(cfg.SampleEvery, "sample", sample)
	}
	sim.After(cfg.SampleEvery, "sample", sample)
}

func printSummary(cfg *Config, services map[string]*Service, metrics *Metrics) {
	fmt.Println("\n=== Reservoir Simulation Summary ===")
	fmt.Printf("Duration: %s\n", cfg.Duration)
	fmt.Printf("Services: %d\n", len(cfg.Services))
	fmt.Printf("Rate limit: %d/sec\n", cfg.LimiterPerSec)
	fmt.Println()

	for _, svc := range services {
		stats := svc.Reservoir.Stats()
		fmt.Printf("Service: %s\n", svc.Name)
		fmt.Printf("  Final size: %d / %d\n", stats.Size, stats.Target)
		fmt.Printf("  Checkouts: %d\n", stats.Checkouts)
		fmt.Printf("  Empty events: %d\n", stats.EmptyEvents)
		fmt.Printf("  Refills: %d\n", stats.Refills)
		fmt.Printf("  Discards: %d (expired=%d, guard=%d, full=%d)\n",
			stats.Discards, stats.DiscardExpired, stats.DiscardGuard, stats.DiscardFull)
		fmt.Println()
	}

	maxPerSec := metrics.MaxConnectsPerSecond()
	fmt.Printf("Max connects/sec: %d (limit: %d)\n", maxPerSec, cfg.AssertMaxConnectsPerSec)
	fmt.Printf("Total empty events: %d\n", metrics.TotalEmptyEvents())
}

func checkAssertions(cfg *Config, metrics *Metrics) error {
	// Check rate limit compliance
	if err := AssertConnectRate(cfg, metrics); err != nil {
		return err
	}

	// Check convergence and stability
	if err := AssertConverge(cfg, metrics); err != nil {
		return err
	}

	// Check for empty events
	if cfg.AssertZeroEmptyEvents {
		if err := AssertZeroEmptyEvents(metrics); err != nil {
			return err
		}
	}

	return nil
}
