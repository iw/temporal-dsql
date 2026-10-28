package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
)

func main() {
	configPath := flag.String("config", "", "Path to config file (YAML or JSON)")
	outPath := flag.String("out", "", "Path to output CSV file")
	cpuprofile := flag.String("cpuprofile", "", "Write CPU profile to file")
	flag.Parse()

	// CPU profiling
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = f.Close() }()
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

	if err := runSimulation(cfg, *outPath); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
}
