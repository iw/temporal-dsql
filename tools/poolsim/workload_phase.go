package main

import (
	"fmt"
	"strings"
	"time"
)

// WorkloadPhase uses offsets from simulation start, including for replacement pools.
type WorkloadPhase struct {
	Name         string        `json:"name" yaml:"name"`
	At           time.Duration `json:"at" yaml:"at"`
	Ramp         time.Duration `json:"ramp" yaml:"ramp"`
	Duration     time.Duration `json:"duration" yaml:"duration"`
	MaxInUse     int           `json:"maxInUse" yaml:"maxInUse"`
	HoldTime     time.Duration `json:"holdTime" yaml:"holdTime"`
	HoldJitter   time.Duration `json:"holdJitter" yaml:"holdJitter"`
	SpawnEvery   time.Duration `json:"spawnEvery" yaml:"spawnEvery"`
	ArrivalBurst int           `json:"arrivalBurst" yaml:"arrivalBurst"`
	MaxWait      time.Duration `json:"maxWait" yaml:"maxWait"`
}

func (p WorkloadPhase) level(base WorkloadConfig) WorkloadConfig {
	w := base
	if p.MaxInUse != 0 {
		w.MaxInUse = p.MaxInUse
	}
	if p.HoldTime != 0 {
		w.HoldTime = p.HoldTime
	}
	if p.HoldJitter != 0 {
		w.HoldJitter = p.HoldJitter
	}
	if p.SpawnEvery != 0 {
		w.SpawnEvery = p.SpawnEvery
	}
	if p.ArrivalBurst != 0 {
		w.ArrivalBurst = p.ArrivalBurst
	}
	if p.MaxWait != 0 {
		w.MaxWait = p.MaxWait
	}
	return w
}

// at returns the current level, request tag and next absolute phase boundary.
// A zero boundary means there are no more transitions in this workload.
func (w WorkloadConfig) at(elapsed time.Duration) (WorkloadConfig, string, time.Duration) {
	for _, p := range w.Phases {
		if elapsed < p.At {
			return w, "", p.At
		}
		plateau := p.At + p.Ramp
		end := plateau + p.Duration
		if elapsed >= end {
			continue
		}
		level := p.level(w)
		if elapsed < plateau {
			fraction := float64(elapsed-p.At) / float64(p.Ramp)
			level.MaxInUse = w.MaxInUse + int(float64(level.MaxInUse-w.MaxInUse)*fraction)
			return level, p.Name, plateau
		}
		return level, p.Name, end
	}
	return w, "", 0
}

func (w WorkloadConfig) validatePhases() error {
	var previousEnd time.Duration
	for _, p := range w.Phases {
		if strings.TrimSpace(p.Name) == "" || p.At < 0 || p.Duration <= 0 || p.Ramp < 0 {
			return fmt.Errorf("phase %q requires a name, at >= 0, duration > 0 and ramp >= 0", p.Name)
		}
		end := p.At + p.Ramp + p.Duration
		if p.At < previousEnd || end <= p.At || p.At+p.Ramp < p.At {
			return fmt.Errorf("phase %q is out of order, overlaps or overflows its time window", p.Name)
		}
		level := p.level(w)
		if level.MaxInUse < 1 || level.SpawnEvery <= 0 || level.ArrivalBurst < 1 || level.HoldTime <= 0 || level.HoldJitter < 0 || level.MaxWait <= 0 {
			return fmt.Errorf("invalid workload for phase %q", p.Name)
		}
		previousEnd = end
	}
	return nil
}
