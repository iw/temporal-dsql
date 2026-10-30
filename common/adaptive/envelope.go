package adaptive

import "math"

// Number is the type of a controller's output.
type Number interface {
	~int | ~int32 | ~int64 | ~float64
}

// Envelope is a controller's dynamic config: how it runs, which hosts apply it, and the
// bounds its output stays in.
type Envelope[T Number] struct {
	// Mode is "disabled", "shadow" or "enabled", as ParseMode reads it.
	Mode string
	// RolloutPercent is the share of hosts, picked by HostKey, that apply an "enabled"
	// Mode; the others run it in shadow.
	RolloutPercent int
	Min, Max       T
	// MaxStepFrac is the largest change in one step, as a share of the current output.
	// An integer output can always move by one. Zero leaves the step unlimited.
	MaxStepFrac float64
}

// NewEnvelope returns a disabled envelope over [lo, hi] that applies to every host once
// it's enabled.
func NewEnvelope[T Number](lo, hi T, maxStepFrac float64) Envelope[T] {
	return Envelope[T]{
		Mode:           ModeNameDisabled,
		RolloutPercent: 100,
		Min:            lo,
		Max:            hi,
		MaxStepFrac:    maxStepFrac,
	}
}

// HostMode is the mode the envelope gives a host.
func (e Envelope[T]) HostMode(hostKey []byte) Mode {
	return HostMode(e.Mode, e.RolloutPercent, hostKey)
}

// Clamp keeps a proposed output within MaxStepFrac of current, then within [Min, Max].
// If the bounds cross, Max wins.
func (e Envelope[T]) Clamp(current, proposed T) T {
	next := float64(proposed)
	if e.MaxStepFrac > 0 {
		step := e.MaxStepFrac * math.Abs(float64(current))
		if isInteger[T]() {
			step = max(1, math.Floor(step))
		}
		next = min(max(next, float64(current)-step), float64(current)+step)
	}
	return T(min(max(next, float64(e.Min)), float64(e.Max)))
}

func isInteger[T Number]() bool {
	half := 0.5
	return T(half) == 0
}
