// Package adaptive is the shared core of the controllers that adjust Temporal's capacity
// settings while it runs: how a controller runs on a host, the bounds its output stays in,
// the key that freezes every controller, and the reasons its decisions carry. See
// docs/dsql/adaptive-behaviour.md, Proposal 1 and "Safety, operability and testing".
package adaptive

import (
	"os"

	"go.temporal.io/server/common/dynamicconfig"
)

// Mode is how a controller runs on a host.
type Mode int

const (
	// ModeOff applies the static dynamic config value, exactly as without the controller.
	ModeOff Mode = iota
	// ModeShadow runs the controller and reports its proposals, but applies the static value.
	ModeShadow
	// ModeOn applies the controller's output, within its envelope.
	ModeOn
)

// The names modes have in dynamic config.
const (
	ModeNameDisabled = "disabled"
	ModeNameShadow   = "shadow"
	ModeNameEnabled  = "enabled"
)

// ParseMode reads a mode by its dynamic config name. Anything but "shadow" or "enabled"
// is ModeOff.
func ParseMode(name string) Mode {
	switch name {
	case ModeNameShadow:
		return ModeShadow
	case ModeNameEnabled:
		return ModeOn
	default:
		return ModeOff
	}
}

func (m Mode) String() string {
	switch m {
	case ModeShadow:
		return ModeNameShadow
	case ModeOn:
		return ModeNameEnabled
	default:
		return ModeNameDisabled
	}
}

// HostMode is the mode a host applies. An "enabled" mode runs in shadow on hosts outside
// the cohort of rolloutPercent, which hostKey places the host in or out of. Raising the
// percentage keeps the hosts already in, so a cohort grows steadily.
func HostMode(name string, rolloutPercent int, hostKey []byte) Mode {
	mode := ParseMode(name)
	if mode == ModeOn && !dynamicconfig.RolloutAccepts(hostKey, rolloutPercent) {
		return ModeShadow
	}
	return mode
}

// HostKey is this host's key for rollouts: its hostname, so a host keeps its place in or
// out of a cohort across restarts. Processes that share a hostname are in or out together.
func HostKey() []byte {
	hostname, _ := os.Hostname()
	return []byte(hostname)
}
