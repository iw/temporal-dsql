package fairshare

import (
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/dynamicconfig"
)

// Name is the controller's name in its decisions and their metric.
const Name = "fair_share"

// Settings is the controller's dynamic config.
type Settings struct {
	// Mode is "disabled", "shadow" or "enabled". RolloutPercent is the share of namespaces,
	// picked by name, that apply an enabled Mode; the others run it in shadow.
	Mode           string
	RolloutPercent int
	// Interval is how often a frontend sends its demand to the others, and Window how long
	// a span its demand is measured over. A frontend counts another's report for StaleAfter,
	// and waits Timeout for another's reply.
	Interval   time.Duration
	Window     time.Duration
	StaleAfter time.Duration
	Timeout    time.Duration
	MinShare   float64
	Headroom   float64
	// Deadband is how far, as a share of an equal share, a namespace's share must move
	// before a frontend applies it.
	Deadband float64
}

// DefaultSettings leaves the controller off.
var DefaultSettings = Settings{
	Mode:           adaptive.ModeNameDisabled,
	RolloutPercent: 100,
	Interval:       5 * time.Second,
	Window:         30 * time.Second,
	StaleAfter:     15 * time.Second,
	Timeout:        2 * time.Second,
	MinShare:       0.2,
	Headroom:       0.2,
	Deadband:       0.1,
}

// Config is the controller's dynamic config setting.
var Config = dynamicconfig.NewGlobalTypedSetting(
	"frontend.fairShareController",
	DefaultSettings,
	`frontend.fairShareController splits each namespace's limits across the cluster, such as
frontend.globalNamespaceRPS, between frontends by their demand, in place of an equal share each.
Every Interval, each frontend sends the others the tokens a second each namespace asked of its
namespace rate limits over the last Window, and takes a share of each limit: it keeps MinShare of
an equal share, and the rest follows its part of the demand, with Headroom to spare while the
demand fits the limit. A frontend whose report is older than StaleAfter keeps an equal share. A
share applies once it moves Deadband of an equal share. Mode is disabled, shadow or enabled.`,
)

// Params are the law's parameters from the settings.
func (s Settings) Params() Params {
	return Params{MinShare: s.MinShare, Headroom: s.Headroom}
}
