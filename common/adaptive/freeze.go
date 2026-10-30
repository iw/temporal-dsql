package adaptive

import "go.temporal.io/server/common/dynamicconfig"

// Frozen holds every adaptive controller at its current output, for use during an
// incident. Modes still change while it's set, so a controller can still be disabled.
var Frozen = dynamicconfig.NewGlobalBoolSetting(
	"adaptive.frozen",
	false,
	`adaptive.frozen holds every adaptive controller at its current output, for use during an incident.
Modes still change while it's set, so a controller can still be disabled.`,
)
