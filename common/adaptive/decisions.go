package adaptive

import "go.temporal.io/server/common/metrics"

// Reason explains a controller's decision, as a metric tag and in its logs.
type Reason string

// ReasonFrozen marks a step where Frozen held a controller's output.
const ReasonFrozen Reason = "frozen"

// ControllerDecisions counts each controller's steps by reason.
var ControllerDecisions = metrics.NewCounterDef(
	"adaptive_controller_decisions",
	metrics.WithDescription("Steps of the adaptive controllers, keyed by `controller` and `reason`"),
)

// RecordDecision counts one step of a controller, with its reason.
func RecordDecision(handler metrics.Handler, controller string, reason Reason) {
	ControllerDecisions.With(handler).Record(1,
		metrics.StringTag("controller", controller), metrics.StringTag("reason", string(reason)))
}
