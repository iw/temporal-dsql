package adaptive

import (
	"time"

	"go.temporal.io/server/common/metrics"
)

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

// Decision is one step of a controller, as the signal plane keeps it and its trace
// records it.
type Decision struct {
	Controller string
	Time       time.Time
	Mode       Mode
	Reason     Reason
	// Static is the value the controller stands in for. Proposed is what the controller
	// wants, and Applied what the host uses: Proposed when it's on, otherwise Static.
	Static, Proposed, Applied float64
	// Detail is the controller's own inputs and outputs, so a trace can replay the step.
	Detail any `json:",omitempty"`
}

// DecisionSink receives every controller's decisions, as the signal plane does.
type DecisionSink interface {
	RecordDecision(Decision)
}

// Report counts a decision, as RecordDecision does, and passes it to sink when there is
// one.
func Report(handler metrics.Handler, sink DecisionSink, d Decision) {
	RecordDecision(handler, d.Controller, d.Reason)
	if sink != nil {
		sink.RecordDecision(d)
	}
}
