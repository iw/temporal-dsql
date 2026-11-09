package adaptivereplay

import (
	"encoding/json"
	"fmt"
	"strings"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/adaptive/fairshare"
	"go.temporal.io/server/common/adaptive/memory"
	"go.temporal.io/server/common/adaptive/partitions"
	"go.temporal.io/server/common/adaptive/taskrate"
	"go.temporal.io/server/common/adaptive/workers"
)

type (
	// law replays one series of a controller's decisions.
	law interface {
		// step replays a decision, with the overrides for its controller. It returns the
		// proposal and its reason, and whether the reason is the law's: a decision the host
		// held, as while the controller was frozen, keeps the reason it recorded.
		step(d *Decision, overrides []Override) (proposal float64, reason adaptive.Reason, stepped bool, err error)
	}

	// controllerKind is how the tool replays a controller's decisions.
	controllerKind struct {
		// name is the controller's name, or, with prefix set, how its names start.
		name   string
		prefix bool
		newLaw func() law
		// key names the series a decision belongs to, for a controller with more than one.
		key func(d *Decision) (string, error)
	}
)

// kinds are the controllers the tool replays. A name matches the first that fits it.
var kinds = []controllerKind{
	{name: capacity.Name, newLaw: func() law { return &capacityLaw{} }},
	{name: taskrate.Name, newLaw: newTaskRateLaw},
	{name: workers.NamePrefix, prefix: true, newLaw: newWorkersLaw},
	{name: memory.NamePrefix + memory.PendingTasks, newLaw: newPendingLaw},
	{name: memory.NamePrefix, prefix: true, newLaw: newCacheLaw},
	{name: partitions.Name, newLaw: func() law { return &partitionsLaw{} }, key: partitionsKey},
	{name: fairshare.Name, newLaw: func() law { return fairShareLaw{} }, key: fairShareKey},
}

func kindOf(controller string) (controllerKind, bool) {
	for _, k := range kinds {
		if controller == k.name || k.prefix && strings.HasPrefix(controller, k.name) {
			return k, true
		}
	}
	return controllerKind{}, false
}

// capacityLaw replays the persistence capacity controller. The persistence rate limiter
// starts it at the static rate, and keeps it at the static rate while it's disabled, so
// the decision after either records the rate as its detail's Start.
type capacityLaw struct {
	ctrl *capacity.Controller
}

func (l *capacityLaw) step(d *Decision, overrides []Override) (float64, adaptive.Reason, bool, error) {
	if l.ctrl == nil {
		l.ctrl = capacity.New(d.Static)
	}
	var detail capacity.Detail
	if len(d.Detail) > 0 {
		if err := decode(d, &detail, overrides, &detail.Envelope); err != nil {
			return 0, "", false, err
		}
	}
	if detail.Start > 0 {
		// The law takes a rate other than by a step as it does while the controller is off.
		l.ctrl.Step(capacity.Envelope{Mode: capacity.ModeOff}, detail.Start, capacity.Signals{})
	}
	if d.Reason == adaptive.ReasonFrozen {
		// The freeze held the rate.
		return l.ctrl.Rate(), d.Reason, false, nil
	}
	out := l.ctrl.Step(detail.Envelope, d.Static, detail.Signals)
	return out.Proposed, adaptive.Reason(out.Reason), true, nil
}

// drivenLaw replays a controller that the signal plane drives: its law's step, then the
// envelope's clamp from the value before, as signals.Driven applies them. While the
// controller is disabled, the plane resets the law to the static value on each tick, and
// reports nothing, so a tick without its decision means the law started again.
type drivenLaw[T adaptive.Number] struct {
	reset   func(static T)
	stepLaw func(d *Decision, overrides []Override, static T) (T, adaptive.Reason, adaptive.Envelope[T], error)

	proposed T
	tick     int // the tick of the last decision
	started  bool
}

func (l *drivenLaw[T]) step(d *Decision, overrides []Override) (float64, adaptive.Reason, bool, error) {
	static := T(d.Static)
	if !l.started || d.Tick > l.tick+1 {
		l.reset(static)
		l.proposed = static
		l.started = true
	}
	l.tick = d.Tick
	if len(d.Detail) == 0 {
		// The freeze held the value.
		return float64(l.proposed), d.Reason, false, nil
	}
	proposal, reason, env, err := l.stepLaw(d, overrides, static)
	if err != nil {
		return 0, "", false, err
	}
	l.proposed = env.Clamp(l.proposed, proposal)
	return float64(l.proposed), reason, true, nil
}

func newWorkersLaw() law {
	ctrl := workers.New(0)
	return &drivenLaw[int]{
		reset: ctrl.Reset,
		stepLaw: func(d *Decision, overrides []Override, static int) (int, adaptive.Reason, adaptive.Envelope[int], error) {
			var detail workers.Detail
			if err := decode(d, &detail, overrides, &detail.Params, &detail); err != nil {
				return 0, "", adaptive.Envelope[int]{}, err
			}
			if overridden(overrides, d.Controller, "GrowFrac") {
				// GrowFrac is the envelope's step limit too.
				detail.MaxStepFrac = detail.Params.GrowFrac
			}
			count, reason := ctrl.Step(detail.Params, static, detail.Signals)
			return count, adaptive.Reason(reason), adaptive.Envelope[int]{Min: detail.Params.Min, Max: detail.Params.Max, MaxStepFrac: detail.MaxStepFrac}, nil
		},
	}
}

func newTaskRateLaw() law {
	ctrl := taskrate.New(0)
	return &drivenLaw[float64]{
		reset: ctrl.Reset,
		stepLaw: func(d *Decision, overrides []Override, _ float64) (float64, adaptive.Reason, adaptive.Envelope[float64], error) {
			var detail taskrate.Detail
			if err := decode(d, &detail, overrides, &detail.Params, &detail); err != nil {
				return 0, "", adaptive.Envelope[float64]{}, err
			}
			rate, reason := ctrl.Step(detail.Params, detail.Signals)
			return rate, adaptive.Reason(reason), adaptive.Envelope[float64]{Min: detail.Params.Min, Max: detail.Params.Max, MaxStepFrac: detail.MaxStepFrac}, nil
		},
	}
}

func newCacheLaw() law {
	return newMemoryLaw[int]()
}

func newPendingLaw() law {
	return newMemoryLaw[float64]()
}

func newMemoryLaw[T memory.Value]() law {
	ctrl := memory.New(T(0))
	return &drivenLaw[T]{
		reset: ctrl.Reset,
		stepLaw: func(d *Decision, overrides []Override, _ T) (T, adaptive.Reason, adaptive.Envelope[T], error) {
			var detail memory.Detail[T]
			if err := decode(d, &detail, overrides, &detail.Params, &detail); err != nil {
				return 0, "", adaptive.Envelope[T]{}, err
			}
			size, reason := ctrl.Step(detail.Params, detail.Signals)
			return size, adaptive.Reason(reason), adaptive.Envelope[T]{Min: detail.Params.Min, Max: detail.Params.Max, MaxStepFrac: detail.MaxStepFrac}, nil
		},
	}
}

// partitionsLaw replays one task queue's partition scaler. A scaler starts its law without
// a learned capacity, each time matching loads the task queue, and starts it again from
// the one the task queue's state kept when the controller is first enabled for it.
type partitionsLaw struct {
	ctrl *partitions.Controller
}

func (l *partitionsLaw) step(d *Decision, overrides []Override) (float64, adaptive.Reason, bool, error) {
	var detail partitions.Detail
	if err := decode(d, &detail, overrides, &detail.Params); err != nil {
		return 0, "", false, err
	}
	if l.ctrl == nil || detail.Started {
		l.ctrl = partitions.New(detail.Loaded)
	}
	if detail.Held {
		return float64(detail.Signals.Partitions), d.Reason, false, nil
	}
	target, reason := l.ctrl.Step(detail.Params, detail.Signals)
	return float64(target), adaptive.Reason(reason), true, nil
}

func partitionsKey(d *Decision) (string, error) {
	var detail struct{ Namespace, TaskQueue, TaskQueueType string }
	if err := json.Unmarshal(d.Detail, &detail); err != nil {
		return "", fmt.Errorf("%s decision at %s: %w", d.Controller, d.Time, err)
	}
	return detail.Namespace + "/" + detail.TaskQueue + "/" + detail.TaskQueueType, nil
}

// fairShareLaw replays fair share's law, which keeps no state. The exchange reports a
// namespace's share when it changes, and can hold or freeze the share the law gives, or keep
// an equal share while the frontends' windows fill, so a held, frozen or filling decision
// keeps the reason it recorded.
type fairShareLaw struct{}

func (fairShareLaw) step(d *Decision, overrides []Override) (float64, adaptive.Reason, bool, error) {
	var detail fairshare.Detail
	if err := decode(d, &detail, overrides, &detail.Params); err != nil {
		return 0, "", false, err
	}
	share, reason := fairshare.Share(detail.Params, detail.Limit, detail.Demand)
	switch d.Reason {
	case adaptive.ReasonFrozen, adaptive.Reason(fairshare.ReasonHeld), adaptive.Reason(fairshare.ReasonFilling):
		return share, d.Reason, false, nil
	}
	return share, adaptive.Reason(reason), true, nil
}

func fairShareKey(d *Decision) (string, error) {
	var detail struct{ Class, Namespace string }
	if err := json.Unmarshal(d.Detail, &detail); err != nil {
		return "", fmt.Errorf("%s decision at %s: %w", d.Controller, d.Time, err)
	}
	return detail.Class + "/" + detail.Namespace, nil
}

// decode reads a decision's detail, then sets the overrides for its controller on the
// first of targets that has each field.
func decode(d *Decision, detail any, overrides []Override, targets ...any) error {
	if err := json.Unmarshal(d.Detail, detail); err != nil {
		return fmt.Errorf("%s decision at %s: %w", d.Controller, d.Time, err)
	}
	return apply(overrides, d.Controller, targets...)
}
