package signals

import (
	"slices"
	"sync"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
)

type (
	// Controller computes a setting's value from the plane's snapshots, for Drive.
	Controller[T adaptive.Number] interface {
		// Name names the controller in its decisions.
		Name() string
		// Envelope is the controller's dynamic config, for the setting's static value.
		Envelope(static T) adaptive.Envelope[T]
		// Reset sets the controller's output, so that it starts there when it runs again.
		Reset(value T)
		// Step proposes the next value from a snapshot, within the envelope, with the reason
		// and the detail a trace records.
		Step(snap *Snapshot, env adaptive.Envelope[T], static T) (T, adaptive.Reason, any)
	}

	// Driven is a subscribable setting that a Controller drives, on the plane's ticks,
	// while its envelope's mode enables it. While the controller is off, the setting's
	// subscribers get the static setting's value, as without the controller.
	Driven[T adaptive.Number] struct {
		plane   *Plane
		ctrl    Controller[T]
		frozen  func() bool
		hostKey []byte
		metrics metrics.Handler
		cancels []func()

		mu          sync.Mutex // guards the fields below, and orders notifications
		static      T
		proposed    T
		applied     T
		mode        adaptive.Mode
		subscribers map[int]func(T)
		nextID      int
	}
)

// Drive drives a setting from ctrl. Its static value is the controller's fallback, and
// its envelope's bounds and step limit hold whatever the controller proposes.
func Drive[T adaptive.Number](
	plane *Plane,
	static dynamicconfig.TypedSubscribable[T],
	ctrl Controller[T],
	frozen func() bool,
	hostKey []byte,
	handler metrics.Handler,
) *Driven[T] {
	d := &Driven[T]{
		plane:       plane,
		ctrl:        ctrl,
		frozen:      frozen,
		hostKey:     hostKey,
		metrics:     handler,
		subscribers: make(map[int]func(T)),
	}
	value, cancelStatic := static(d.staticChanged)
	d.static, d.proposed, d.applied = value, value, value
	ctrl.Reset(value)
	if cancelStatic != nil {
		d.cancels = append(d.cancels, cancelStatic)
	}
	d.cancels = append(d.cancels, plane.Subscribe(d.step, d.active))
	return d
}

// Subscribable is the setting as its consumer subscribes to it.
func (d *Driven[T]) Subscribable() dynamicconfig.TypedSubscribable[T] {
	return func(callback func(T)) (T, func()) {
		d.mu.Lock()
		defer d.mu.Unlock()
		id := d.nextID
		d.nextID++
		d.subscribers[id] = callback
		return d.applied, func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			delete(d.subscribers, id)
		}
	}
}

// Stop ends the setting's subscriptions to the static setting and the plane.
func (d *Driven[T]) Stop() {
	for _, cancel := range d.cancels {
		cancel()
	}
}

func (d *Driven[T]) staticChanged(value T) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.static = value
	switch d.mode {
	case adaptive.ModeOff:
		d.ctrl.Reset(value)
		d.proposed = value
		d.apply(value)
	case adaptive.ModeShadow:
		d.apply(value)
	default:
		// The next step holds the controller within the new value's bounds.
	}
}

// active keeps the plane ticking while the controller runs, and until the setting is
// back at its static value.
func (d *Driven[T]) active() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ctrl.Envelope(d.static).HostMode(d.hostKey) != adaptive.ModeOff || d.applied != d.static
}

func (d *Driven[T]) step(snap *Snapshot) {
	d.mu.Lock()
	defer d.mu.Unlock()

	env := d.ctrl.Envelope(d.static)
	mode := env.HostMode(d.hostKey)
	d.mode = mode
	if mode == adaptive.ModeOff {
		d.ctrl.Reset(d.static)
		d.proposed = d.static
		d.apply(d.static)
		return
	}

	decision := adaptive.Decision{Controller: d.ctrl.Name(), Time: snap.Time, Mode: mode, Static: float64(d.static)}
	if d.frozen() {
		// The freeze holds the value, but a change of mode still applies.
		decision.Reason = adaptive.ReasonFrozen
	} else {
		proposed, reason, detail := d.ctrl.Step(snap, env, d.static)
		d.proposed = env.Clamp(d.proposed, proposed)
		decision.Reason, decision.Detail = reason, detail
	}
	if mode == adaptive.ModeOn {
		d.apply(d.proposed)
	} else {
		d.apply(d.static)
	}
	decision.Proposed, decision.Applied = float64(d.proposed), float64(d.applied)
	adaptive.Report(d.metrics, d.plane, decision)
}

// apply sets the value the subscribers get, and tells them when it changes. Callers
// hold mu.
func (d *Driven[T]) apply(value T) {
	if value == d.applied {
		return
	}
	d.applied = value
	ids := make([]int, 0, len(d.subscribers))
	for id := range d.subscribers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		d.subscribers[id](value)
	}
}
