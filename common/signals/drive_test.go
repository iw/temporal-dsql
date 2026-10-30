package signals

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/metrics"
)

// fakeController proposes next on every step.
type fakeController struct {
	env   adaptive.Envelope[int]
	next  int
	reset []int
}

func (f *fakeController) Name() string                        { return "fake" }
func (f *fakeController) Envelope(int) adaptive.Envelope[int] { return f.env }
func (f *fakeController) Reset(value int)                     { f.reset = append(f.reset, value) }
func (f *fakeController) Step(*Snapshot, adaptive.Envelope[int], int) (int, adaptive.Reason, any) {
	return f.next, "fake", f.next
}

// staticSetting is a subscribable dynamic config setting.
type staticSetting struct {
	value     int
	callbacks []func(int)
}

func (s *staticSetting) subscribe(callback func(int)) (int, func()) {
	s.callbacks = append(s.callbacks, callback)
	return s.value, func() {}
}

func (s *staticSetting) set(value int) {
	s.value = value
	for _, callback := range s.callbacks {
		callback(value)
	}
}

type driveFixture struct {
	*planeFixture
	static *staticSetting
	ctrl   *fakeController
	frozen bool
	driven *Driven[int]
	got    []int // values the setting's subscriber got
}

func newDriveFixture(t *testing.T, mode string) *driveFixture {
	t.Helper()
	f := &driveFixture{
		planeFixture: newPlaneFixture(t, nil),
		static:       &staticSetting{value: 100},
		ctrl: &fakeController{
			env:  adaptive.Envelope[int]{Mode: mode, RolloutPercent: 100, Min: 50, Max: 200, MaxStepFrac: 0.25},
			next: 120,
		},
	}
	f.settings.Enabled = false // the controller alone runs the plane
	f.driven = Drive(f.plane, f.static.subscribe, f.ctrl, func() bool { return f.frozen }, []byte("history-1"), metrics.NoopMetricsHandler)
	t.Cleanup(f.driven.Stop)
	initial, cancel := f.driven.Subscribable()(func(v int) { f.got = append(f.got, v) })
	t.Cleanup(cancel)
	require.Equal(t, 100, initial)
	return f
}

// lastDecision is the controller's decision on the previous tick, as the next snapshot
// carries it.
func (f *driveFixture) lastDecision() adaptive.Decision {
	return f.tick().Decisions["fake"]
}

func TestDrivenSettingPassesTheStaticValueThroughWhileOff(t *testing.T) {
	f := newDriveFixture(t, adaptive.ModeNameDisabled)
	require.Nil(t, f.tick(), "nothing runs the plane")
	f.static.set(80)
	require.Equal(t, []int{80}, f.got)
	require.Equal(t, []int{100, 80}, f.ctrl.reset)
}

func TestDrivenSettingAppliesTheControllerWhileEnabled(t *testing.T) {
	f := newDriveFixture(t, adaptive.ModeNameEnabled)
	require.NotNil(t, f.tick(), "an enabled controller runs the plane")
	require.Equal(t, []int{120}, f.got)

	// The envelope holds the step to a quarter of 120, and the bounds hold it below 200.
	f.ctrl.next = 400
	f.tick()
	require.Equal(t, []int{120, 150}, f.got)

	d := f.lastDecision()
	require.Equal(t, adaptive.ModeOn, d.Mode)
	require.Equal(t, adaptive.Reason("fake"), d.Reason)
	require.InDelta(t, 100, d.Static, 0)
	require.InDelta(t, 150, d.Proposed, 0)
	require.InDelta(t, 150, d.Applied, 0)
	require.Equal(t, 400, d.Detail)

	// A change to the static value waits for the next step while the controller is on.
	f.static.set(80)
	require.Equal(t, []int{120, 150, 187}, f.got)
}

func TestDrivenSettingKeepsTheStaticValueInShadow(t *testing.T) {
	f := newDriveFixture(t, adaptive.ModeNameShadow)
	f.tick()
	require.Empty(t, f.got)
	d := f.lastDecision()
	require.Equal(t, adaptive.ModeShadow, d.Mode)
	require.InDelta(t, 120, d.Proposed, 0)
	require.InDelta(t, 100, d.Applied, 0)
}

func TestDrivenSettingReturnsToTheStaticValueWhenDisabled(t *testing.T) {
	f := newDriveFixture(t, adaptive.ModeNameEnabled)
	f.tick()
	f.ctrl.env.Mode = adaptive.ModeNameDisabled
	require.NotNil(t, f.tick(), "the plane runs until the setting is back at its static value")
	require.Equal(t, []int{120, 100}, f.got)
	require.Nil(t, f.tick())
}

func TestDrivenSettingHoldsWhileFrozen(t *testing.T) {
	f := newDriveFixture(t, adaptive.ModeNameEnabled)
	f.tick()
	f.frozen = true
	f.ctrl.next = 140
	f.tick()
	require.Equal(t, []int{120}, f.got)
	require.Equal(t, adaptive.ReasonFrozen, f.lastDecision().Reason)

	// A change of mode still applies.
	f.ctrl.env.Mode = adaptive.ModeNameShadow
	f.tick()
	require.Equal(t, []int{120, 100}, f.got)
}
