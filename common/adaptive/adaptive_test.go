package adaptive_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
)

// rolloutKeys returns a host key that RolloutAccepts places in a 50% cohort, and one it
// leaves out.
func rolloutKeys(t *testing.T) (in, out []byte) {
	t.Helper()
	for i := 0; in == nil || out == nil; i++ {
		key := fmt.Appendf(nil, "host-%d", i)
		if dynamicconfig.RolloutAccepts(key, 50) {
			if in == nil {
				in = key
			}
		} else if out == nil {
			out = key
		}
	}
	return in, out
}

func TestParseMode(t *testing.T) {
	for name, want := range map[string]adaptive.Mode{
		"disabled": adaptive.ModeOff,
		"shadow":   adaptive.ModeShadow,
		"enabled":  adaptive.ModeOn,
		"":         adaptive.ModeOff,
		"on":       adaptive.ModeOff,
	} {
		require.Equal(t, want, adaptive.ParseMode(name), name)
	}
	for _, mode := range []adaptive.Mode{adaptive.ModeOff, adaptive.ModeShadow, adaptive.ModeOn} {
		require.Equal(t, mode, adaptive.ParseMode(mode.String()))
	}
}

func TestHostModeRollsOutToACohort(t *testing.T) {
	in, out := rolloutKeys(t)
	require.Equal(t, adaptive.ModeOn, adaptive.HostMode(adaptive.ModeNameEnabled, 50, in))
	require.Equal(t, adaptive.ModeShadow, adaptive.HostMode(adaptive.ModeNameEnabled, 50, out))
	require.Equal(t, adaptive.ModeOn, adaptive.HostMode(adaptive.ModeNameEnabled, 100, out))
	require.Equal(t, adaptive.ModeShadow, adaptive.HostMode(adaptive.ModeNameEnabled, 0, in))

	// A rollout only holds back an enabled mode.
	require.Equal(t, adaptive.ModeShadow, adaptive.HostMode(adaptive.ModeNameShadow, 0, in))
	require.Equal(t, adaptive.ModeOff, adaptive.HostMode(adaptive.ModeNameDisabled, 100, in))
}

func TestEnvelopeClamp(t *testing.T) {
	rates := adaptive.NewEnvelope(10.0, 1000.0, 0.5)
	require.InDelta(t, 150.0, rates.Clamp(100, 400), 1e-9)
	require.InDelta(t, 50.0, rates.Clamp(100, 1), 1e-9)
	require.InDelta(t, 120.0, rates.Clamp(100, 120), 1e-9)
	require.InDelta(t, 1000.0, rates.Clamp(900, 2000), 1e-9)

	workers := adaptive.NewEnvelope(1, 64, 0.25)
	require.Equal(t, 5, workers.Clamp(4, 10), "a quarter of 4 is 1")
	require.Equal(t, 20, workers.Clamp(16, 40))
	require.Equal(t, 3, workers.Clamp(2, 10), "an integer output always moves by at least one")
	require.Equal(t, 1, workers.Clamp(1, 0))

	unlimited := adaptive.NewEnvelope(1, 64, 0)
	require.Equal(t, 64, unlimited.Clamp(4, 100))
}

func TestNewEnvelopeIsDisabledAndAppliesToEveryHost(t *testing.T) {
	env := adaptive.NewEnvelope(1, 10, 0.1)
	require.Equal(t, adaptive.ModeOff, env.HostMode([]byte("history-1")))
	env.Mode = adaptive.ModeNameEnabled
	require.Equal(t, adaptive.ModeOn, env.HostMode([]byte("history-1")))
}

func TestRecordDecision(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	t.Cleanup(func() { handler.StopCapture(capture) })

	adaptive.RecordDecision(handler, "persistence_capacity", adaptive.ReasonFrozen)

	recordings := capture.Snapshot()["adaptive_controller_decisions"]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(1), recordings[0].Value)
	require.Equal(t, "persistence_capacity", recordings[0].Tags["controller"])
	require.Equal(t, "frozen", recordings[0].Tags["reason"])
}

func TestFrozenIsOffByDefault(t *testing.T) {
	dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient(nil), log.NewNoopLogger())
	require.False(t, adaptive.Frozen.Get(dc)())

	dc = dynamicconfig.NewCollection(dynamicconfig.StaticClient{adaptive.Frozen.Key(): true}, log.NewNoopLogger())
	require.True(t, adaptive.Frozen.Get(dc)())
}
