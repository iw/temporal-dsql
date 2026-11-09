package taskrate_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive/taskrate"
)

var params = taskrate.Params{Min: 180, Max: 9000, Headroom: 0.9, MinSamples: 100}

func TestTaskRateFollowsThePersistenceRateAndTheMix(t *testing.T) {
	c := taskrate.New(9000)

	// 1,000 calls/s admitted, and five calls per task attempt.
	rate, reason := c.Step(params, taskrate.Signals{PersistenceRate: 1000, Attempts: 200, Calls: 1000})
	require.InDelta(t, 180, rate, 1e-9)
	require.Equal(t, taskrate.ReasonCoupled, reason)

	// The persistence rate is cut, or API calls take a larger share: the tasks get less.
	rate, _ = c.Step(params, taskrate.Signals{PersistenceRate: 4000, Attempts: 200, Calls: 1000})
	require.InDelta(t, 720, rate, 1e-9)
	rate, _ = c.Step(params, taskrate.Signals{PersistenceRate: 4000, Attempts: 200, Calls: 2000})
	require.InDelta(t, 360, rate, 1e-9)
}

func TestTaskRateHoldsWithoutEnoughSignal(t *testing.T) {
	c := taskrate.New(720)
	for _, s := range []taskrate.Signals{
		{PersistenceRate: 1000, Attempts: 5, Calls: 50},  // too few calls
		{PersistenceRate: 1000, Attempts: 0, Calls: 500}, // no task attempts
		{PersistenceRate: 0, Attempts: 100, Calls: 500},  // no persistence rate
	} {
		rate, reason := c.Step(params, s)
		require.InDelta(t, 720, rate, 1e-9)
		require.Equal(t, taskrate.ReasonNoData, reason)
	}
}

func TestTaskRateStaysWithinItsBounds(t *testing.T) {
	c := taskrate.New(9000)
	rate, _ := c.Step(params, taskrate.Signals{PersistenceRate: 1000, Attempts: 10, Calls: 1000})
	require.InDelta(t, params.Min, rate, 1e-9)
	rate, _ = c.Step(params, taskrate.Signals{PersistenceRate: 90000, Attempts: 1000, Calls: 1000})
	require.InDelta(t, params.Max, rate, 1e-9)
}

func TestTaskRateHoldsTheMixWithinTheDeadband(t *testing.T) {
	p := params
	p.Deadband = 0.1
	c := taskrate.New(9000)
	step := func(persistenceRate float64, attempts int) (float64, taskrate.Reason) {
		return c.Step(p, taskrate.Signals{PersistenceRate: persistenceRate, Attempts: attempts, Calls: 1000})
	}

	rate, reason := step(2000, 500)
	require.InDelta(t, 900, rate, 1e-9, "2000 x 0.9 x 0.5")
	require.Equal(t, taskrate.ReasonCoupled, reason)

	// A mix within a tenth of 0.5 holds it.
	rate, reason = step(2000, 540)
	require.InDelta(t, 900, rate, 1e-9)
	require.Equal(t, taskrate.ReasonHeld, reason)

	// The rate still follows the persistence rate, by less than a tenth too.
	rate, reason = step(2100, 540)
	require.InDelta(t, 945, rate, 1e-9)
	require.Equal(t, taskrate.ReasonHeld, reason)
	rate, reason = step(1000, 460)
	require.InDelta(t, 450, rate, 1e-9)
	require.Equal(t, taskrate.ReasonHeld, reason)

	// A mix a tenth or more away sets it again.
	rate, reason = step(1000, 600)
	require.InDelta(t, 540, rate, 1e-9)
	require.Equal(t, taskrate.ReasonCoupled, reason)
}

func TestTaskRateResetForgetsTheMix(t *testing.T) {
	p := params
	p.Deadband = 0.1
	c := taskrate.New(9000)
	rate, _ := c.Step(p, taskrate.Signals{PersistenceRate: 4000, Attempts: 200, Calls: 1000})
	require.InDelta(t, 720, rate, 1e-9)

	// 0.21 is within a tenth of the mix the rate was set from, but a reset forgets it.
	c.Reset(9000)
	rate, reason := c.Step(p, taskrate.Signals{PersistenceRate: 4000, Attempts: 210, Calls: 1000})
	require.InDelta(t, 756, rate, 1e-9)
	require.Equal(t, taskrate.ReasonCoupled, reason)
}
