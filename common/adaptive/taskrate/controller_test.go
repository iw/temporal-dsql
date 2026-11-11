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

// Until a step with MinSamples calls has measured a mix, the rate holds.
func TestTaskRateHoldsUntilItHasAMix(t *testing.T) {
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

// A step with fewer than MinSamples calls moves the mix by its calls' share of MinSamples,
// so at a low rate the rate keeps moving with what the steps measure.
func TestTaskRateMovesTheMixPartWayOnFewCalls(t *testing.T) {
	c := taskrate.New(9000)
	rate, reason := c.Step(params, taskrate.Signals{PersistenceRate: 1000, Attempts: 200, Calls: 1000})
	require.InDelta(t, 180, rate, 1e-9, "1000 x 0.9 x 0.2")
	require.Equal(t, taskrate.ReasonCoupled, reason)

	// 50 calls measure 0.4, and move the mix half way, to 0.3.
	rate, reason = c.Step(params, taskrate.Signals{PersistenceRate: 1000, Attempts: 20, Calls: 50})
	require.InDelta(t, 270, rate, 1e-9)
	require.Equal(t, taskrate.ReasonPartial, reason)

	// 25 calls move it a quarter of the way, to 0.325, and the rate follows the persistence rate.
	rate, reason = c.Step(params, taskrate.Signals{PersistenceRate: 2000, Attempts: 10, Calls: 25})
	require.InDelta(t, 585, rate, 1e-9)
	require.Equal(t, taskrate.ReasonPartial, reason)
}

// The deadband applies to the mix a step with few calls moves to, so their noise mostly
// leaves the mix as it was.
func TestTaskRateHoldsAPartialMixWithinTheDeadband(t *testing.T) {
	p := params
	p.Deadband = 0.1
	c := taskrate.New(9000)
	rate, _ := c.Step(p, taskrate.Signals{PersistenceRate: 1000, Attempts: 300, Calls: 1000})
	require.InDelta(t, 270, rate, 1e-9, "1000 x 0.9 x 0.3")

	// 20 calls measure 0.4, which moves the mix only to 0.32, within a tenth of 0.3.
	rate, reason := c.Step(p, taskrate.Signals{PersistenceRate: 1000, Attempts: 8, Calls: 20})
	require.InDelta(t, 270, rate, 1e-9)
	require.Equal(t, taskrate.ReasonHeld, reason)

	// 50 calls measure 0.4 too, and move it to 0.35.
	rate, reason = c.Step(p, taskrate.Signals{PersistenceRate: 1000, Attempts: 20, Calls: 50})
	require.InDelta(t, 315, rate, 1e-9)
	require.Equal(t, taskrate.ReasonPartial, reason)
}

// A step with calls but no task attempts keeps the mix, and the rate still follows the
// persistence rate. A step without calls, or without a persistence rate, holds the rate,
// and a reset forgets the mix.
func TestTaskRateFollowsThePersistenceRateWithItsLastMix(t *testing.T) {
	c := taskrate.New(9000)
	rate, _ := c.Step(params, taskrate.Signals{PersistenceRate: 1000, Attempts: 200, Calls: 1000})
	require.InDelta(t, 180, rate, 1e-9, "1000 x 0.9 x 0.2")

	rate, reason := c.Step(params, taskrate.Signals{PersistenceRate: 2000, Attempts: 0, Calls: 40})
	require.InDelta(t, 360, rate, 1e-9, "no task attempts")
	require.Equal(t, taskrate.ReasonLastMix, reason)

	rate, reason = c.Step(params, taskrate.Signals{PersistenceRate: 3000, Attempts: 0, Calls: 0})
	require.InDelta(t, 360, rate, 1e-9, "no calls")
	require.Equal(t, taskrate.ReasonNoData, reason)
	rate, reason = c.Step(params, taskrate.Signals{PersistenceRate: 0, Attempts: 5, Calls: 50})
	require.InDelta(t, 360, rate, 1e-9, "no persistence rate")
	require.Equal(t, taskrate.ReasonNoData, reason)

	rate, reason = c.Step(params, taskrate.Signals{PersistenceRate: 1500, Attempts: 0, Calls: 30})
	require.InDelta(t, 270, rate, 1e-9, "the mix outlasts the steps that held")
	require.Equal(t, taskrate.ReasonLastMix, reason)

	c.Reset(720)
	rate, reason = c.Step(params, taskrate.Signals{PersistenceRate: 1000, Attempts: 10, Calls: 50})
	require.InDelta(t, 720, rate, 1e-9, "a reset forgets the mix")
	require.Equal(t, taskrate.ReasonNoData, reason)
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
