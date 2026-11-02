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
