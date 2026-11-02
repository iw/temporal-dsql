package fairshare

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

var testParams = Params{MinShare: 0.2, Headroom: 0.2}

// shares are the shares of frontends that all know each other's demand.
func shares(p Params, limit float64, demand []float64) []float64 {
	var total float64
	for _, d := range demand {
		total += d
	}
	out := make([]float64, len(demand))
	for i, d := range demand {
		out[i], _ = Share(p, limit, Demand{Own: d, Peers: total - d, Members: len(demand), Known: len(demand) - 1})
	}
	return out
}

func TestShareIsEqualWithoutDemand(t *testing.T) {
	share, reason := Share(testParams, 100, Demand{Members: 4, Known: 3})
	require.InDelta(t, 25.0, share, 1e-9)
	require.Equal(t, ReasonNoDemand, reason)
}

func TestShareWithinTheLimitKeepsHeadroomAndSplitsWhatsSpare(t *testing.T) {
	// Floors of 10, and 80 above them. Demand of 30 with 20% headroom is 36, so 44 is spare.
	share, reason := Share(testParams, 100, Demand{Own: 30, Members: 2, Known: 1})
	require.Equal(t, ReasonWithin, reason)
	require.InDelta(t, 10+36+22.0, share, 1e-9)

	got := shares(testParams, 100, []float64{30, 0})
	require.InDeltaSlice(t, []float64{68, 32}, got, 1e-9)
}

func TestShareOverTheLimitFollowsTheDemand(t *testing.T) {
	share, reason := Share(testParams, 100, Demand{Own: 150, Peers: 10, Members: 2, Known: 1})
	require.Equal(t, ReasonOver, reason)
	require.InDelta(t, 10+80*150/160.0, share, 1e-9)

	got := shares(testParams, 100, []float64{150, 10})
	require.InDeltaSlice(t, []float64{85, 15}, got, 1e-9)
}

func TestShareLeavesAnEqualShareForEachFrontendItDoesNotKnow(t *testing.T) {
	// Four frontends, two of which know each other. The two others keep 25 each, and the two
	// split 50: floors of 5, and 40 above them in proportion to 150 and 10.
	a, _ := Share(testParams, 100, Demand{Own: 150, Peers: 10, Members: 4, Known: 1})
	b, _ := Share(testParams, 100, Demand{Own: 10, Peers: 150, Members: 4, Known: 1})
	require.InDelta(t, 5+40*150/160.0, a, 1e-9)
	require.InDelta(t, 5+40*10/160.0, b, 1e-9)
	require.InDelta(t, 50.0, a+b, 1e-9)
}

func TestShareOfASingleFrontendIsTheLimit(t *testing.T) {
	for _, own := range []float64{0, 10, 500} {
		share, _ := Share(testParams, 100, Demand{Own: own, Members: 1})
		require.InDelta(t, 100.0, share, 1e-9, "own demand %v", own)
	}
}

func TestShareKeepsMinShareWithinBounds(t *testing.T) {
	// A floor of a whole equal share leaves nothing to follow the demand.
	got := shares(Params{MinShare: 2, Headroom: 0.2}, 100, []float64{150, 10})
	require.InDeltaSlice(t, []float64{50, 50}, got, 1e-9)

	// No floor at all: a frontend without demand gets none of an overrun limit.
	got = shares(Params{MinShare: -1, Headroom: 0.2}, 100, []float64{150, 0})
	require.InDeltaSlice(t, []float64{100, 0}, got, 1e-9)
}

func TestSharesAddUpToTheLimit(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	for range 1000 {
		n := 1 + rng.IntN(8)
		limit := 1 + rng.Float64()*1000
		demand := make([]float64, n)
		for i := range demand {
			if rng.IntN(3) > 0 {
				demand[i] = rng.Float64() * limit
			}
		}
		var total float64
		for _, share := range shares(testParams, limit, demand) {
			require.GreaterOrEqual(t, share, testParams.MinShare*limit/float64(n)-1e-9)
			total += share
		}
		require.InDelta(t, limit, total, 1e-6*limit)
	}
}
