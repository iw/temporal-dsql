package adaptivereplay

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
)

func TestMeasuresASeries(t *testing.T) {
	s := &Series{}
	for i, value := range []float64{10, 20, 20, 25, 10, 15} {
		reason := adaptive.Reason("up")
		if i == 4 {
			reason = "down"
		}
		s.Steps = append(s.Steps, Step{Static: 10, Recorded: 10, Replayed: value, RecordedReason: "held", ReplayedReason: reason})
	}

	m := s.Replayed()
	require.Equal(t, 6, m.Steps)
	require.Equal(t, 4, m.Changes)
	require.Equal(t, 2, m.Reversals, "up, down, up")
	require.InDelta(t, 1, m.Min, 1e-9)
	require.InDelta(t, 2.5, m.Max, 1e-9)
	require.InDelta(t, 1.5, m.Final, 1e-9)
	require.Equal(t, map[adaptive.Reason]int{"up": 5, "down": 1}, m.Reasons)
	require.Equal(t, "up 5, down 1", m.reasons())

	recorded := s.Recorded()
	require.Zero(t, recorded.Changes)
	require.Zero(t, recorded.Reversals)
	require.InDelta(t, 1, recorded.Final, 1e-9)
}

func TestMeasuresWithoutAStaticValue(t *testing.T) {
	s := &Series{Steps: []Step{{Static: 0, Replayed: 3}, {Static: 0, Replayed: 4}}}
	m := s.Replayed()
	require.Equal(t, 1, m.Changes)
	require.True(t, math.IsNaN(m.Min))
	require.True(t, math.IsNaN(m.Max))
	require.True(t, math.IsNaN(m.Final))
	require.Equal(t, "-", share(m.Final))
}

func TestMatchesProposalsButForRounding(t *testing.T) {
	require.True(t, Step{Recorded: 0.1 + 0.2, Replayed: 0.3}.Matches())
	require.True(t, Step{Recorded: 1e12, Replayed: 1e12 + 1e-4}.Matches())
	require.False(t, Step{Recorded: 1, Replayed: 1.001}.Matches())
	require.False(t, Step{Recorded: 1, Replayed: 1, RecordedReason: "grow", ReplayedReason: "steady"}.Matches())
}
