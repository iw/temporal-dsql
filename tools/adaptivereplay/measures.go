package adaptivereplay

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	"go.temporal.io/server/common/adaptive"
)

// Measures summarise a series' proposals, recorded or replayed.
type Measures struct {
	Steps int
	// Changes is how many steps changed the proposal, and Reversals how many of those
	// turned it the other way from the change before.
	Changes, Reversals int
	// Min, Max and Final are the lowest, highest and last proposals, each as a share of the
	// static value it stood in for. They're NaN without a static value above 0.
	Min, Max, Final float64
	// Reasons counts the steps by reason.
	Reasons map[adaptive.Reason]int
}

// Recorded measures the series' recorded proposals.
func (s *Series) Recorded() Measures {
	return measure(s.Steps, func(step Step) (float64, adaptive.Reason) { return step.Recorded, step.RecordedReason })
}

// Replayed measures the series' replayed proposals.
func (s *Series) Replayed() Measures {
	return measure(s.Steps, func(step Step) (float64, adaptive.Reason) { return step.Replayed, step.ReplayedReason })
}

func measure(steps []Step, proposal func(Step) (float64, adaptive.Reason)) Measures {
	m := Measures{Steps: len(steps), Min: math.NaN(), Max: math.NaN(), Final: math.NaN(), Reasons: make(map[adaptive.Reason]int)}
	var last, direction float64
	for i, step := range steps {
		value, reason := proposal(step)
		m.Reasons[reason]++
		if i > 0 && !same(value, last) {
			m.Changes++
			turn := math.Copysign(1, value-last)
			if direction != 0 && turn != direction {
				m.Reversals++
			}
			direction = turn
		}
		last = value
		share := math.NaN()
		if step.Static > 0 {
			share = value / step.Static
		}
		m.Final = share
		if math.IsNaN(share) {
			continue
		}
		if math.IsNaN(m.Min) || share < m.Min {
			m.Min = share
		}
		if math.IsNaN(m.Max) || share > m.Max {
			m.Max = share
		}
	}
	return m
}

// reasons lists the reasons, the most frequent first.
func (m Measures) reasons() string {
	reasons := slices.SortedFunc(maps.Keys(m.Reasons), func(a, b adaptive.Reason) int {
		return cmp.Or(cmp.Compare(m.Reasons[b], m.Reasons[a]), cmp.Compare(a, b))
	})
	parts := make([]string, len(reasons))
	for i, reason := range reasons {
		parts[i] = fmt.Sprintf("%s %d", reason, m.Reasons[reason])
	}
	return strings.Join(parts, ", ")
}
