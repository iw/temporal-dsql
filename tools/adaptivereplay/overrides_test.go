package adaptivereplay

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive/workers"
)

func TestParsesOverrides(t *testing.T) {
	o, err := ParseOverride("worker_count.TargetDelay=250ms")
	require.NoError(t, err)
	require.Equal(t, Override{Controller: "worker_count", Field: "TargetDelay", Value: "250ms"}, o)
	require.Equal(t, "worker_count.TargetDelay=250ms", o.String())

	o, err = ParseOverride("a.b.Field=1=2")
	require.NoError(t, err)
	require.Equal(t, Override{Controller: "a.b", Field: "Field", Value: "1=2"}, o, "the field follows the last dot before the first =")

	for _, s := range []string{"", "TargetDelay=1s", "worker_count.TargetDelay", ".TargetDelay=1s", "worker_count.=1s"} {
		_, err := ParseOverride(s)
		require.ErrorIs(t, err, errOverrideSyntax, s)
	}
}

func TestOverridesApplyToTheControllersTheyName(t *testing.T) {
	o := Override{Controller: "worker_count", Field: "TargetDelay", Value: "1s"}
	require.True(t, o.appliesTo("worker_count"))
	require.True(t, o.appliesTo("worker_count_transfer"))
	require.False(t, o.appliesTo("worker_counts"))
	require.False(t, o.appliesTo("memory_budget_history_cache"))
	require.True(t, Override{Controller: "worker_count_timer"}.appliesTo("worker_count_timer"))
	require.False(t, Override{Controller: "worker_count_timer"}.appliesTo("worker_count_transfer"))
}

func TestSetsParametersOfEachKind(t *testing.T) {
	detail := workers.Detail{Params: workers.Params{TargetDelay: time.Second, GrowFrac: 0.25, IdleSteps: 30}}
	err := apply([]Override{
		{Controller: "worker_count", Field: "TargetDelay", Value: "250ms"},
		{Controller: "worker_count", Field: "GrowFrac", Value: "0.5"},
		{Controller: "worker_count", Field: "IdleSteps", Value: "3"},
		{Controller: "worker_count", Field: "MaxStepFrac", Value: "0.1"},
		{Controller: "memory_budget", Field: "IdleSteps", Value: "100"},
	}, "worker_count_transfer", &detail.Params, &detail)
	require.NoError(t, err)
	require.Equal(t, 250*time.Millisecond, detail.Params.TargetDelay)
	require.InDelta(t, 0.5, detail.Params.GrowFrac, 0)
	require.Equal(t, 3, detail.Params.IdleSteps, "only the overrides for the controller")
	require.InDelta(t, 0.1, detail.MaxStepFrac, 0)

	var flags struct{ On bool }
	require.NoError(t, apply([]Override{{Controller: "c", Field: "On", Value: "true"}}, "c", &flags))
	require.True(t, flags.On)
}

func TestRejectsAnOverrideTheLawCantTake(t *testing.T) {
	var detail workers.Detail
	err := apply([]Override{{Controller: "worker_count", Field: "Speed", Value: "1"}}, "worker_count_transfer", &detail.Params, &detail)
	require.EqualError(t, err, "worker_count.Speed=1: worker_count_transfer has no parameter Speed; it has "+
		"Min, Max, TargetDelay, GrowFrac, ShrinkFrac, IdleSteps, MinSamples, Cooldown, MaxStepFrac")

	err = apply([]Override{{Controller: "worker_count", Field: "Signals", Value: "1"}}, "worker_count_transfer", &detail.Params, &detail)
	require.ErrorContains(t, err, "has no parameter Signals", "a struct isn't a parameter")

	for field, value := range map[string]string{"TargetDelay": "fast", "GrowFrac": "half", "IdleSteps": "1.5"} {
		err := apply([]Override{{Controller: "worker_count", Field: field, Value: value}}, "worker_count_transfer", &detail.Params, &detail)
		require.ErrorContains(t, err, "worker_count."+field+"="+value+": ", field)
	}
}
