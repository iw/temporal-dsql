package partitions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	params = Params{
		Min:         1,
		Max:         64,
		MaxStepFrac: 0.25,
		Capacity:    1000,
		Headroom:    0.2,
		MinRate:     50,
		BehindRatio: 0.1,
		TrialWindow: 2 * time.Minute,
		MinGain:     0.5,
		NoGainHold:  10 * time.Minute,
		DownHold:    5 * time.Minute,
	}
	t0 = time.Unix(1_700_000_000, 0)
)

// step is one step of the law on a task queue's rates.
func step(c *Controller, p Params, at time.Duration, partitions int, addRate, dispatchRate float64) (int, Reason) {
	return c.Step(p, Signals{Now: t0.Add(at), Partitions: partitions, AddRate: addRate, DispatchRate: dispatchRate})
}

func TestPartitionsFollowTheAddRate(t *testing.T) {
	c := New(0)

	// 3,600 tasks a second at 1,000 a partition, with 20% to spare, needs 5 partitions.
	target, reason := step(c, params, 0, 4, 3600, 3600)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonRate, reason)
	target, _ = step(c, params, 23*time.Second, 5, 4500, 4500)
	require.Equal(t, 6, target)

	// At 250 a partition, 6,000 tasks a second needs 29 partitions. They grow by a quarter,
	// but at least one, a step.
	c = New(250)
	target, reason = step(c, params, 0, 4, 6000, 1000)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonRate, reason)
	target, _ = step(c, params, 23*time.Second, 5, 6000, 1250)
	require.Equal(t, 6, target)
	target, _ = step(c, params, 46*time.Second, 8, 6000, 2000)
	require.Equal(t, 10, target)

	// At 1,000 tasks a second, 8 partitions would keep twice the headroom with 2, but they
	// shrink only DownHold after they last changed, by a step at a time.
	c = New(0)
	target, reason = step(c, params, 0, 8, 1000, 1000)
	require.Equal(t, 8, target)
	require.Equal(t, ReasonSteady, reason)
	target, reason = step(c, params, 5*time.Minute, 8, 1000, 1000)
	require.Equal(t, 6, target)
	require.Equal(t, ReasonRate, reason)
	target, reason = step(c, params, 5*time.Minute+23*time.Second, 6, 1000, 1000)
	require.Equal(t, 6, target)
	require.Equal(t, ReasonSteady, reason)
	target, _ = step(c, params, 10*time.Minute+23*time.Second, 6, 1000, 1000)
	require.Equal(t, 5, target)
}

func TestPartitionsWaitForTheirRatesToSettle(t *testing.T) {
	settle := params
	settle.Settle = 30 * time.Second
	c := New(0)

	// The first step holds, since the rates may still cover an earlier count.
	target, reason := step(c, settle, 0, 4, 3600, 3600)
	require.Equal(t, 4, target)
	require.Equal(t, ReasonSettle, reason)
	target, reason = step(c, settle, 30*time.Second, 4, 3600, 3600)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonRate, reason)

	// Just after the fifth partition, the four before it still count the tasks they took
	// alone, so the sum reads high. The controller neither grows the partitions nor learns
	// from it until Settle has passed.
	target, reason = step(c, settle, 35*time.Second, 5, 6000, 6000)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonSettle, reason)
	require.Zero(t, c.Learned())
	target, reason = step(c, settle, 65*time.Second, 5, 3600, 3600)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonSteady, reason)

	// Nor does it run a trial.
	c = New(0)
	step(c, settle, 0, 4, 3000, 2000)
	target, reason = step(c, settle, 23*time.Second, 4, 3000, 2000)
	require.Equal(t, 4, target)
	require.Equal(t, ReasonSettle, reason)
	target, reason = step(c, settle, 46*time.Second, 4, 3000, 2000)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)
}

func TestTrialThatRaisesDispatchMeasuresTheCapacity(t *testing.T) {
	c := New(0)

	// 4 partitions dispatch 2,000 of 3,000 tasks a second. The capacity says 4 are enough,
	// so after two steps behind the controller tries a fifth.
	target, reason := step(c, params, 0, 4, 3000, 2000)
	require.Equal(t, 4, target)
	require.Equal(t, ReasonSteady, reason)
	target, reason = step(c, params, 23*time.Second, 4, 3000, 2000)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)
	target, reason = step(c, params, 46*time.Second, 5, 3000, 2300)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)

	// Dispatch rose by more than half of a quarter, so the partitions were the limit: each
	// dispatched 500 tasks a second, and 3,000 a second needs 8 of them.
	target, reason = step(c, params, 23*time.Second+2*time.Minute, 5, 3000, 2500)
	require.Equal(t, 6, target)
	require.Equal(t, ReasonLearned, reason)
	require.InDelta(t, 500, c.Learned(), 1e-9)
	target, reason = step(c, params, 46*time.Second+2*time.Minute, 6, 3000, 2900)
	require.Equal(t, 7, target)
	require.Equal(t, ReasonRate, reason)
}

func TestTrialThatDoesNotRaiseDispatchIsUndone(t *testing.T) {
	c := New(0)

	// Workers hold the task queue to 2,000 tasks a second, however many partitions it has.
	step(c, params, 0, 4, 3000, 2000)
	target, reason := step(c, params, 23*time.Second, 4, 3000, 2000)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)
	target, reason = step(c, params, 23*time.Second+2*time.Minute, 5, 3000, 2000)
	require.Equal(t, 4, target)
	require.Equal(t, ReasonNoGain, reason)
	require.Zero(t, c.Learned())

	// No trials for NoGainHold, then one more, which fails too, so the hold doubles.
	at := 23*time.Second + 2*time.Minute
	for range 5 {
		at += time.Minute
		target, reason = step(c, params, at, 4, 3000, 2000)
		require.Equal(t, 4, target)
		require.Equal(t, ReasonNoGain, reason)
	}
	at = 23*time.Second + 12*time.Minute
	target, reason = step(c, params, at, 4, 3000, 2000)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)
	at += 2 * time.Minute
	target, reason = step(c, params, at, 5, 3000, 2000)
	require.Equal(t, 4, target)
	require.Equal(t, ReasonNoGain, reason)
	target, reason = step(c, params, at+19*time.Minute, 4, 3000, 2000)
	require.Equal(t, 4, target)
	require.Equal(t, ReasonNoGain, reason)
	target, reason = step(c, params, at+20*time.Minute, 4, 3000, 2000)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)
}

// described steps on rates the given partitions cover.
func described(c *Controller, p Params, at time.Duration, partitions, covered int, addRate, dispatchRate float64) (int, Reason) {
	return c.Step(p, Signals{Now: t0.Add(at), Partitions: partitions, Described: covered, AddRate: addRate, DispatchRate: dispatchRate})
}

func TestTrialsWaitForTheRatesToSettle(t *testing.T) {
	p := params
	p.Settle = 30 * time.Second
	p.TrialWindow = 10 * time.Second
	p.MinRate = 10

	// Workers hold the task queue to 30 tasks a second. The trial's fifth partition shows
	// from 35s, and the controller judges the trial once the rates settle after it, though
	// its window passed before.
	c := New(0)
	described(c, p, 0, 4, 4, 60, 30)
	target, reason := described(c, p, 30*time.Second, 4, 4, 60, 30)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)
	described(c, p, 35*time.Second, 5, 5, 60, 34)
	target, reason = described(c, p, 45*time.Second, 5, 5, 60, 34)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)
	target, reason = described(c, p, 65*time.Second, 5, 5, 60, 30)
	require.Equal(t, 4, target)
	require.Equal(t, ReasonNoGain, reason)

	// Once a draining partition stops being described, the rates leave out the tasks it
	// dispatched and read low, so they settle again before a trial.
	c = New(0)
	described(c, p, 0, 4, 5, 60, 30)
	target, reason = described(c, p, 35*time.Second, 4, 4, 60, 24)
	require.Equal(t, 4, target)
	require.Equal(t, ReasonSettle, reason)
	target, reason = described(c, p, 65*time.Second, 4, 4, 60, 30)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)

	// A trial starts from the highest settled dispatch rate, so a low reading when it
	// starts can't pass as a gain.
	c = New(0)
	described(c, p, 0, 4, 4, 60, 30)
	target, reason = c.Step(p, Signals{Now: t0.Add(30 * time.Second), Partitions: 4, Described: 4, AddRate: 60, DispatchRate: 30, RateLimited: true})
	require.Equal(t, 4, target)
	require.Equal(t, ReasonRateLimited, reason)
	target, reason = described(c, p, 35*time.Second, 4, 4, 60, 26)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)
	described(c, p, 40*time.Second, 5, 5, 60, 30)
	target, reason = described(c, p, 70*time.Second, 5, 5, 60, 30)
	require.Equal(t, 4, target)
	require.Equal(t, ReasonNoGain, reason)
	require.Zero(t, c.Learned())
}

func TestTrialShowsNothingWhenItsPartitionsNeverApplied(t *testing.T) {
	c := New(0)
	step(c, params, 0, 4, 3000, 2000)
	target, _ := step(c, params, 23*time.Second, 4, 3000, 2000)
	require.Equal(t, 5, target)

	// The step up never reached the task queue, so the trial is dropped, and the next step
	// tries again.
	target, reason := step(c, params, 23*time.Second+2*time.Minute, 4, 3000, 2000)
	require.Equal(t, 5, target)
	require.Equal(t, ReasonTrial, reason)
	require.Zero(t, c.Learned())
}

func TestNoTrialWhenPartitionsCannotBeWhatHoldsTheTaskQueueBack(t *testing.T) {
	behind := Signals{Partitions: 4, AddRate: 3000, DispatchRate: 2000}
	for name, tc := range map[string]struct {
		signals Signals
		params  Params
		want    Reason
	}{
		"dispatch rate limited": {signals: Signals{Partitions: 4, AddRate: 3000, DispatchRate: 2000, RateLimited: true}, params: params, want: ReasonRateLimited},
		"persistence backs off": {signals: Signals{Partitions: 4, AddRate: 3000, DispatchRate: 2000, BackingOff: true}, params: params, want: ReasonBackoff},
		"nothing dispatches":    {signals: Signals{Partitions: 4, AddRate: 3000, DispatchRate: 10}, params: params, want: ReasonNoGain},
		"at the most":           {signals: behind, params: Params{Min: 1, Max: 4, MaxStepFrac: 0.25, Capacity: 1000, Headroom: 0.2, MinRate: 50, BehindRatio: 0.1}, want: ReasonMax},
		"too few tasks":         {signals: Signals{Partitions: 4, AddRate: 40, DispatchRate: 10}, params: params, want: ReasonSteady},
	} {
		t.Run(name, func(t *testing.T) {
			c := New(0)
			for i := range 2 {
				s := tc.signals
				s.Now = t0.Add(time.Duration(i) * 23 * time.Second)
				target, reason := c.Step(tc.params, s)
				require.Equal(t, 4, target)
				if i == 1 {
					require.Equal(t, tc.want, reason)
				}
			}
		})
	}

	// Persistence backing off holds the partitions the add rate needs, too.
	c := New(0)
	target, reason := c.Step(params, Signals{Now: t0, Partitions: 4, AddRate: 6000, DispatchRate: 6000, BackingOff: true})
	require.Equal(t, 4, target)
	require.Equal(t, ReasonBackoff, reason)
}

func TestShadowProposesTrialsWithoutRunningThem(t *testing.T) {
	c := New(0)
	shadow := params
	shadow.Shadow = true
	step(c, shadow, 0, 4, 3000, 2000)
	at := 23 * time.Second
	for range 8 {
		target, reason := step(c, shadow, at, 4, 3000, 2000)
		require.Equal(t, 5, target)
		require.Equal(t, ReasonTrial, reason)
		at += 23 * time.Second
	}
	require.Zero(t, c.Learned())
}

func TestCapacityRisesWhenPartitionsDispatchMoreWithoutFallingBehind(t *testing.T) {
	c := New(0)
	target, reason := step(c, params, 0, 2, 2400, 2400)
	require.InDelta(t, 1200, c.Learned(), 1e-9)
	require.Equal(t, 3, target)
	require.Equal(t, ReasonRate, reason)

	// A learned capacity carries over, as a scaler's private state brings it back.
	c = New(1200)
	require.InDelta(t, 1200, c.Capacity(params), 1e-9)
	target, reason = step(c, params, 0, 3, 2400, 2400)
	require.Equal(t, 3, target)
	require.Equal(t, ReasonSteady, reason)
}

func TestPartitionsStayWithinTheirBounds(t *testing.T) {
	c := New(0)
	target, reason := step(c, params, 0, 70, 1000, 1000)
	require.Equal(t, 64, target)
	require.Equal(t, ReasonBounds, reason)

	// An idle task queue shrinks to Min, and no further.
	c = New(0)
	target, reason = step(c, params, 0, 2, 0, 0)
	require.Equal(t, 2, target)
	require.Equal(t, ReasonSteady, reason)
	target, reason = step(c, params, 5*time.Minute, 2, 0, 0)
	require.Equal(t, 1, target)
	require.Equal(t, ReasonRate, reason)
	target, reason = step(c, params, 10*time.Minute, 1, 0, 0)
	require.Equal(t, 1, target)
	require.Equal(t, ReasonSteady, reason)

	// The add rate can't take the partitions past Max.
	c = New(0)
	target, reason = step(c, params, 0, 64, 100000, 100000)
	require.Equal(t, 64, target)
	require.Equal(t, ReasonMax, reason)

	target, reason = step(New(0), params, 0, 0, 1000, 1000)
	require.Equal(t, 0, target)
	require.Equal(t, ReasonNoData, reason)
}
