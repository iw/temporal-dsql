package capacity

import (
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

func healthy(at time.Duration, p99 time.Duration, binding bool) Signals {
	return Signals{Now: t0.Add(at), P99: p99, Samples: 1000, Binding: binding}
}

// withFacts is a signal from a store whose plugin reports facts.
func withFacts(at time.Duration, p99 time.Duration, binding bool) Signals {
	s := healthy(at, p99, binding)
	s.FactsReported = true
	return s
}

func TestThrottledFactsCutWhileLatencyIsFine(t *testing.T) {
	c := New(1000)
	env := DefaultEnvelope(1000)
	c.Step(env, 1000, withFacts(0, 10*time.Millisecond, false))
	s := withFacts(time.Second, 10*time.Millisecond, true)
	s.ThrottledRatio = 0.05
	s.Throughput = 800
	if d := c.Step(env, 1000, s); d.Reason != ReasonThrottled || d.Proposed != 720 {
		t.Fatalf("expected a cut to 0.9 x delivered for throttling, got %+v", d)
	}
}

func TestThrottledFactsAndTimeoutsShareTheErrorRatio(t *testing.T) {
	c := New(1000)
	env := DefaultEnvelope(1000)
	s := withFacts(0, 10*time.Millisecond, true)
	s.ErrorRatio, s.ThrottledRatio = 0.006, 0.006 // neither alone is over 1%
	if d := c.Step(env, 1000, s); d.Reason != ReasonThrottled || d.Proposed != 500 {
		t.Fatalf("expected throttling plus timeouts to halve the rate, got %+v", d)
	}
}

func TestQueuedFactsCut(t *testing.T) {
	c := New(1000)
	env := DefaultEnvelope(1000)
	s := withFacts(0, 10*time.Millisecond, true)
	s.QueuedRatio = 0.2
	if d := c.Step(env, 1000, s); d.Reason == ReasonQueued {
		t.Fatalf("queueing under MaxQueuedRatio must not cut, got %+v", d)
	}
	s = withFacts(time.Second, 10*time.Millisecond, true)
	s.QueuedRatio = 0.4
	if d := c.Step(env, 1000, s); d.Reason != ReasonQueued || d.Proposed >= 1000 {
		t.Fatalf("expected a cut for queueing on the plugin's pool, got %+v", d)
	}
}

func TestLatencyRunsInShadowWhenTheStoreReportsFacts(t *testing.T) {
	env := DefaultEnvelope(1000)
	c := New(1000)
	c.Step(env, 1000, withFacts(0, 10*time.Millisecond, false)) // baseline 10ms, target 20ms
	d := c.Step(env, 1000, withFacts(time.Second, 200*time.Millisecond, false))
	if d.Proposed != 1000 || d.Reason != ReasonIdle || !d.LatencyShadow {
		t.Fatalf("latency must not cut a store that reports facts, got %+v", d)
	}

	// Without facts, the same signals cut as before.
	c = New(1000)
	c.Step(env, 1000, healthy(0, 10*time.Millisecond, false))
	d = c.Step(env, 1000, healthy(time.Second, 200*time.Millisecond, false))
	if d.Reason != ReasonLatency || d.Proposed >= 1000 || d.LatencyShadow {
		t.Fatalf("expected the latency law to cut a store without facts, got %+v", d)
	}
}

func TestOffModeIsExactlyStatic(t *testing.T) {
	c := New(3000)
	env := DefaultEnvelope(3000)
	env.Mode = ModeOff
	for i := range 100 {
		d := c.Step(env, 1234, healthy(time.Duration(i)*time.Second, time.Second, true))
		if d.RPS != 1234 || d.Reason != ReasonOff {
			t.Fatalf("off mode must return static value, got %+v", d)
		}
	}
	env.Mode = ModeOn
	if d := c.Step(env, 1234, healthy(200*time.Second, 10*time.Millisecond, false)); d.RPS != 1234 {
		t.Fatalf("Off->On must be bumpless, got %v", d.RPS)
	}
}

func TestShadowNeverApplies(t *testing.T) {
	c := New(3000)
	env := DefaultEnvelope(3000)
	env.Mode = ModeShadow
	c.Step(env, 3000, healthy(0, 10*time.Millisecond, true))
	d := c.Step(env, 3000, healthy(time.Second, 500*time.Millisecond, true))
	if d.RPS != 3000 {
		t.Fatalf("shadow applied a value: %v", d.RPS)
	}
	if d.Proposed >= 3000 || d.Reason != ReasonLatency {
		t.Fatalf("shadow should still propose a decrease, got %+v", d)
	}
}

func TestDecreaseIsProportionalBoundedAndCooledDown(t *testing.T) {
	c := New(1000)
	env := DefaultEnvelope(1000)
	c.Step(env, 1000, healthy(0, 10*time.Millisecond, false)) // baseline 10ms, target 20ms

	d := c.Step(env, 1000, healthy(time.Second, 22*time.Millisecond, true)) // 10% over
	if d.Reason != ReasonLatency || d.Proposed < 900 || d.Proposed > 920 {
		t.Fatalf("expected ~9%% cut, got %+v", d)
	}
	d = c.Step(env, 1000, healthy(2*time.Second, time.Second, true))
	if d.Reason != ReasonCooldown {
		t.Fatalf("second cut inside cooldown: %+v", d)
	}
	d = c.Step(env, 1000, healthy(7*time.Second, 10*time.Second, true)) // 500x over
	if want := 0.5 * 909.09; d.Proposed < want-1 || d.Proposed > want+1 {
		t.Fatalf("decrease must be capped at MaxDecrease, got %v", d.Proposed)
	}
}

func TestNoWindupWhenNotBinding(t *testing.T) {
	c := New(1000)
	env := DefaultEnvelope(1000)
	for i := range 600 {
		d := c.Step(env, 1000, healthy(time.Duration(i)*time.Second, 10*time.Millisecond, false))
		if d.Proposed != 1000 {
			t.Fatalf("limit inflated while idle at step %d: %+v", i, d)
		}
	}
}

func TestEnvelopeClamps(t *testing.T) {
	c := New(1000)
	env := DefaultEnvelope(1000)
	for i := range 1000 {
		c.Step(env, 1000, healthy(time.Duration(i)*time.Second, 10*time.Millisecond, true))
	}
	if c.Rate() != env.MaxRPS {
		t.Fatalf("expected clamp at MaxRPS %v, got %v", env.MaxRPS, c.Rate())
	}
	for i := range 1000 {
		c.Step(env, 1000, healthy(time.Duration(2000+i*10)*time.Second, 10*time.Second, true))
	}
	if c.Rate() != env.MinRPS {
		t.Fatalf("expected clamp at MinRPS %v, got %v", env.MinRPS, c.Rate())
	}
}

func TestErrorsCutEvenWhenLatencyLooksFine(t *testing.T) {
	c := New(1000)
	env := DefaultEnvelope(1000)
	s := healthy(0, 10*time.Millisecond, true)
	s.ErrorRatio = 0.2
	if d := c.Step(env, 1000, s); d.Reason != ReasonErrors || d.Proposed != 500 {
		t.Fatalf("expected error-driven halving, got %+v", d)
	}
}

func TestBaselineForgetsAfterWindow(t *testing.T) {
	c := New(1000)
	env := DefaultEnvelope(1000)
	c.Step(env, 1000, healthy(0, 5*time.Millisecond, false))
	d := c.Step(env, 1000, healthy(time.Minute, 40*time.Millisecond, false))
	if d.Baseline != 5*time.Millisecond {
		t.Fatalf("baseline should still remember 5ms, got %v", d.Baseline)
	}
	d = c.Step(env, 1000, healthy(11*time.Minute, 40*time.Millisecond, false))
	if d.Baseline != 40*time.Millisecond {
		t.Fatalf("baseline should have moved to 40ms after window, got %v", d.Baseline)
	}
}

func TestOverloadCutsToDeliveredThroughput(t *testing.T) {
	c := New(6000)
	env := DefaultEnvelope(3000)
	c.Step(env, 3000, healthy(0, 10*time.Millisecond, false))
	s := healthy(time.Second, 200*time.Millisecond, true)
	s.Throughput = 5000 // the DB actually delivered 5000/s
	if d := c.Step(env, 3000, s); d.Proposed != 4500 {
		t.Fatalf("expected cut to 0.9 x delivered = 4500, got %+v", d)
	}
	// A collapse in delivery is still bounded by MaxDecrease.
	s = healthy(10*time.Second, 200*time.Millisecond, true)
	s.Throughput = 100
	if d := c.Step(env, 3000, s); d.Proposed != 2250 {
		t.Fatalf("expected MaxDecrease floor 2250, got %+v", d)
	}
}

func TestProbeIsCautiousNearLastOverloadThenAccelerates(t *testing.T) {
	c := New(5000)
	env := DefaultEnvelope(3000)
	c.Step(env, 3000, healthy(0, 10*time.Millisecond, false))
	s := healthy(time.Second, 200*time.Millisecond, true)
	s.Throughput = 5000
	c.Step(env, 3000, s) // cut to 4500, capacity estimate 5000

	d := c.Step(env, 3000, healthy(2*time.Second, 10*time.Millisecond, true))
	if got := d.Proposed / 4500; got > 1.006 {
		t.Fatalf("first probe near capacity estimate should be ~0.5%%, got %.4f", got)
	}
	before := c.Rate()
	d = c.Step(env, 3000, healthy(40*time.Second, 10*time.Millisecond, true))
	if got := d.Proposed / before; got < 1.049 {
		t.Fatalf("after ProbeRamp the full 5%% step should return, got %.4f", got)
	}
}

// storm is a step during a timeout storm: the store's queue holds only calls that will
// time out, so the delivered rate collapses.
func storm(at time.Duration) Signals {
	s := healthy(at, 2*time.Second, true)
	s.ErrorRatio, s.Span = 0.9, 3*time.Second
	return s
}

// draining is a step while the store drains its queue: still slow, delivering what it can.
func draining(at, p99 time.Duration, delivered float64) Signals {
	s := healthy(at, p99, true)
	s.Throughput, s.Span = delivered, 3*time.Second
	return s
}

func TestRecoversToWhatTheStoreDeliveredAfterACut(t *testing.T) {
	env := DefaultEnvelope(3000)
	c := New(4000)
	c.Step(env, 3000, healthy(0, 10*time.Millisecond, false))
	if d := c.Step(env, 3000, storm(time.Second)); d.Reason != ReasonErrors || d.Proposed != 2000 {
		t.Fatalf("expected the storm to halve the rate, got %+v", d)
	}

	// A signal window that began before the cut doesn't say what the store delivers now.
	if d := c.Step(env, 3000, draining(2*time.Second, time.Second, 2500)); d.Delivered != 0 {
		t.Fatalf("a window from before the cut counted, got %+v", d)
	}
	if d := c.Step(env, 3000, draining(4*time.Second, 500*time.Millisecond, 3000)); d.Reason != ReasonCooldown || d.Delivered != 3000 {
		t.Fatalf("expected the store's 3000/s since the cut to count, got %+v", d)
	}

	// Healthy and binding again: back to 0.9 x what the store delivered, not 5% up from the cut.
	s := healthy(8*time.Second, 10*time.Millisecond, true)
	s.Throughput, s.Span = 2000, 3*time.Second
	d := c.Step(env, 3000, s)
	if d.Reason != ReasonRecover || d.Proposed != 2700 || d.Delivered != 3000 {
		t.Fatalf("expected a return to 0.9 x 3000 = 2700, got %+v", d)
	}
	// From there it probes slowly, as near any other point where the store overloaded.
	d = c.Step(env, 3000, healthy(9*time.Second, 10*time.Millisecond, true))
	if d.Reason != ReasonProbe || d.Proposed/2700 > 1.02 {
		t.Fatalf("expected a slow probe after the return, got %+v", d)
	}
}

func TestEachCutStartsOverWhatTheStoreDelivered(t *testing.T) {
	env := DefaultEnvelope(3000)
	c := New(4000)
	c.Step(env, 3000, healthy(0, 10*time.Millisecond, false))
	c.Step(env, 3000, storm(time.Second)) // 4000 -> 2000
	c.Step(env, 3000, draining(4*time.Second, 500*time.Millisecond, 3000))
	// The store slows again before the controller recovers: a second storm, and less delivered.
	if d := c.Step(env, 3000, storm(7*time.Second)); d.Reason != ReasonErrors || d.Proposed != 1000 || d.Delivered != 0 {
		t.Fatalf("expected a second cut that starts over, got %+v", d)
	}
	c.Step(env, 3000, draining(10*time.Second, 500*time.Millisecond, 1500))
	d := c.Step(env, 3000, healthy(14*time.Second, 10*time.Millisecond, true))
	if d.Reason != ReasonRecover || d.Proposed != 1350 {
		t.Fatalf("expected a return to 0.9 x the 1500 since the second cut, got %+v", d)
	}
}

func TestRecoveryStopsAtTheRateBeforeTheCut(t *testing.T) {
	env := DefaultEnvelope(3000)
	c := New(2200)
	c.Step(env, 3000, healthy(0, 10*time.Millisecond, false))
	c.Step(env, 3000, storm(time.Second)) // 2200 -> 1100
	c.Step(env, 3000, draining(4*time.Second, 500*time.Millisecond, 3000))
	d := c.Step(env, 3000, healthy(8*time.Second, 10*time.Millisecond, true))
	if d.Reason != ReasonRecover || d.Proposed != 2200 {
		t.Fatalf("expected a return to no more than the 2200 before the cut, got %+v", d)
	}
}

func TestRecoveryOnlyWhileBindingAndWithinTheDeliveryWindow(t *testing.T) {
	env := DefaultEnvelope(3000)
	c := New(4000)
	c.Step(env, 3000, healthy(0, 10*time.Millisecond, false))
	c.Step(env, 3000, storm(time.Second)) // 4000 -> 2000
	c.Step(env, 3000, draining(4*time.Second, 500*time.Millisecond, 3000))

	// Healthy but idle: no windup.
	for at := 8 * time.Second; at <= 31*time.Second; at += time.Second {
		if d := c.Step(env, 3000, healthy(at, 10*time.Millisecond, false)); d.Proposed != 2000 || d.Reason != ReasonIdle {
			t.Fatalf("the rate moved while idle: %+v", d)
		}
	}
	// Binding once DeliveryWindow has passed: only the probe.
	d := c.Step(env, 3000, healthy(32*time.Second, 10*time.Millisecond, true))
	if d.Reason != ReasonProbe || d.Delivered != 0 || d.Proposed > 2100 {
		t.Fatalf("expected a plain probe after DeliveryWindow, got %+v", d)
	}
}

func TestNoRecoveryWithoutACutOrWithoutADeliveryWindow(t *testing.T) {
	env := DefaultEnvelope(1000)
	c := New(1000)
	s := healthy(0, 10*time.Millisecond, true)
	s.Throughput, s.Span = 2000, 3*time.Second // a burst the bucket had saved up
	if d := c.Step(env, 1000, s); d.Reason != ReasonProbe || d.Proposed != 1050 {
		t.Fatalf("a burst without a cut must only probe, got %+v", d)
	}

	env = DefaultEnvelope(3000)
	env.DeliveryWindow = 0
	c = New(4000)
	c.Step(env, 3000, healthy(0, 10*time.Millisecond, false))
	c.Step(env, 3000, storm(time.Second))
	c.Step(env, 3000, draining(4*time.Second, 500*time.Millisecond, 3000))
	if d := c.Step(env, 3000, healthy(8*time.Second, 10*time.Millisecond, true)); d.Reason != ReasonProbe || d.Proposed > 2100 {
		t.Fatalf("DeliveryWindow 0 must turn the return off, got %+v", d)
	}
}

func TestBaselineAlertReportsMovesFromWhereTheBaselineSettled(t *testing.T) {
	env := DefaultEnvelope(1000) // BaselineWindow 10m, BaselineAlert 2
	c := New(1000)
	step := func(minute int, p99 time.Duration) Decision {
		return c.Step(env, 1000, healthy(time.Duration(minute)*time.Minute, p99, false))
	}
	moves := func(from, to int, p99 time.Duration) []Decision {
		var moved []Decision
		for m := from; m <= to; m++ {
			if d := step(m, p99); d.BaselineMovedFrom != 0 {
				moved = append(moved, d)
			}
		}
		return moved
	}

	// The first calls are slow, as on a cold start. The baseline settles once it has a
	// whole window of samples, so its fall to 10ms isn't a move.
	if moved := moves(0, 0, 40*time.Millisecond); len(moved) != 0 {
		t.Fatalf("reported a move on the first sample: %+v", moved)
	}
	if moved := moves(1, 10, 10*time.Millisecond); len(moved) != 0 {
		t.Fatalf("reported a move while the baseline was settling: %+v", moved)
	}
	// From the 10ms it settled on, 15ms is under the factor.
	if moved := moves(11, 25, 15*time.Millisecond); len(moved) != 0 {
		t.Fatalf("a move of 1.5x was reported: %+v", moved)
	}
	// 30ms is twice 10ms. The move is reported once, when the 15ms samples leave the window.
	moved := moves(26, 40, 30*time.Millisecond)
	if len(moved) != 1 || moved[0].BaselineMovedFrom != 10*time.Millisecond || moved[0].Baseline != 30*time.Millisecond {
		t.Fatalf("expected one move from 10ms to 30ms, got %+v", moved)
	}
	// The next move is measured from 30ms: down to 14ms is more than halving it.
	moved = moves(41, 55, 14*time.Millisecond)
	if len(moved) != 1 || moved[0].BaselineMovedFrom != 30*time.Millisecond || moved[0].Baseline != 14*time.Millisecond {
		t.Fatalf("expected one move from 30ms down to 14ms, got %+v", moved)
	}
}

func TestBaselineAlertWaitsForAWholeWindowAfterAGap(t *testing.T) {
	env := DefaultEnvelope(1000)
	c := New(1000)
	for m := 0; m <= 10; m++ {
		c.Step(env, 1000, healthy(time.Duration(m)*time.Minute, 10*time.Millisecond, false))
	}
	// No calls for 20 minutes, so the window forgets the 10ms samples. A baseline learned
	// from a few calls after the gap isn't reported until it has a whole window.
	for m := 30; m < 40; m++ {
		if d := c.Step(env, 1000, healthy(time.Duration(m)*time.Minute, 40*time.Millisecond, false)); d.BaselineMovedFrom != 0 {
			t.Fatalf("reported a baseline learned since the gap at minute %d: %+v", m, d)
		}
	}
	if d := c.Step(env, 1000, healthy(40*time.Minute, 40*time.Millisecond, false)); d.BaselineMovedFrom != 10*time.Millisecond {
		t.Fatalf("expected the move from 10ms to be reported a window after the gap, got %+v", d)
	}
}

func TestBaselineAlertOff(t *testing.T) {
	env := DefaultEnvelope(1000)
	env.BaselineAlert = 0
	c := New(1000)
	for m := 0; m <= 30; m++ {
		p99 := 10 * time.Millisecond
		if m > 10 {
			p99 = 100 * time.Millisecond
		}
		if d := c.Step(env, 1000, healthy(time.Duration(m)*time.Minute, p99, false)); d.BaselineMovedFrom != 0 {
			t.Fatalf("BaselineAlert 0 reported a move: %+v", d)
		}
	}
}
