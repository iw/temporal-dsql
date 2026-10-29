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
