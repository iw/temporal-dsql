package gate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func newTestPolicy(size int) (*Policy, *clock.EventTimeSource) {
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC))
	return NewPolicy(size, timeSource), timeSource
}

func recordHolds(p *Policy, kind Kind, hold time.Duration, n int) {
	for range n {
		p.RecordHold(kind, hold)
	}
}

func TestPolicyAdmitsUpToItsSizeAtOnce(t *testing.T) {
	p, timeSource := newTestPolicy(2)
	deadline := timeSource.Now().Add(time.Second)
	for range 2 {
		admission := p.Admit(Read, deadline, true)
		require.NotNil(t, admission.Ticket)
		require.Nil(t, admission.Waiter)
	}
	require.Equal(t, 2, p.Held())
}

func TestPolicyHandsReleasedSlotsToWritesFirst(t *testing.T) {
	p, timeSource := newTestPolicy(1)
	deadline := timeSource.Now().Add(time.Minute)
	held := p.Admit(Read, deadline, true).Ticket
	require.NotNil(t, held)

	read := p.Admit(Read, deadline, true).Waiter
	write := p.Admit(Write, deadline, true).Waiter
	noDeadline := p.Admit(Read, time.Time{}, false).Waiter
	require.NotNil(t, read)
	require.NotNil(t, write)
	require.NotNil(t, noDeadline)
	priority, reads := p.Queued()
	require.Equal(t, 2, priority)
	require.Equal(t, 1, reads)

	require.Same(t, write, held.Release(true))
	require.Same(t, noDeadline, write.Ticket().Release(true))
	require.Same(t, read, noDeadline.Ticket().Release(true))
	require.Nil(t, read.Ticket().Release(true))
	require.Zero(t, p.Held())
	for _, w := range []*Waiter{read, write, noDeadline} {
		select {
		case <-w.Ready():
		default:
			require.Fail(t, "a granted waiter's Ready should be closed")
		}
	}
}

func TestPolicyRefusesAtOnceACallerItCantServeInTime(t *testing.T) {
	p, timeSource := newTestPolicy(1)
	// Reads have no holds yet, so they're judged by the writes': a 1 s wait for the slot,
	// and 1.5 s reserved for the read.
	recordHolds(p, Write, time.Second, 10)
	require.NotNil(t, p.Admit(Write, time.Time{}, false).Ticket)

	refused := p.Admit(Read, timeSource.Now().Add(2499*time.Millisecond), true)
	require.Nil(t, refused.Ticket)
	require.Nil(t, refused.Waiter)

	queued := p.Admit(Read, timeSource.Now().Add(2500*time.Millisecond), true).Waiter
	require.NotNil(t, queued)
	require.Equal(t, timeSource.Now().Add(time.Second), queued.GiveUp())
}

func TestPolicyCountsTheCallersAheadInTheWait(t *testing.T) {
	p, timeSource := newTestPolicy(2)
	recordHolds(p, Read, 100*time.Millisecond, 10)
	recordHolds(p, Write, 400*time.Millisecond, 10)
	deadline := timeSource.Now().Add(time.Minute)
	for range 2 {
		require.NotNil(t, p.Admit(Read, deadline, true).Ticket)
	}
	require.NotNil(t, p.Admit(Read, deadline, true).Waiter)

	// One read ahead: (1+1)/2 slots × the larger mean hold, 400 ms, and 150 ms reserved.
	require.Nil(t, p.Admit(Read, timeSource.Now().Add(549*time.Millisecond), true).Waiter)
	require.NotNil(t, p.Admit(Read, timeSource.Now().Add(550*time.Millisecond), true).Waiter)
	// A write waits only behind the writes, and reserves 600 ms for its own hold.
	require.Nil(t, p.Admit(Write, timeSource.Now().Add(799*time.Millisecond), true).Waiter)
	require.NotNil(t, p.Admit(Write, timeSource.Now().Add(800*time.Millisecond), true).Waiter)
}

func TestPolicyWithdraw(t *testing.T) {
	p, timeSource := newTestPolicy(1)
	deadline := timeSource.Now().Add(time.Minute)
	held := p.Admit(Read, deadline, true).Ticket

	gaveUp := p.Admit(Read, deadline, true).Waiter
	require.False(t, p.Withdraw(gaveUp))
	_, reads := p.Queued()
	require.Zero(t, reads)

	granted := p.Admit(Read, deadline, true).Waiter
	require.Same(t, granted, held.Release(false))
	require.True(t, p.Withdraw(granted), "a caller granted a slot before it withdrew holds the slot")
	require.Equal(t, 1, p.Held())
}

func TestTicketRecordsTheHoldOnTheClock(t *testing.T) {
	p, timeSource := newTestPolicy(2)
	deadline := timeSource.Now().Add(time.Minute)
	used := p.Admit(Write, deadline, true).Ticket
	unused := p.Admit(Read, deadline, true).Ticket
	timeSource.Advance(30 * time.Millisecond)

	used.Release(true)
	used.Release(true)
	unused.Release(false)
	require.Zero(t, p.Held())
	require.Equal(t, 1, p.Holds(Write))
	require.Zero(t, p.Holds(Read))
	p90, mean := p.Estimate(Write)
	require.Equal(t, 30*time.Millisecond, p90)
	require.Equal(t, 30*time.Millisecond, mean)
}

func TestPolicySeedsAnEmptyEstimateFromTheOtherKind(t *testing.T) {
	p, _ := newTestPolicy(1)
	recordHolds(p, Write, 40*time.Millisecond, 3)
	p90, mean := p.Estimate(Read)
	require.Equal(t, 40*time.Millisecond, p90)
	require.Equal(t, 40*time.Millisecond, mean)

	recordHolds(p, Read, 5*time.Millisecond, 3)
	p90, mean = p.Estimate(Read)
	require.Equal(t, 5*time.Millisecond, p90)
	require.Equal(t, 5*time.Millisecond, mean)
}

func TestPolicyRecomputesItsEstimateAtMostEveryInterval(t *testing.T) {
	p, timeSource := newTestPolicy(1)
	recordHolds(p, Read, 10*time.Millisecond, 10)
	p90, _ := p.Estimate(Read)
	require.Equal(t, 10*time.Millisecond, p90)

	recordHolds(p, Read, 20*time.Millisecond, 10)
	timeSource.Advance(holdEstimateInterval - time.Millisecond)
	p90, _ = p.Estimate(Read)
	require.Equal(t, 10*time.Millisecond, p90)
	timeSource.Advance(time.Millisecond)
	p90, _ = p.Estimate(Read)
	require.Equal(t, 20*time.Millisecond, p90)
}

func TestHoldTimesEstimate(t *testing.T) {
	var h holdTimes
	now := time.Now()
	_, _, ok := h.estimate(now)
	require.False(t, ok)
	for i := 1; i <= 10; i++ {
		h.record(time.Duration(i) * time.Millisecond)
	}
	p90, mean, ok := h.estimate(now)
	require.True(t, ok)
	require.Equal(t, 9*time.Millisecond, p90)
	require.Equal(t, 5500*time.Microsecond, mean)
}

func TestHoldTimesKeepsTheMostRecent(t *testing.T) {
	var h holdTimes
	for range HoldSamples {
		h.record(time.Second)
	}
	for range HoldSamples {
		h.record(time.Millisecond)
	}
	p90, mean, ok := h.estimate(time.Now())
	require.True(t, ok)
	require.Equal(t, time.Millisecond, p90)
	require.Equal(t, time.Millisecond, mean)
}
