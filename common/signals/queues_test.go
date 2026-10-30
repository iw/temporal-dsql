package signals

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type countingRecorder struct {
	delays []time.Duration
}

func (c *countingRecorder) RecordScheduleLatency(latency time.Duration) {
	c.delays = append(c.delays, latency)
}

func TestPlaneKeepsEachQueuesDelayWhileItRuns(t *testing.T) {
	f := newPlaneFixture(t, nil)
	next := &countingRecorder{}
	transfer := f.plane.QueueRecorder("transfer", next)
	timer := f.plane.QueueRecorder("timer", nil)

	transfer.RecordScheduleLatency(time.Second) // before the plane runs: passed on, not kept
	require.Equal(t, []time.Duration{time.Second}, next.delays)

	require.Equal(t, Queue{}, f.tick().Queues["transfer"])
	for range 10 {
		transfer.RecordScheduleLatency(40 * time.Millisecond)
	}
	f.plane.QueueRecorder("transfer", nil).RecordScheduleLatency(40 * time.Millisecond) // the same queue
	timer.RecordScheduleLatency(2 * time.Millisecond)

	queues := f.tick().Queues
	require.Equal(t, Queue{DelayP90: 40 * time.Millisecond, Samples: 11}, queues["transfer"])
	require.Equal(t, Queue{DelayP90: 2 * time.Millisecond, Samples: 1}, queues["timer"])
	require.Len(t, next.delays, 11)
}
