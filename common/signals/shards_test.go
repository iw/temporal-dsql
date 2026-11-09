package signals

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPlaneKeepsShardWaitsWhileItRuns(t *testing.T) {
	f := newPlaneFixture(t, nil)
	f.plane.RecordShardWait(time.Second) // before the plane runs: not kept

	require.Equal(t, ShardWrites{}, f.tick().ShardWrites)
	for range 10 {
		f.plane.RecordShardWait(40 * time.Millisecond)
	}
	require.Equal(t, ShardWrites{WaitP90: 40 * time.Millisecond, Samples: 10}, f.tick().ShardWrites)
}
