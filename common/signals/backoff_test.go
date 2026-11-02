package signals

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
	"go.temporal.io/server/common/persistence"
)

func TestPersistenceBackingOff(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	backoff := Backoff{Hold: 10 * time.Second, MaxErrorRatio: 0.01, MaxQueuedRatio: 0.25, MinSamples: 20}
	healthy := persistence.HealthSnapshot{Samples: 1000}
	capacityDecision := func(reason capacity.Reason, age time.Duration) map[string]adaptive.Decision {
		return map[string]adaptive.Decision{capacity.Name: {Controller: capacity.Name, Time: now.Add(-age), Reason: adaptive.Reason(reason)}}
	}
	for name, tc := range map[string]struct {
		persistence persistence.HealthSnapshot
		decisions   map[string]adaptive.Decision
		want        bool
	}{
		"healthy":                          {persistence: healthy},
		"capacity controller cut":          {persistence: healthy, decisions: capacityDecision(capacity.ReasonLatency, 5*time.Second), want: true},
		"capacity controller cooling down": {persistence: healthy, decisions: capacityDecision(capacity.ReasonCooldown, time.Second), want: true},
		"capacity controller cut long ago": {persistence: healthy, decisions: capacityDecision(capacity.ReasonErrors, time.Minute)},
		"capacity controller probing":      {persistence: healthy, decisions: capacityDecision(capacity.ReasonProbe, time.Second)},
		"errors":                           {persistence: persistence.HealthSnapshot{Samples: 1000, ErrorRatio: 0.02}, want: true},
		"throttled":                        {persistence: persistence.HealthSnapshot{Samples: 1000, Throttled: 20}, want: true},
		"limiter rejecting":                {persistence: persistence.HealthSnapshot{Samples: 1000, Rejected: 20}, want: true},
		"a few failures among few calls":   {persistence: persistence.HealthSnapshot{Samples: 5, ErrorRatio: 0.2}},
		"pool acquires waiting":            {persistence: persistence.HealthSnapshot{Samples: 1000, Queued: 300, Acquired: 1000}, want: true},
		"a few pool acquires waiting":      {persistence: persistence.HealthSnapshot{Samples: 1000, Queued: 100, Acquired: 1000}},
	} {
		t.Run(name, func(t *testing.T) {
			snap := &Snapshot{Time: now, Persistence: tc.persistence, Decisions: tc.decisions}
			require.Equal(t, tc.want, snap.PersistenceBackingOff(backoff))
		})
	}
}
