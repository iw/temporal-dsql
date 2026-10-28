package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReservoirRefillConcurrency(t *testing.T) {
	for _, variant := range []string{"as-deployed", "option-a"} {
		t.Run(variant, func(t *testing.T) {
			r, p := testPool(t, variant)
			p.cfg.InflightLimit = 3
			p.cfg.TargetReady = 8
			r.cluster.cfg.Handshake = LatencyRange{10 * time.Millisecond, 10 * time.Millisecond}
			p.start()
			limit := 1
			if variant == "option-a" {
				limit = 3
			}
			require.Equal(t, limit, p.constructing)
			for range 80 {
				advance(r, time.Millisecond)
				require.LessOrEqual(t, p.constructing, limit)
				require.LessOrEqual(t, len(p.ready)+p.constructing, p.cfg.TargetReady)
			}
			require.Len(t, p.ready, 8)
			require.Equal(t, limit, p.instance.peakInflight)
		})
	}
}

func TestConcurrentRefillFailureBackoff(t *testing.T) {
	r, p := testPool(t, "option-a")
	p.cfg.InflightLimit = 3
	p.cfg.TargetReady = 10
	r.cluster.cfg.MaxConnections = 1
	p.start()
	advance(r, time.Millisecond)
	require.Equal(t, 2, p.refillBackoffs)
	require.Equal(t, 1, p.constructing, "success may refill while other slots back off")
	advance(r, time.Millisecond)
	require.Equal(t, 3, p.refillBackoffs)
	require.Zero(t, p.constructing)
	attempts := r.stats.attempts[0]
	p.refill()
	advance(r, FailureBackoff-time.Millisecond-time.Nanosecond)
	require.Equal(t, attempts, r.stats.attempts[0], "other triggers cannot bypass failed-slot backoff")
	advance(r, time.Nanosecond)
	require.Equal(t, attempts+2, r.stats.attempts[0])
	require.Equal(t, 2, p.constructing)
	p.close()
	advance(r, time.Second)
	require.Zero(t, p.constructing)
	require.Zero(t, p.physical)
}

func TestConcurrentRefillUsesSharedAndEphemeralLimits(t *testing.T) {
	cfg := testConfig("option-a")
	cfg.Services[0].TargetReady = 10
	cfg.Services[0].InflightLimit = 3
	cfg.Services[0].Ephemeral = EphemeralConfig{Pools: 2, Connections: 3, Duration: time.Second}
	r, err := newComparison(cfg, "")
	require.NoError(t, err)
	advance(r, 0)
	i := r.instances[0]
	require.Equal(t, 3, i.active)
	require.Equal(t, 3, i.main.constructing)
	for _, p := range i.ephemeral {
		require.Equal(t, 2, p.constructing)
	}
	advance(r, 2*time.Second)
	require.Equal(t, 3, i.peakInflight)
}

func TestSQLIdlePing(t *testing.T) {
	for _, variant := range []string{"as-deployed", "option-a"} {
		for _, age := range []time.Duration{time.Second - time.Nanosecond, time.Second, time.Second + time.Nanosecond} {
			t.Run(variant+"/"+age.String(), func(t *testing.T) {
				r, p := testPool(t, variant)
				p.start()
				advance(r, 3*time.Millisecond)
				first := p.request(900*time.Millisecond, time.Second, nil)
				handedOut := p.now()
				advance(r, age)
				require.True(t, first.done)
				require.Equal(t, handedOut, first.conn.lastReset, "release must not reset the ping clock")
				r.cluster.drop(first.conn)
				next := p.request(time.Millisecond, time.Second, nil)
				advance(r, time.Millisecond)
				if age > time.Second {
					require.NotSame(t, first.conn, next.conn)
					require.Equal(t, 1, next.badConnRetries)
					require.Zero(t, r.stats.failures["dead"])
					require.Zero(t, r.stats.rebuilds)
					require.EqualValues(t, 2, r.stats.completed)
				} else {
					require.Same(t, first.conn, next.conn)
					require.EqualValues(t, 1, r.stats.failures["dead"])
					if variant == "as-deployed" {
						require.EqualValues(t, 1, r.stats.rebuilds)
					} else {
						require.Zero(t, r.stats.rebuilds)
					}
				}
			})
		}
	}
}

func TestSQLIdlePingRetriesTwiceThenReservoir(t *testing.T) {
	for _, variant := range []string{"as-deployed", "option-a"} {
		t.Run(variant, func(t *testing.T) {
			r, p := testPool(t, variant)
			p.cfg.MaxOpen = 4
			p.cfg.TargetReady = 4
			p.start()
			advance(r, 5*time.Millisecond)
			for range 4 {
				p.request(time.Millisecond, time.Second, nil)
			}
			advance(r, 1100*time.Millisecond)
			require.Len(t, p.idle, 4)
			survivor := p.idle[0]
			for _, c := range p.idle[1:] {
				r.cluster.drop(c)
			}
			fresh := p.ready[0]
			req := p.request(time.Millisecond, time.Second, nil)
			require.Same(t, fresh, req.conn, "third attempt bypasses remaining idle connections")
			require.Equal(t, 2, req.badConnRetries)
			require.Len(t, p.idle, 2)
			require.Same(t, survivor, p.idle[0])
			require.False(t, p.idle[1].alive, "third stale idle was not inspected")
			advance(r, time.Millisecond)
			require.True(t, req.done)
			require.Empty(t, r.stats.failures)
			require.Zero(t, r.stats.rebuilds)
		})
	}
}

func TestSQLLifetimeDiscardsShareRetries(t *testing.T) {
	r, p := testPool(t, "option-a")
	p.cfg.MaxOpen = 4
	p.cfg.TargetReady = 4
	p.start()
	advance(r, 5*time.Millisecond)
	for range 4 {
		p.request(time.Millisecond, time.Second, nil)
	}
	advance(r, 100*time.Millisecond)
	require.Len(t, p.idle, 4)
	for _, c := range p.idle {
		c.expires = p.now().Add(p.cfg.GuardWindow / 2)
	}
	fresh := p.ready[0]
	req := p.request(time.Millisecond, time.Second, nil)
	require.Same(t, fresh, req.conn, "two lifetime discards exhaust the cached attempts")
	require.Equal(t, 2, req.badConnRetries)
	require.Len(t, p.idle, 2, "later requests discard the remaining expired connections")
	advance(r, time.Millisecond)
	require.True(t, req.done)
	require.Empty(t, r.stats.failures)
}
