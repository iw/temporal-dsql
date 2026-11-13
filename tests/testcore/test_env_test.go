package testcore

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/parallelsuite"
)

type TestEnvSuite struct {
	parallelsuite.Suite[*TestEnvSuite]
}

func TestTestEnvSuite(t *testing.T) {
	parallelsuite.Run(t, &TestEnvSuite{})
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_NoErrorWithoutExplicitRequest() {
	guard := newDedicatedClusterGuard(false)

	s.NoError(guard.validate())
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_FailsWhenUnused() {
	guard := newDedicatedClusterGuard(true)

	s.EqualError(guard.validate(),
		`testcore.WithDedicatedCluster() was requested but no dedicated-cluster-only feature was used`)
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_NoErrorAfterUse() {
	guard := newDedicatedClusterGuard(true)
	guard.record("global hook")

	s.NoError(guard.validate())
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_ConcurrentRecord() {
	guard := newDedicatedClusterGuard(true)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			guard.record("reason")
		})
	}
	wg.Wait()
	s.NoError(guard.validate())
}

func TestTestShardRunsTestsWithTheirEnclosingTest(t *testing.T) {
	t.Parallel()
	const total = 4
	const method = "TestSuite/TestMethod"
	var cases []string
	shards := map[int]bool{}
	for i := range 32 {
		name := fmt.Sprintf("%s/Case_%d", method, i)
		cases = append(cases, name, name+"/Step")
		shards[shardOf(name, total)] = true
	}
	// The cases' own names hash to several shards, but they can only run in their method's.
	require.Greater(t, len(shards), 1)

	runs := map[string]int{}
	for index := range total {
		// Each shard is a test process of its own. The method checks its shard, as one that
		// creates a test env does, and then so does each case.
		var inShard sync.Map
		if shard, _ := testShard(&inShard, method, total, index); shard != index {
			continue
		}
		for _, name := range cases {
			shard, enclosing := testShard(&inShard, name, total, index)
			require.Equal(t, index, shard, name)
			require.Equal(t, method, enclosing, name)
			runs[name]++
		}
	}
	for _, name := range cases {
		require.Equal(t, 1, runs[name], name)
	}
}

func TestTestShardRunsTestsWithNoEnclosingTestByTheirNames(t *testing.T) {
	t.Parallel()
	const total = 4
	runs := map[string]int{}
	for index := range total {
		var inShard sync.Map
		for i := range 32 {
			name := fmt.Sprintf("TestSuite/TestMethod/Case_%d", i)
			shard, enclosing := testShard(&inShard, name, total, index)
			require.Equal(t, shardOf(name, total), shard, name)
			require.Empty(t, enclosing, name)
			if shard == index {
				runs[name]++
			}
		}
	}
	require.Len(t, runs, 32)
	for name, count := range runs {
		require.Equal(t, 1, count, name)
	}
}
