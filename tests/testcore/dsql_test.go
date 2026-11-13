package testcore

import (
	"bufio"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
)

func persistenceOn(plugin string) config.Persistence {
	return config.Persistence{
		DefaultStore: "test",
		DataStores:   map[string]config.DataStore{"test": {SQL: &config.SQL{PluginName: plugin}}},
	}
}

func TestClusterDynamicConfigOverrides(t *testing.T) {
	defaults := maps.Clone(defaultDynamicConfigOverrides)
	require.Equal(t, defaults, clusterDynamicConfigOverrides(persistenceOn(sqlite.PluginName)))
	require.Equal(t, defaults, clusterDynamicConfigOverrides(config.Persistence{}))

	got := clusterDynamicConfigOverrides(persistenceOn(dsqlPluginName))
	want := maps.Clone(defaults)
	want[dynamicconfig.TransactionSizeLimit.Key()] = 1 << 20
	want[dynamicconfig.BlobSizeLimitError.Key()] = 1_000_000
	require.Equal(t, want, got)
	require.Equal(t, defaults, defaultDynamicConfigOverrides)
}

func TestUseSQLVisibilityOnDSQL(t *testing.T) {
	driver := cliFlags.persistenceDriver
	t.Cleanup(func() { cliFlags.persistenceDriver = driver })
	cliFlags.persistenceDriver = dsqlPluginName
	require.True(t, UseSQLVisibility())
}

func TestPoolMetricsHandler(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pools.jsonl")
	file, err := openPoolMetricsFile(path)
	require.NoError(t, err)
	capture := metricstest.NewCaptureHandler()
	recording := capture.StartCapture()
	defer capture.StopCapture(recording)

	var handler metrics.Handler = &poolMetricsHandler{Handler: capture, file: file, cluster: "corpus_1", test: "TestPools"}
	pool := handler.WithTags(metrics.ServiceNameTag(primitives.HistoryService)).WithTags(metrics.PersistenceDBKindTag("main"))
	pool.Gauge("dsql_pool_in_use").Record(3)
	pool.Counter("dsql_pool_wait_total").Record(2, metrics.StringTag("reason", "test"))
	pool.Timer("dsql_pool_connect_latency").Record(1500 * time.Microsecond)
	pool.Counter("persistence_requests").Record(1)
	handler.Gauge("dsql_pool_open").Record(1)

	opened, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, opened.Close()) }()
	var lines []poolMetric
	scanner := bufio.NewScanner(opened)
	for scanner.Scan() {
		var line poolMetric
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line))
		require.WithinDuration(t, time.Now(), line.Time, time.Minute)
		line.Time = time.Time{}
		lines = append(lines, line)
	}
	require.NoError(t, scanner.Err())
	poolTags := map[string]string{"service_name": "history", metrics.PersistenceDBKindTagName: "main"}
	withReason := maps.Clone(poolTags)
	withReason["reason"] = "test"
	require.Equal(t, []poolMetric{
		{Cluster: "corpus_1", Test: "TestPools", Metric: "dsql_pool_in_use", Value: 3, Tags: poolTags},
		{Cluster: "corpus_1", Test: "TestPools", Metric: "dsql_pool_wait_total", Value: 2, Tags: withReason},
		{Cluster: "corpus_1", Test: "TestPools", Metric: "dsql_pool_connect_latency", Value: 1.5, Tags: poolTags},
		{Cluster: "corpus_1", Test: "TestPools", Metric: "dsql_pool_open", Value: 1},
	}, lines)

	// The cluster's handler records every metric.
	snapshot := recording.Snapshot()
	for _, name := range []string{"dsql_pool_in_use", "dsql_pool_wait_total", "dsql_pool_connect_latency", "persistence_requests", "dsql_pool_open"} {
		require.Len(t, snapshot[name], 1, name)
	}
}

func TestWithDSQLPoolMetricsNeedsFile(t *testing.T) {
	handler := metrics.NoopMetricsHandler
	require.Equal(t, handler, withDSQLPoolMetrics(handler, persistenceOn(dsqlPluginName), "TestPools"))
	require.False(t, runsOnDSQL(persistenceOn(sqlite.PluginName)))
	require.True(t, runsOnDSQL(persistenceOn(dsqlPluginName)))
}

func TestDSQLSuitePlaceCount(t *testing.T) {
	places, err := dsqlSuitePlaceCount(9, 3, 2)
	require.NoError(t, err)
	require.Equal(t, 3, places)

	places, err = dsqlSuitePlaceCount(7, 3, 2)
	require.NoError(t, err)
	require.Equal(t, 1, places)

	_, err = dsqlSuitePlaceCount(6, 3, 2)
	require.ErrorContains(t, err, "lists 6 schemas, but 3 shared and 2 dedicated test clusters need at least 7")
}

func TestTakeDSQLSuitePlace(t *testing.T) {
	takeDSQLSuitePlace(t, nil)()

	places := make(chan struct{}, 1)
	first := takeDSQLSuitePlace(t, places)
	taken := make(chan func(), 1)
	go func() { taken <- takeDSQLSuitePlace(t, places) }()
	select {
	case <-taken:
		t.Fatal("took a place while none was free")
	case <-time.After(50 * time.Millisecond):
	}

	first()
	first()
	var second func()
	await.RequireTrue(t, func() bool {
		select {
		case second = <-taken:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Len(t, places, 1)
	second()
	require.Empty(t, places)
}
