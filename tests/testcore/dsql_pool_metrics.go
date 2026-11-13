package testcore

import (
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"os"
	"strings"
	"sync"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/metrics"
)

// dsqlPoolMetricsEnv names a file to which test clusters on DSQL append their connection
// pools' metrics, one JSON object a line, so that a run shows how each service used its
// pools. docs/dsql/ci.md describes it.
const dsqlPoolMetricsEnv = "DSQL_TEST_POOL_METRICS_FILE"

// The DSQL plugin's pool metrics start with this prefix.
const dsqlPoolMetricPrefix = "dsql_pool_"

// dsqlPoolMetricsOutput is the test process's file for pool metrics, or nil.
var dsqlPoolMetricsOutput = sync.OnceValue(func() *poolMetricsFile {
	path := os.Getenv(dsqlPoolMetricsEnv)
	if path == "" {
		return nil
	}
	file, err := openPoolMetricsFile(path)
	if err != nil {
		log.Printf("DSQL pool metrics disabled: %v", err)
		return nil
	}
	return file
})

type (
	poolMetricsFile struct {
		mu   sync.Mutex
		file *os.File
	}

	// poolMetric is one line of the file. Cluster is the cluster's persistence schema,
	// and Test the test that created the cluster.
	poolMetric struct {
		Time    time.Time         `json:"time"`
		Cluster string            `json:"cluster"`
		Test    string            `json:"test"`
		Metric  string            `json:"metric"`
		Value   float64           `json:"value"`
		Tags    map[string]string `json:"tags,omitempty"`
	}

	// poolMetricsHandler passes every metric on to a cluster's handler, and writes the
	// DSQL plugin's pool metrics to the file too. Timers are written in milliseconds.
	poolMetricsHandler struct {
		metrics.Handler
		file    *poolMetricsFile
		cluster string
		test    string
		tags    map[string]string
	}
)

func openPoolMetricsFile(path string) (*poolMetricsFile, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("cannot open %q: %w", path, err)
	}
	return &poolMetricsFile{file: file}, nil
}

func (f *poolMetricsFile) write(metric poolMetric) {
	line, err := json.Marshal(metric)
	if err != nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	_, _ = f.file.Write(append(line, '\n'))
}

// withDSQLPoolMetrics returns the metrics handler for a cluster's server. On DSQL, with
// DSQL_TEST_POOL_METRICS_FILE set, it also writes the pool metrics to that file.
func withDSQLPoolMetrics(handler metrics.Handler, cfg config.Persistence, test string) metrics.Handler {
	file := dsqlPoolMetricsOutput()
	if file == nil || !runsOnDSQL(cfg) {
		return handler
	}
	schema := cfg.DataStores[cfg.DefaultStore].SQL.ConnectAttributes["search_path"]
	return &poolMetricsHandler{Handler: handler, file: file, cluster: schema, test: test}
}

func (h *poolMetricsHandler) WithTags(tags ...metrics.Tag) metrics.Handler {
	merged := make(map[string]string, len(h.tags)+len(tags))
	maps.Copy(merged, h.tags)
	for _, tag := range tags {
		merged[tag.Key] = tag.Value
	}
	tagged := *h
	tagged.Handler = h.Handler.WithTags(tags...)
	tagged.tags = merged
	return &tagged
}

func (h *poolMetricsHandler) Counter(name string) metrics.CounterIface {
	counter := h.Handler.Counter(name)
	if !strings.HasPrefix(name, dsqlPoolMetricPrefix) {
		return counter
	}
	return metrics.CounterFunc(func(value int64, tags ...metrics.Tag) {
		counter.Record(value, tags...)
		h.write(name, float64(value), tags)
	})
}

func (h *poolMetricsHandler) Gauge(name string) metrics.GaugeIface {
	gauge := h.Handler.Gauge(name)
	if !strings.HasPrefix(name, dsqlPoolMetricPrefix) {
		return gauge
	}
	return metrics.GaugeFunc(func(value float64, tags ...metrics.Tag) {
		gauge.Record(value, tags...)
		h.write(name, value, tags)
	})
}

func (h *poolMetricsHandler) Timer(name string) metrics.TimerIface {
	timer := h.Handler.Timer(name)
	if !strings.HasPrefix(name, dsqlPoolMetricPrefix) {
		return timer
	}
	return metrics.TimerFunc(func(value time.Duration, tags ...metrics.Tag) {
		timer.Record(value, tags...)
		h.write(name, float64(value)/float64(time.Millisecond), tags)
	})
}

func (h *poolMetricsHandler) write(name string, value float64, tags []metrics.Tag) {
	all := h.tags
	if len(tags) > 0 {
		all = maps.Clone(h.tags)
		if all == nil {
			all = make(map[string]string, len(tags))
		}
		for _, tag := range tags {
			all[tag.Key] = tag.Value
		}
	}
	h.file.write(poolMetric{Time: time.Now(), Cluster: h.cluster, Test: h.test, Metric: name, Value: value, Tags: all})
}
