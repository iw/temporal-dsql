package testcore

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHistogramChange(t *testing.T) {
	change := histogramChange{
		buckets: []float64{0, 0.001, 0.01, 0.1, 1, math.Inf(1)},
		counts:  []uint64{90, 8, 1, 0, 1},
	}
	require.Equal(t, uint64(100), change.total())
	require.InDelta(t, 0.001, change.quantile(0.5), 1e-12)
	require.InDelta(t, 0.1, change.quantile(0.99), 1e-12)
	require.InDelta(t, 1, change.quantile(1), 1e-12, "the unbounded bucket gives its lower boundary")
	require.InDelta(t, 1, change.largest(), 1e-12)
	require.Equal(t, uint64(1), change.atLeast(0.1))
	require.Equal(t, uint64(2), change.atLeast(0.01))
	require.InDelta(t, 90*0.0005+8*0.0055+0.055+1, change.sum(), 1e-12)

	lowest := histogramChange{buckets: []float64{math.Inf(-1), 0.5, 1}, counts: []uint64{2, 0}}
	require.InDelta(t, 0.5, lowest.largest(), 1e-12)
	require.InDelta(t, 1, lowest.sum(), 1e-12, "the bucket below its upper boundary counts at that boundary")

	none := histogramChange{buckets: []float64{0, 1}, counts: []uint64{0}}
	require.Zero(t, none.quantile(0.99))
	require.Zero(t, none.largest())
	require.Zero(t, none.sum())
}

func TestReadProcFiles(t *testing.T) {
	root := t.TempDir()
	writeProcFiles(t, root, procState{cpuTicks: [2]int{250, 50}, residentPages: 1000, cpuSome: 12345, ioFull: 678, outSegs: 2000, retrans: 7})

	cpu, err := readProcessCPU(filepath.Join(root, "self", "stat"))
	require.NoError(t, err)
	require.InDelta(t, 3.0, cpu, 1e-12)

	pages, err := readResidentPages(filepath.Join(root, "self", "statm"))
	require.NoError(t, err)
	require.InDelta(t, 1000.0, pages, 1e-12)

	cpuPressure, err := readPressure(filepath.Join(root, "pressure", "cpu"))
	require.NoError(t, err)
	require.Equal(t, map[string]float64{"some": 12345, "full": 0}, cpuPressure)
	ioPressure, err := readPressure(filepath.Join(root, "pressure", "io"))
	require.NoError(t, err)
	require.Equal(t, map[string]float64{"some": 0, "full": 678}, ioPressure)

	tcp, err := readTCPCounters(filepath.Join(root, "net", "snmp"))
	require.NoError(t, err)
	require.InDelta(t, 2000.0, tcp["OutSegs"], 1e-12)
	require.InDelta(t, 7.0, tcp["RetransSegs"], 1e-12)
	require.InDelta(t, -1.0, tcp["MaxConn"], 1e-12)

	_, err = readProcessCPU(filepath.Join(root, "missing"))
	require.Error(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "bad"), []byte("1234 S 1"), 0o644))
	_, err = readProcessCPU(filepath.Join(root, "bad"))
	require.Error(t, err)
	_, err = readTCPCounters(filepath.Join(root, "bad"))
	require.Error(t, err)
}

func TestProcessSampler(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(t.TempDir(), "pools.jsonl")
	file, err := openPoolMetricsFile(path)
	require.NoError(t, err)
	sampler := newProcessSampler(file, root, 5*time.Second)
	sampler.pageSize = 4096

	start := time.Now()
	writeProcFiles(t, root, procState{cpuTicks: [2]int{100, 20}, residentPages: 256, cpuSome: 1_000, ioFull: 0, outSegs: 500, retrans: 1})
	sampler.sample(start)
	first := readProcessSamples(t, path)
	require.InDelta(t, 1.0, first["process_rss_mb"], 1e-12)
	require.Positive(t, first["process_goroutines"])
	require.Positive(t, first["process_gomaxprocs"])
	for _, name := range []string{"process_sample_late_ms", "process_cpu_seconds", "host_pressure_cpu_some_ms", "process_sched_latency_max_ms"} {
		require.NotContains(t, first, name, "a counter or histogram needs an earlier sample")
	}

	writeProcFiles(t, root, procState{cpuTicks: [2]int{400, 70}, residentPages: 512, cpuSome: 251_000, ioFull: 2_000, outSegs: 900, retrans: 4})
	sampler.sample(start.Add(7 * time.Second))
	both := readProcessSamples(t, path)
	require.InDelta(t, 2000.0, both["process_sample_late_ms"], 1e-6)
	require.InDelta(t, 3.5, both["process_cpu_seconds"], 1e-12)
	require.InDelta(t, 2.0, both["process_rss_mb"], 1e-12)
	require.InDelta(t, 250.0, both["host_pressure_cpu_some_ms"], 1e-12)
	require.InDelta(t, 2.0, both["host_pressure_io_full_ms"], 1e-12)
	require.InDelta(t, 0.0, both["host_pressure_memory_some_ms"], 1e-12)
	require.InDelta(t, 400.0, both["host_tcp_out_segs"], 1e-12)
	require.InDelta(t, 3.0, both["host_tcp_retrans_segs"], 1e-12)
	for _, name := range []string{
		"process_go_cpu_user_seconds", "process_go_cpu_gc_seconds", "process_go_cpu_idle_seconds", "process_gc_cycles",
		"process_gc_pause_ms", "process_mutex_wait_seconds", "process_sched_latency_p99_ms", "process_sched_latency_max_ms",
		"process_sched_waits_over_100ms", "process_gc_pause_max_ms", "process_other_pauses", "process_other_pause_ms",
		"process_other_pause_max_ms", "process_goroutines_runnable", "process_threads", "process_heap_live_mb", "process_memory_mb",
	} {
		require.Contains(t, both, name)
		require.GreaterOrEqual(t, both[name], 0.0, name)
	}
}

func TestProcessSamplerWithoutProcFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pools.jsonl")
	file, err := openPoolMetricsFile(path)
	require.NoError(t, err)
	sampler := newProcessSampler(file, filepath.Join(t.TempDir(), "missing"), 5*time.Second)

	start := time.Now()
	sampler.sample(start)
	sampler.sample(start.Add(5 * time.Second))
	samples := readProcessSamples(t, path)
	require.InDelta(t, 0.0, samples["process_sample_late_ms"], 1e-12)
	require.Contains(t, samples, "process_sched_latency_max_ms")
	for name := range samples {
		require.False(t, strings.HasPrefix(name, "host_"), name)
		require.NotEqual(t, "process_cpu_seconds", name)
		require.NotEqual(t, "process_rss_mb", name)
	}
}

type procState struct {
	cpuTicks      [2]int
	residentPages int
	cpuSome       int
	ioFull        int
	outSegs       int
	retrans       int
}

// writeProcFiles writes the files a process sampler reads under root, as Linux lays them out
// under /proc.
func writeProcFiles(t *testing.T, root string, state procState) {
	t.Helper()
	itoa := strconv.Itoa
	files := map[string]string{
		"self/stat": "1234 (functional test) S 1 1234 1234 0 -1 4194560 100 0 0 0 " +
			itoa(state.cpuTicks[0]) + " " + itoa(state.cpuTicks[1]) + " 0 0 20 0 30 0 100 1000 200\n",
		"self/statm":      "5000 " + itoa(state.residentPages) + " 100 10 0 400 0\n",
		"pressure/cpu":    "some avg10=0.00 avg60=0.00 avg300=0.00 total=" + itoa(state.cpuSome) + "\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
		"pressure/memory": "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
		"pressure/io":     "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=" + itoa(state.ioFull) + "\n",
		"net/snmp": "Ip: Forwarding DefaultTTL\nIp: 1 64\n" +
			"Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors\n" +
			"Tcp: 1 200 120000 -1 10 2 0 0 5 1000 " + itoa(state.outSegs) + " " + itoa(state.retrans) + " 0 3 0\n" +
			"Udp: InDatagrams NoPorts\nUdp: 10 0\n",
	}
	for name, content := range files {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
}

// readProcessSamples returns the latest value of each process and host metric in the file,
// and checks that their lines name no cluster or test.
func readProcessSamples(t *testing.T, path string) map[string]float64 {
	t.Helper()
	opened, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, opened.Close()) }()
	samples := make(map[string]float64)
	scanner := bufio.NewScanner(opened)
	for scanner.Scan() {
		var line poolMetric
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line))
		require.Empty(t, line.Cluster)
		require.Empty(t, line.Test)
		require.Empty(t, line.Tags)
		samples[line.Metric] = line.Value
	}
	require.NoError(t, scanner.Err())
	return samples
}
