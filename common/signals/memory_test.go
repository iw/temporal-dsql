package signals

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestCgroupMemory(t *testing.T) {
	for name, tc := range map[string]struct {
		files        fstest.MapFS
		limit, usage uint64
	}{
		"v2 in its own namespace": {
			files: fstest.MapFS{
				"proc/self/cgroup":             {Data: []byte("0::/\n")},
				"sys/fs/cgroup/memory.max":     {Data: []byte("536870912\n")},
				"sys/fs/cgroup/memory.current": {Data: []byte("1000\n")},
			},
			limit: 512 << 20, usage: 1000,
		},
		"v2 under its path": {
			files: fstest.MapFS{
				"proc/self/cgroup":         {Data: []byte("0::/system.slice/temporal.service\n")},
				"sys/fs/cgroup/memory.max": {Data: []byte("max\n")},
				"sys/fs/cgroup/system.slice/temporal.service/memory.max":     {Data: []byte("2147483648\n")},
				"sys/fs/cgroup/system.slice/temporal.service/memory.current": {Data: []byte("4096\n")},
			},
			limit: 2 << 30, usage: 4096,
		},
		"v2 mounted alone": {
			files: fstest.MapFS{
				"proc/self/cgroup":             {Data: []byte("0::/kubepods/burstable/pod1/abc\n")},
				"sys/fs/cgroup/memory.max":     {Data: []byte("268435456\n")},
				"sys/fs/cgroup/memory.current": {Data: []byte("2048\n")},
			},
			limit: 256 << 20, usage: 2048,
		},
		"v2 without a limit": {
			files: fstest.MapFS{
				"proc/self/cgroup":             {Data: []byte("0::/\n")},
				"sys/fs/cgroup/memory.max":     {Data: []byte("max\n")},
				"sys/fs/cgroup/memory.current": {Data: []byte("8192\n")},
			},
			usage: 8192,
		},
		"v1": {
			files: fstest.MapFS{
				"proc/self/cgroup": {Data: []byte("12:pids:/docker/abc\n4:cpuacct,cpu:/docker/abc\n3:memory:/docker/abc\n")},
				"sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes": {Data: []byte("1073741824\n")},
				"sys/fs/cgroup/memory/docker/abc/memory.usage_in_bytes": {Data: []byte("65536\n")},
			},
			limit: 1 << 30, usage: 65536,
		},
		"v1 without a limit": {
			files: fstest.MapFS{
				"proc/self/cgroup":                           {Data: []byte("3:memory:/\n")},
				"sys/fs/cgroup/memory/memory.limit_in_bytes": {Data: []byte("9223372036854771712\n")},
				"sys/fs/cgroup/memory/memory.usage_in_bytes": {Data: []byte("65536\n")},
			},
			usage: 65536,
		},
		"hybrid reads the v1 memory controller": {
			files: fstest.MapFS{
				"proc/self/cgroup":                           {Data: []byte("0::/\n3:memory:/\n")},
				"sys/fs/cgroup/memory.max":                   {Data: []byte("max\n")},
				"sys/fs/cgroup/memory/memory.limit_in_bytes": {Data: []byte("1073741824\n")},
				"sys/fs/cgroup/memory/memory.usage_in_bytes": {Data: []byte("4096\n")},
			},
			limit: 1 << 30, usage: 4096,
		},
		"outside a cgroup": {
			files: fstest.MapFS{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			limit, usage := newCgroupMemory(tc.files).read()
			require.Equal(t, tc.limit, limit)
			require.Equal(t, tc.usage, usage)
		})
	}
}
