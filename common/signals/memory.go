package signals

import (
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
)

const (
	cgroupV1Memory = "sys/fs/cgroup/memory"
	cgroupV2       = "sys/fs/cgroup"
	// cgroup v1 reports no limit as a page-rounded MaxInt64.
	cgroupV1Unlimited = 1 << 62
)

// cgroupMemory reads the memory limit and use of the process's cgroup, from a file
// system rooted at "/".
type cgroupMemory struct {
	fsys                 fs.FS
	limitPath, usagePath string // empty outside a cgroup with a memory controller
}

func newCgroupMemory(fsys fs.FS) *cgroupMemory {
	m := &cgroupMemory{fsys: fsys}
	data, err := fs.ReadFile(fsys, "proc/self/cgroup")
	if err != nil {
		return m
	}
	var v2Path string
	for line := range strings.SplitSeq(string(data), "\n") {
		// Each line is hierarchy-ID:controllers:path.
		fields := strings.SplitN(line, ":", 3)
		if len(fields) != 3 {
			continue
		}
		// On a hybrid host the v1 memory controller sets the limit, so it comes first.
		if slices.Contains(strings.Split(fields[1], ","), "memory") {
			if m.find(cgroupV1Memory, fields[2], "memory.limit_in_bytes", "memory.usage_in_bytes") {
				return m
			}
		}
		if fields[0] == "0" && fields[1] == "" {
			v2Path = fields[2]
		}
	}
	if v2Path != "" {
		m.find(cgroupV2, v2Path, "memory.max", "memory.current")
	}
	return m
}

// find looks for the cgroup's files under its path, then at the mount's root, where a
// container that has its own cgroup namespace, or its cgroup mounted alone, finds them.
func (m *cgroupMemory) find(mount, cgroupPath, limitFile, usageFile string) bool {
	for _, dir := range []string{path.Join(mount, strings.TrimPrefix(cgroupPath, "/")), mount} {
		if _, err := fs.Stat(m.fsys, path.Join(dir, limitFile)); err == nil {
			m.limitPath, m.usagePath = path.Join(dir, limitFile), path.Join(dir, usageFile)
			return true
		}
	}
	return false
}

func (m *cgroupMemory) read() (limit, usage uint64) {
	if m.limitPath == "" {
		return 0, 0
	}
	limit = m.readBytes(m.limitPath)
	if limit >= cgroupV1Unlimited {
		limit = 0
	}
	return limit, m.readBytes(m.usagePath)
}

// readBytes reads a byte count, or 0 for "max" or a file it can't read.
func (m *cgroupMemory) readBytes(name string) uint64 {
	data, err := fs.ReadFile(m.fsys, name)
	if err != nil {
		return 0
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return v
}
