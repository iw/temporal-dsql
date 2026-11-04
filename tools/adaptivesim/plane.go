package adaptivesim

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/signals"
)

// traceService is the service a host's trace names.
const traceService = "history"

type (
	// plane is a host's signal plane: a snapshot of its persistence health every tick, and
	// its controllers' decisions, kept for the run's result and written to a trace in the
	// signal plane's format (common/signals/trace.go), so tools/adaptivereplay can replay
	// the run.
	plane struct {
		l         *loop
		interval  time.Duration
		windows   int
		health    *health
		trace     *traceFile
		decisions []adaptive.Decision
	}

	traceFile struct {
		file *os.File
		buf  *bufio.Writer
		err  error
	}
)

func newPlane(l *loop, cfg Signals, h *health, trace *traceFile) *plane {
	return &plane{l: l, interval: cfg.PlaneInterval, windows: cfg.PlaneWindows, health: h, trace: trace}
}

func (pl *plane) start() {
	if pl.interval > 0 {
		pl.l.after(pl.interval, pl.tick)
	}
}

func (pl *plane) tick() {
	snap := &signals.Snapshot{Time: pl.l.now, Persistence: pl.health.snapshot(pl.l.now, max(1, pl.windows))}
	pl.trace.write(signals.Record{Snapshot: snap})
	pl.l.after(pl.interval, pl.tick)
}

// recordDecision keeps a controller's decision, and writes it to the trace.
func (pl *plane) recordDecision(d adaptive.Decision) {
	pl.decisions = append(pl.decisions, d)
	pl.trace.write(signals.Record{Decision: &d})
}

// openTrace opens a host's trace file in dir, named as the signal plane names its files,
// and writes its header.
func openTrace(dir string, header signals.Header) (*traceFile, error) {
	name := filepath.Join(dir, fmt.Sprintf("%s-%s-%d.jsonl", header.Service, header.Host, header.PID))
	file, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	t := &traceFile{file: file, buf: bufio.NewWriterSize(file, 64<<10)}
	t.write(signals.Record{Header: &header})
	return t, t.err
}

// write writes a record. A nil trace writes nothing.
func (t *traceFile) write(rec signals.Record) {
	if t == nil || t.err != nil {
		return
	}
	line, err := json.Marshal(rec)
	if err == nil {
		line = append(line, '\n')
		_, err = t.buf.Write(line)
	}
	t.err = err
}

func (t *traceFile) close() error {
	if t == nil {
		return nil
	}
	if err := t.buf.Flush(); err != nil && t.err == nil {
		t.err = err
	}
	if err := t.file.Close(); err != nil && t.err == nil {
		t.err = err
	}
	return t.err
}
