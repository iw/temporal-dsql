package signals

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	// Record is one line of a trace: a header, which starts each file, a snapshot or a
	// decision. Durations are in nanoseconds.
	Record struct {
		Header   *Header            `json:",omitempty"`
		Snapshot *Snapshot          `json:",omitempty"`
		Decision *adaptive.Decision `json:",omitempty"`
	}

	// Header names the process a trace file comes from.
	Header struct {
		Service string
		Host    string
		PID     int
		Start   time.Time
	}

	// traceWriter writes a service's trace to one file per process. It isn't safe for
	// concurrent use.
	traceWriter struct {
		header Header
		logger log.Logger

		dir      string // the directory of the open file, if any
		failed   string // a directory that couldn't be opened, so it isn't retried
		file     *os.File
		buf      *bufio.Writer
		maxBytes int64
		written  int64
		full     bool
	}
)

func newTraceWriter(header Header, logger log.Logger) *traceWriter {
	return &traceWriter{header: header, logger: logger}
}

// configure opens, switches or closes the file to match the settings.
func (w *traceWriter) configure(dir string, maxBytes int64, now time.Time) {
	if maxBytes != w.maxBytes {
		w.maxBytes, w.full = maxBytes, false
	}
	if dir == w.dir || (dir != "" && dir == w.failed) {
		return
	}
	w.close()
	w.failed = ""
	if dir == "" {
		return
	}
	name := filepath.Join(dir, fmt.Sprintf("%s-%s-%d.jsonl", w.header.Service, w.header.Host, w.header.PID))
	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		w.logger.Error("Signal plane can't open its trace file.", tag.Error(err), tag.NewStringTag("path", name))
		w.failed = dir
		return
	}
	info, err := file.Stat()
	if err == nil {
		w.written = info.Size()
	}
	w.dir, w.file, w.buf, w.full = dir, file, bufio.NewWriterSize(file, 64<<10), false
	header := w.header
	header.Start = now
	w.write(Record{Header: &header})
}

func (w *traceWriter) write(rec Record) {
	if w.file == nil || w.full {
		return
	}
	line, err := json.Marshal(rec)
	if err != nil {
		w.logger.Error("Signal plane can't encode a trace record.", tag.Error(err))
		return
	}
	line = append(line, '\n')
	if w.maxBytes > 0 && w.written+int64(len(line)) > w.maxBytes {
		w.full = true
		w.logger.Warn("Signal plane's trace file reached TraceMaxBytes; it records no more.",
			tag.NewStringTag("path", w.file.Name()), tag.NewInt64("maxBytes", w.maxBytes))
		return
	}
	if _, err := w.buf.Write(line); err != nil {
		w.fail(err)
		return
	}
	w.written += int64(len(line))
}

func (w *traceWriter) flush() {
	if w.buf == nil {
		return
	}
	if err := w.buf.Flush(); err != nil {
		w.fail(err)
	}
}

// fail closes a file that can't be written, and keeps its directory from being reopened
// until the setting changes.
func (w *traceWriter) fail(err error) {
	w.logger.Error("Signal plane can't write its trace file.", tag.Error(err))
	dir := w.dir
	w.close()
	w.failed = dir
}

func (w *traceWriter) close() {
	if w.file == nil {
		return
	}
	_ = w.buf.Flush()
	if err := w.file.Close(); err != nil {
		w.logger.Error("Signal plane can't close its trace file.", tag.Error(err))
	}
	w.dir, w.file, w.buf, w.written, w.full = "", nil, nil, 0, false
}
