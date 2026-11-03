package adaptivereplay

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/signals"
)

// maxLine is the longest trace line Read accepts.
const maxLine = 16 << 20

type (
	// Trace is one recording in a trace file: the records after a header. The signal plane
	// writes a header each time it opens the file, so a file holds one recording for each
	// time its directory was set.
	Trace struct {
		File   string
		Header signals.Header
		// Ticks is how many snapshots the recording holds, and First and Last are the times
		// of its first and last records.
		Ticks       int
		First, Last time.Time
		Decisions   []Decision
	}

	// Decision is a recorded decision, with its detail still encoded, and how many of the
	// recording's snapshots came before it.
	Decision struct {
		adaptive.Decision
		Detail json.RawMessage
		Tick   int
	}

	// line is one line of a trace file. Only a snapshot's time is read.
	line struct {
		Header   *signals.Header
		Snapshot *struct{ Time time.Time }
		Decision *Decision
	}
)

// Read reads the recordings in a trace file. A process that stops mid-write can leave its
// last line cut short, so a last line that doesn't decode ends the file.
func Read(file string, r io.Reader) ([]*Trace, error) {
	var (
		traces  []*Trace
		badLine error
	)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64<<10), maxLine)
	for n := 1; scanner.Scan(); n++ {
		if badLine != nil {
			return nil, badLine
		}
		var l line
		if err := json.Unmarshal(scanner.Bytes(), &l); err != nil {
			badLine = fmt.Errorf("%s: line %d: %w", file, n, err)
			continue
		}
		if l.Header != nil {
			traces = append(traces, &Trace{File: file, Header: *l.Header})
			continue
		}
		if len(traces) == 0 {
			traces = append(traces, &Trace{File: file})
		}
		t := traces[len(traces)-1]
		switch {
		case l.Snapshot != nil:
			t.Ticks++
			t.observe(l.Snapshot.Time)
		case l.Decision != nil:
			l.Decision.Tick = t.Ticks
			t.Decisions = append(t.Decisions, *l.Decision)
			t.observe(l.Decision.Time)
		default:
			// A record of a kind the tool doesn't know.
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	return traces, nil
}

func (t *Trace) observe(at time.Time) {
	if t.First.IsZero() || at.Before(t.First) {
		t.First = at
	}
	if at.After(t.Last) {
		t.Last = at
	}
}
