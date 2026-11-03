package adaptivereplay

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/signals"
)

func TestReadsEachRecordingInAFile(t *testing.T) {
	rec := &recording{}
	rec.decision(t, adaptive.Decision{Controller: "before_a_header", Time: start.Add(-time.Minute)})
	for pid := 1; pid <= 2; pid++ {
		opened := start.Add(time.Duration(pid) * time.Hour)
		rec.write(t, signals.Record{Header: &signals.Header{Service: "history", Host: "host", PID: pid, Start: opened}})
		for i := range 3 {
			rec.write(t, signals.Record{Snapshot: &signals.Snapshot{Time: opened.Add(time.Duration(i) * time.Second)}})
			rec.decision(t, adaptive.Decision{Controller: "c", Time: opened.Add(time.Duration(i) * time.Second), Detail: map[string]int{"Step": i}})
		}
	}

	traces, err := Read("trace.jsonl", &rec.buf)
	require.NoError(t, err)
	require.Len(t, traces, 3)

	require.Equal(t, signals.Header{}, traces[0].Header, "records before a header have none")
	require.Len(t, traces[0].Decisions, 1)
	require.Zero(t, traces[0].Ticks)

	for i, tr := range traces[1:] {
		opened := start.Add(time.Duration(i+1) * time.Hour)
		require.Equal(t, "trace.jsonl", tr.File)
		require.Equal(t, i+1, tr.Header.PID)
		require.Equal(t, 3, tr.Ticks)
		require.True(t, opened.Equal(tr.First))
		require.True(t, opened.Add(2*time.Second).Equal(tr.Last))
		require.Len(t, tr.Decisions, 3)
		for j, d := range tr.Decisions {
			require.Equal(t, j+1, d.Tick, "a decision follows its tick's snapshot")
			require.JSONEq(t, fmt.Sprintf(`{"Step":%d}`, j), string(d.Detail))
		}
	}
}

func TestDescribesARecording(t *testing.T) {
	tr := &Trace{File: "trace.jsonl", Ticks: 1, Decisions: make([]Decision, 2)}
	require.Equal(t, "trace.jsonl, 1 snapshot, 2 decisions", describe(tr))
	tr.Header = signals.Header{Service: "matching", Host: "host", PID: 7}
	tr.First, tr.Last, tr.Ticks, tr.Decisions = start, start.Add(90*time.Second), 0, tr.Decisions[:1]
	require.Equal(t, "trace.jsonl: matching on host, pid 7, 2023-11-14T22:13:20Z for 1m30s, 0 snapshots, 1 decision", describe(tr))
}

func TestReadAcceptsACutShortLastLine(t *testing.T) {
	rec := &recording{}
	rec.decision(t, adaptive.Decision{Controller: "c", Time: start})
	rec.buf.WriteString(`{"Decision":{"Controller":"c","Ti`)

	traces, err := Read("trace.jsonl", &rec.buf)
	require.NoError(t, err)
	require.Len(t, traces, 1)
	require.Len(t, traces[0].Decisions, 1)
}

func TestReadRejectsABadLineBeforeTheLast(t *testing.T) {
	rec := &recording{}
	rec.decision(t, adaptive.Decision{Controller: "c", Time: start})
	rec.buf.WriteString("not json\n")
	rec.decision(t, adaptive.Decision{Controller: "c", Time: start.Add(time.Second)})

	_, err := Read("trace.jsonl", &rec.buf)
	require.ErrorContains(t, err, "trace.jsonl: line 2: ")
}

func TestReadRejectsALineLongerThanItsLimit(t *testing.T) {
	_, err := Read("trace.jsonl", strings.NewReader(`{"Decision":{"Controller":"`+strings.Repeat("c", maxLine)+`"}}`+"\n"))
	require.ErrorContains(t, err, "trace.jsonl: ")
}
