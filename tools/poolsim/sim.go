package main

import (
	"container/heap"
	"fmt"
	"time"
)

// Event represents a scheduled event in the simulation.
type Event struct {
	At   time.Time
	Seq  int64
	Name string
	Run  func()
}

// eventQueue implements heap.Interface for priority queue of events.
type eventQueue []*Event

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(i, j int) bool {
	if q[i].At.Equal(q[j].At) {
		return q[i].Seq < q[j].Seq
	}
	return q[i].At.Before(q[j].At)
}

func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *eventQueue) Push(x any) { *q = append(*q, x.(*Event)) }

func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[:n-1]
	return x
}

// Sim is the discrete event simulator.
type Sim struct {
	quiet bool
	now   time.Time
	end   time.Time
	seq   int64
	q     eventQueue
}

// NewSim creates a new simulator with the given start time and duration.
func NewSim(start time.Time, dur time.Duration) *Sim {
	s := &Sim{
		now: start.UTC(),
		end: start.UTC().Add(dur),
		q:   eventQueue{},
	}
	heap.Init(&s.q)
	return s
}

// Now returns the current simulation time.
func (s *Sim) Now() time.Time { return s.now }

// End returns the simulation end time.
func (s *Sim) End() time.Time { return s.end }

// Schedule adds an event at the specified time.
func (s *Sim) Schedule(at time.Time, name string, fn func()) {
	s.seq++
	heap.Push(&s.q, &Event{At: at, Seq: s.seq, Name: name, Run: fn})
}

// After schedules an event relative to the current time.
func (s *Sim) After(d time.Duration, name string, fn func()) {
	s.Schedule(s.now.Add(d), name, fn)
}

// Run executes the simulation until completion or end time.
func (s *Sim) Run() {
	eventCount := 0
	lastReport := s.now
	for s.q.Len() > 0 {
		ev, ok := heap.Pop(&s.q).(*Event)
		if !ok {
			continue
		}
		if ev.At.After(s.end) {
			return
		}
		s.now = ev.At
		ev.Run()
		eventCount++

		// Progress report every simulated minute
		if !s.quiet && s.now.Sub(lastReport) >= time.Minute {
			fmt.Printf("  [sim] t=%s events=%d queue=%d\n", s.now.Format("15:04:05"), eventCount, s.q.Len())
			lastReport = s.now
		}
	}
	if !s.quiet {
		fmt.Printf("  [sim] total events=%d\n", eventCount)
	}
}
