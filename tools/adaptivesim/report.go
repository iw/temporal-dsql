package adaptivesim

import (
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/capacity"
)

// Report writes a run's summary.
func Report(w io.Writer, r *Result) error {
	p := &printer{w: w}
	p.printf("Scenario %s, seed %d: %s of load, then %s to drain\n\n", r.Scenario, r.Seed, r.Duration, r.Drain)
	p.hosts(r)
	p.connections(r)
	p.decisions(r)
	return p.err
}

// printer writes a report, keeping the first error.
type printer struct {
	w   io.Writer
	err error
}

func (p *printer) printf(format string, args ...any) {
	if p.err == nil {
		_, p.err = fmt.Fprintf(p.w, format, args...)
	}
}

func (p *printer) table(header string, rows [][]string) {
	tw := tabwriter.NewWriter(p.w, 0, 0, 2, ' ', 0)
	p.printf("")
	if p.err != nil {
		return
	}
	_, p.err = fmt.Fprintln(tw, header)
	for _, row := range rows {
		if p.err == nil {
			_, p.err = fmt.Fprintln(tw, strings.Join(row, "\t"))
		}
	}
	if p.err == nil {
		p.err = tw.Flush()
	}
	p.printf("\n")
}

func (p *printer) hosts(r *Result) {
	var tasks, calls, pool [][]string
	for _, h := range r.Hosts {
		t := h.Tasks
		if t.Arrived > 0 {
			cleared := "-"
			if t.ReloadArrived > 0 {
				cleared = "never"
				if t.ReloadCleared > 0 {
					cleared = round(t.ReloadCleared).String()
				}
			}
			tasks = append(tasks, []string{
				h.Name, fmt.Sprint(t.Arrived), fmt.Sprint(t.Completed), fmt.Sprint(t.Backlog), cleared,
				round(t.P50).String(), round(t.P99).String(), fmt.Sprint(t.Attempts),
			})
		}
		if wf := h.Workflows; wf.Started > 0 {
			tasks = append(tasks, []string{
				h.Name + " workflows", fmt.Sprint(wf.Started), fmt.Sprint(wf.Completed), "-", "-",
				round(wf.P50).String(), round(wf.P99).String(), fmt.Sprint(wf.StartFailures + wf.ResponseFailures),
			})
		}
		if h.Reads.Tried+h.Writes.Tried+h.Reads.Limited+h.Writes.Limited > 0 {
			calls = append(calls, []string{
				h.Name,
				fmt.Sprintf("%d / %d", h.Reads.OK, h.Reads.Tried),
				fmt.Sprintf("%d / %d", h.Writes.OK, h.Writes.Tried),
				fmt.Sprint(h.Reads.Exhausted + h.Writes.Exhausted),
				fmt.Sprint(h.Reads.Limited + h.Writes.Limited),
				fmt.Sprint(h.Store.Refused), fmt.Sprint(h.Store.CutOff), fmt.Sprint(h.Store.AcquireTimeouts),
			})
		}
		waited := h.Pool.EmptyAcquires + h.Pool.GateWaited
		queued := 0.0
		if h.Pool.Acquires > 0 {
			queued = float64(waited) / float64(h.Pool.Acquires)
		}
		pool = append(pool, []string{
			h.Name, round(h.Started).String(), fmt.Sprint(h.Pool.Opened), fmt.Sprint(h.Pool.Acquires),
			fmt.Sprint(waited), fmt.Sprintf("%.2f", queued), fmt.Sprint(h.Pool.CanceledAcquires), fmt.Sprint(h.Pool.LifetimeDestroys),
		})
		if pl := h.PoolLoad; pl.Requests > 0 {
			p.printf("%s bare statements: %d completed, %d failed, wait p50 %s, p99 %s, max %s\n",
				h.Name, pl.Completed, pl.Failed, pl.WaitP50, pl.WaitP99, pl.WaitMax)
		}
	}
	if len(tasks) > 0 {
		p.table("Host\tArrived\tCompleted\tBacklog at end of load\tReload cleared\tp50\tp99\tAttempts", tasks)
	}
	if len(calls) > 0 {
		p.table("Host\tReads ok / tried\tWrites ok / tried\tResourceExhausted\tRate limited\tGate refusals\tCut off\tAcquire timeouts", calls)
	}
	p.table("Host\tStarted\tConnections opened\tAcquires\tWaited\tQueued share\tCanceled acquires\tLifetime destroys", pool)
}

func (p *printer) connections(r *Result) {
	c := r.Connections
	peakAttempts, peakOpens := 0, 0
	for _, s := range c.PerSecond {
		peakAttempts = max(peakAttempts, s.Dials+s.Cancels)
		peakOpens = max(peakOpens, s.Opens)
	}
	perSecond, cancels, total := c.Attempts(0, r.Duration)
	p.printf("Connections: %d dials and %d cancel requests in all, %d opened, %d rejected (53400) and %d (53300), %d cut at their maximum age\n",
		c.Total.Dials, c.Total.Cancels, c.Total.Opens, c.Total.RejectedRate, c.Total.RejectedCap, c.Total.Cuts)
	p.printf("  during the load: %.2f attempts/s, %d of %d cancel requests; %.2f opens/s over its second half\n",
		perSecond, cancels, total, c.Opens(r.Duration/2, r.Duration))
	p.printf("  peak %d attempts and %d opens in a second; at most %d open\n\n", peakAttempts, peakOpens, c.PeakOpen)
}

func (p *printer) decisions(r *Result) {
	var rows [][]string
	for _, h := range r.Hosts {
		rows = append(rows, decisionRows(h.Name, h.Started, h.Decisions)...)
	}
	if len(rows) > 0 {
		p.table("Host\tController\tMode\tDecisions\tFirst change, after start\tLowest proposed\tHighest proposed\tLast applied\tReasons", rows)
	}
}

// decisionRows summarizes a host's decisions, a row for each controller, and for a
// controller deciding for several keys, for each key.
func decisionRows(host string, started time.Duration, ds Decisions) [][]string {
	var rows [][]string
	for _, s := range ds.series() {
		first := s.decisions[0]
		reasons := make(map[adaptive.Reason]int)
		lowest, highest := first.Proposed, first.Proposed
		change, prev := "-", first.Static
		for _, d := range s.decisions {
			reasons[d.Reason]++
			lowest, highest = min(lowest, d.Proposed), max(highest, d.Proposed)
			if change == "-" && d.Proposed != prev {
				change = fmt.Sprintf("%s at %s", d.Reason, round(d.Time.Sub(epoch)-started))
			}
			prev = d.Proposed
		}
		name := s.controller
		if s.key != "" {
			name += " " + s.key
		}
		last := s.decisions[len(s.decisions)-1]
		rows = append(rows, []string{
			host, name, first.Mode.String(), fmt.Sprint(len(s.decisions)), change,
			number(lowest), number(highest), number(last.Applied), formatReasons(reasons),
		})
	}
	return rows
}

// isCut reports whether a capacity decision with this reason cut the rate.
func isCut(reason adaptive.Reason) bool {
	r := capacity.Reason(reason)
	return r.Overloaded() && r != capacity.ReasonCooldown
}

// number formats a controller's value: a rate or a count to the unit, a multiplier to
// three figures.
func number(v float64) string {
	switch a := math.Abs(v); {
	case a >= 100:
		return fmt.Sprintf("%.0f", v)
	case a >= 10:
		return fmt.Sprintf("%.1f", v)
	default:
		return fmt.Sprintf("%.3g", v)
	}
}

func formatReasons(reasons map[adaptive.Reason]int) string {
	keys := make([]string, 0, len(reasons))
	for reason := range reasons {
		keys = append(keys, string(reason))
	}
	slices.Sort(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s %d", k, reasons[adaptive.Reason(k)])
	}
	return strings.Join(parts, ", ")
}

func round(d time.Duration) time.Duration {
	switch {
	case d >= 10*time.Second:
		return d.Round(time.Second)
	case d >= 10*time.Millisecond:
		return d.Round(time.Millisecond)
	default:
		return d.Round(10 * time.Microsecond)
	}
}
