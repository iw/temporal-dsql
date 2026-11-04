package adaptivesim

import (
	"fmt"
	"io"
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
	p.capacity(r)
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

func (p *printer) capacity(r *Result) {
	var rows [][]string
	for _, h := range r.Hosts {
		if len(h.Decisions) == 0 {
			continue
		}
		reasons := make(map[adaptive.Reason]int)
		minApplied, minProposed := h.Decisions[0].Applied, h.Decisions[0].Proposed
		firstCut := "-"
		for _, d := range h.Decisions {
			reasons[d.Reason]++
			minApplied, minProposed = min(minApplied, d.Applied), min(minProposed, d.Proposed)
			if firstCut == "-" && isCut(d.Reason) {
				firstCut = fmt.Sprintf("%s at %s", d.Reason, round(d.Time.Sub(epoch)-h.Started))
			}
		}
		rows = append(rows, []string{
			h.Name, h.Decisions[0].Mode.String(), fmt.Sprint(len(h.Decisions)), firstCut,
			fmt.Sprintf("%.0f", minProposed), fmt.Sprintf("%.0f", minApplied), formatReasons(reasons),
		})
	}
	if len(rows) > 0 {
		p.table("Host\tCapacity mode\tSteps\tFirst cut, after start\tLowest proposed\tLowest applied\tReasons", rows)
	}
}

// isCut reports whether a capacity decision with this reason cut the rate.
func isCut(reason adaptive.Reason) bool {
	r := capacity.Reason(reason)
	return r.Overloaded() && r != capacity.ReasonCooldown
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
