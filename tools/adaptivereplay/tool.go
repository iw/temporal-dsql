package adaptivereplay

import (
	"cmp"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

var errNoTraces = errors.New("give one or more trace files")

// overrideFlags collects -set flags.
type overrideFlags []Override

func (o *overrideFlags) String() string {
	parts := make([]string, len(*o))
	for i, override := range *o {
		parts[i] = override.String()
	}
	return strings.Join(parts, " ")
}

func (o *overrideFlags) Set(s string) error {
	override, err := ParseOverride(s)
	if err != nil {
		return err
	}
	*o = append(*o, override)
	return nil
}

// RunTool runs the tool with its command-line arguments.
func RunTool(args []string) error {
	return run(args, os.Stdout)
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("adaptivereplay", flag.ContinueOnError)
	fs.SetOutput(stdout)
	var overrides overrideFlags
	fs.Var(&overrides, "set", "replay with a law's parameter set to a value, as `controller.Field=value`; repeat it for more")
	only := fs.String("controller", "", "replay only the controllers whose names start with `prefix`")
	csvPath := fs.String("csv", "", "write each replayed step to this CSV `file`")
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "Usage: adaptivereplay [flags] trace-file...\n\n"+
			"Replays the signal plane's trace files through the adaptive controllers' laws.\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return errNoTraces
	}
	var include func(string) bool
	if *only != "" {
		include = func(controller string) bool { return strings.HasPrefix(controller, *only) }
	}

	var replays []*Replay
	used := make(map[Override]bool)
	for _, path := range fs.Args() {
		traces, err := readFile(path)
		if err != nil {
			return err
		}
		for _, t := range traces {
			r, err := Run(t, overrides, include, used)
			if err != nil {
				return err
			}
			replays = append(replays, r)
		}
	}
	for _, o := range overrides {
		if !used[o] {
			return fmt.Errorf("%s: no controller in the traces has it", o)
		}
	}
	if err := report(stdout, replays, len(overrides) > 0); err != nil {
		return err
	}
	if *csvPath != "" {
		return writeCSV(*csvPath, replays)
	}
	return nil
}

func readFile(path string) ([]*Trace, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return Read(filepath.Base(path), f)
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

// report writes a table of each recording's series. With overrides, each series has a
// second row that measures its recorded proposals.
func report(w io.Writer, replays []*Replay, overridden bool) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	p := &printer{w: tw}
	for i, r := range replays {
		if i > 0 {
			p.printf("\n")
		}
		p.printf("%s\n", describe(r.Trace))
		if len(r.Series) == 0 && len(r.Unknown) == 0 {
			continue
		}
		p.printf("\n")
		if overridden {
			p.printf("CONTROLLER\tKEY\tSTEPS\tMATCHED\tPROPOSALS\tCHANGES\tREVERSALS\tMIN\tMAX\tFINAL\tREASONS\n")
		} else {
			p.printf("CONTROLLER\tKEY\tSTEPS\tMATCHED\tCHANGES\tREVERSALS\tMIN\tMAX\tFINAL\tREASONS\n")
		}
		for _, s := range r.Series {
			m := s.Replayed()
			p.printf("%s\t%s\t%d\t%d\t", s.Controller, cmp.Or(s.Key, "-"), m.Steps, s.Matched())
			if overridden {
				p.printf("replayed\t")
			}
			p.printf("%s\n", measures(m))
			if overridden {
				p.printf("\t\t\t\trecorded\t%s\n", measures(s.Recorded()))
			}
		}
		for _, controller := range slices.Sorted(maps.Keys(r.Unknown)) {
			p.printf("%s\t-\t%d\tnot replayed\n", controller, r.Unknown[controller])
		}
	}
	if p.err != nil {
		return p.err
	}
	return tw.Flush()
}

func describe(t *Trace) string {
	b := &strings.Builder{}
	b.WriteString(t.File)
	if h := t.Header; h.Service != "" {
		_, _ = fmt.Fprintf(b, ": %s on %s, pid %d", h.Service, h.Host, h.PID)
	}
	if !t.First.IsZero() {
		_, _ = fmt.Fprintf(b, ", %s for %s", t.First.UTC().Format(time.RFC3339), t.Last.Sub(t.First).Round(time.Second))
	}
	_, _ = fmt.Fprintf(b, ", %s, %s", count(t.Ticks, "snapshot"), count(len(t.Decisions), "decision"))
	return b.String()
}

func count(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}

func measures(m Measures) string {
	return fmt.Sprintf("%d\t%d\t%s\t%s\t%s\t%s", m.Changes, m.Reversals, share(m.Min), share(m.Max), share(m.Final), m.reasons())
}

func share(x float64) string {
	if math.IsNaN(x) {
		return "-"
	}
	return strconv.FormatFloat(x, 'f', 3, 64)
}

// writeCSV writes each replayed step as a row.
func writeCSV(path string, replays []*Replay) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"file", "service", "pid", "controller", "key", "time", "static", "recorded", "replayed", "recorded_reason", "replayed_reason", "stepped"})
	for _, r := range replays {
		for _, s := range r.Series {
			for _, step := range s.Steps {
				_ = w.Write([]string{
					r.Trace.File,
					r.Trace.Header.Service,
					strconv.Itoa(r.Trace.Header.PID),
					s.Controller,
					s.Key,
					step.Time.UTC().Format(time.RFC3339Nano),
					number(step.Static),
					number(step.Recorded),
					number(step.Replayed),
					string(step.RecordedReason),
					string(step.ReplayedReason),
					strconv.FormatBool(step.Stepped),
				})
			}
		}
	}
	w.Flush()
	return w.Error()
}

func number(x float64) string {
	return strconv.FormatFloat(x, 'g', -1, 64)
}
