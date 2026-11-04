package adaptivesim

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"go.temporal.io/server/common/adaptive"
)

// RunTool runs the tool with its command-line arguments.
func RunTool(args []string) error {
	return run(args, os.Stdout)
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("adaptivesim", flag.ContinueOnError)
	fs.SetOutput(stdout)
	name := fs.String("scenario", "", "the `scenario` to run; -list lists them")
	variantName := fs.String("variant", "", "the scenario's `variant`; its first by default")
	list := fs.Bool("list", false, "list the scenarios and their variants")
	seed := fs.Uint64("seed", 0, "the run's random `seed`, in place of the scenario's")
	traceDir := fs.String("trace", "", "write each host's signal plane trace to this `directory`, for tools/adaptivereplay")
	mode := fs.String("capacity", "", "run the capacity controller in this `mode`: disabled, shadow or enabled")
	grace := fs.Duration("queued-grace", -1, "set the capacity controller's QueuedGrace to this `duration`")
	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "Usage: adaptivesim -scenario name [flags]\n\n"+
			"Simulates history's adaptive plane on a coarse model of its DSQL store.\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *list {
		_, err := fmt.Fprint(stdout, describe())
		return err
	}
	if *name == "" {
		fs.Usage()
		return errors.New("give a scenario")
	}
	sc, err := Lookup(*name, *variantName)
	if err != nil {
		return err
	}
	if *seed != 0 {
		sc.Seed = *seed
	}
	if err := override(&sc, *mode, *grace); err != nil {
		return err
	}
	if *traceDir != "" {
		if err := os.MkdirAll(*traceDir, 0o755); err != nil {
			return err
		}
	}
	started := time.Now()
	result, err := Run(sc, Options{TraceDir: *traceDir})
	if err != nil {
		return err
	}
	if err := Report(stdout, result); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "Simulated in %s.\n", time.Since(started).Round(time.Millisecond))
	return err
}

func override(sc *Scenario, mode string, grace time.Duration) error {
	params := &sc.Host.Capacity.Params
	switch mode {
	case "":
	case adaptive.ModeNameDisabled, adaptive.ModeNameShadow, adaptive.ModeNameEnabled:
		params.Enabled = true
		params.Capacity.Mode = mode
	default:
		return fmt.Errorf("capacity mode %q isn't disabled, shadow or enabled", mode)
	}
	if grace >= 0 {
		params.Capacity.QueuedGrace = grace
	}
	return nil
}
