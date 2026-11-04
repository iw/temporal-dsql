package adaptivesim

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/memory"
	"go.temporal.io/server/common/adaptive/taskrate"
	workercount "go.temporal.io/server/common/adaptive/workers"
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
	var o overrides
	fs.StringVar(&o.capacity, "capacity", "", "run the capacity controller in this `mode`: disabled, shadow or enabled")
	fs.DurationVar(&o.grace, "queued-grace", -1, "set the capacity controller's QueuedGrace to this `duration`")
	fs.StringVar(&o.taskRate, "task-rate", "", "run the task scheduler rate controller in this `mode`")
	fs.StringVar(&o.workers, "workers", "", "run the worker count controllers in this `mode`")
	fs.StringVar(&o.memory, "memory", "", "run the memory budget controllers in this `mode`")
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
	if err := o.apply(&sc); err != nil {
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

// overrides are the controllers' settings the command line sets.
type overrides struct {
	capacity, taskRate, workers, memory string
	grace                               time.Duration
}

func (o overrides) apply(sc *Scenario) error {
	params := &sc.Host.Capacity.Params
	if o.capacity != "" {
		if err := checkMode("capacity", o.capacity); err != nil {
			return err
		}
		params.Enabled = true
		params.Capacity.Mode = o.capacity
	}
	if o.grace >= 0 {
		params.Capacity.QueuedGrace = o.grace
	}
	c := &sc.Host.Controllers
	if o.taskRate != "" {
		if err := checkMode("task-rate", o.taskRate); err != nil {
			return err
		}
		if c.TaskRate == (taskrate.Settings{}) {
			c.TaskRate = taskrate.DefaultSettings
		}
		c.TaskRate.Mode = o.taskRate
	}
	if o.workers != "" {
		if err := checkMode("workers", o.workers); err != nil {
			return err
		}
		if c.WorkerCount == (workercount.Settings{}) {
			c.WorkerCount = workercount.DefaultSettings
		}
		c.WorkerCount.Mode = o.workers
	}
	if o.memory != "" {
		if err := checkMode("memory", o.memory); err != nil {
			return err
		}
		if c.Memory == (memory.Settings{}) {
			c.Memory = memory.DefaultSettings
		}
		c.Memory.Mode = o.memory
	}
	return nil
}

func checkMode(controller, mode string) error {
	switch mode {
	case adaptive.ModeNameDisabled, adaptive.ModeNameShadow, adaptive.ModeNameEnabled:
		return nil
	default:
		return fmt.Errorf("%s mode %q isn't disabled, shadow or enabled", controller, mode)
	}
}
