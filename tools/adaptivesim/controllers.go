package adaptivesim

import (
	"go.temporal.io/server/common/adaptive/memory"
	"go.temporal.io/server/common/adaptive/taskrate"
	workercount "go.temporal.io/server/common/adaptive/workers"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

// newCollection is a host's dynamic config: its controllers' settings, each its default
// where the scenario leaves it zero.
func newCollection(c Controllers) *dynamicconfig.Collection {
	values := dynamicconfig.StaticClient{}
	if c.TaskRate != (taskrate.Settings{}) {
		values[taskrate.Config.Key()] = c.TaskRate
	}
	if c.WorkerCount != (workercount.Settings{}) {
		values[workercount.Config.Key()] = c.WorkerCount
	}
	if c.Memory != (memory.Settings{}) {
		values[memory.Config.Key()] = c.Memory
	}
	return dynamicconfig.NewCollection(values, log.NewNoopLogger())
}
