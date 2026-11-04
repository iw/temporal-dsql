package sql

import (
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

var _ ExecutionStoreCreator = (*DbConn)(nil)

// NewExecutionStore forwards the optional hook through the factory's reference-counted
// connection, so a plugin that provides an execution store has it used.
func (c *DbConn) NewExecutionStore(baseStore p.ExecutionStore, logger log.Logger) p.ExecutionStore {
	if creator, ok := c.DB.(ExecutionStoreCreator); ok {
		return creator.NewExecutionStore(baseStore, logger)
	}
	return baseStore
}
