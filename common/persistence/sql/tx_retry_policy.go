package sql

import (
	"time"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

var _ sqlplugin.TxRetryPolicy = (*DbConn)(nil)

// IsRetryableTxError forwards the optional hook through the factory's reference-counted connection.
func (c *DbConn) IsRetryableTxError(err error) bool {
	policy, ok := c.DB.(sqlplugin.TxRetryPolicy)
	return ok && policy.IsRetryableTxError(err)
}

func (c *DbConn) TxRetryBackoff(operation string, attempt int, err error) (time.Duration, bool) {
	if policy, ok := c.DB.(sqlplugin.TxRetryPolicy); ok {
		return policy.TxRetryBackoff(operation, attempt, err)
	}
	return 0, false
}
