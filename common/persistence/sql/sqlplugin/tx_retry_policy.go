package sqlplugin

import "time"

// TxRetryPolicy lets a SQL plugin retry a whole transaction that the database
// rejected in a way that is safe to retry, such as a serialization failure
// (SQLSTATE 40001).
type TxRetryPolicy interface {
	// IsRetryableTxError reports whether err, before SqlStore converts it,
	// means the transaction did not commit and can run again.
	IsRetryableTxError(err error) bool
	// TxRetryBackoff returns the delay before retrying operation after its
	// attempt-th failure (attempt starts at 1), or false when no retries remain.
	TxRetryBackoff(operation string, attempt int, err error) (time.Duration, bool)
}
