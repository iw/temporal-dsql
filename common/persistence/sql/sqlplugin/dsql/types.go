package dsql

// ErrorType represents the classification of DSQL errors.
type ErrorType int

const (
	ErrorTypeUnknown ErrorType = iota
	ErrorTypeRetryable
	ErrorTypeConditionFailed
	ErrorTypeUnsupportedFeature
	ErrorTypePermanent
	ErrorTypeConnectionLimit    // DSQL cluster connection limit reached
	ErrorTypeTransactionTimeout // DSQL transaction timeout (5 min limit)
)

func (e ErrorType) String() string {
	switch e {
	case ErrorTypeRetryable:
		return "retryable"
	case ErrorTypeConditionFailed:
		return "condition_failed"
	case ErrorTypeUnsupportedFeature:
		return "unsupported_feature"
	case ErrorTypePermanent:
		return "permanent"
	case ErrorTypeConnectionLimit:
		return "connection_limit"
	case ErrorTypeTransactionTimeout:
		return "transaction_timeout"
	default:
		return "unknown"
	}
}
