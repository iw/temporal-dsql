package dsql

import (
	"errors"
	"fmt"
)

// ConditionFailedError represents a CAS / fencing condition failure.
// This must not be retried automatically by the retry layer.
type ConditionFailedError struct {
	Msg  string
	Kind ConditionFailedKind
}

type ConditionFailedKind string

const (
	ConditionFailedUnknown   ConditionFailedKind = "unknown"
	ConditionFailedShard     ConditionFailedKind = "shard"
	ConditionFailedExecution ConditionFailedKind = "execution"
	ConditionFailedTaskQueue ConditionFailedKind = "task_queue"
	ConditionFailedNamespace ConditionFailedKind = "namespace"
)

func (e *ConditionFailedError) Error() string {
	if e.Kind == "" || e.Kind == ConditionFailedUnknown {
		return fmt.Sprintf("condition failed: %s", e.Msg)
	}
	return fmt.Sprintf("condition failed (%s): %s", e.Kind, e.Msg)
}

func IsConditionFailedError(err error) bool {
	var cfe *ConditionFailedError
	return errors.As(err, &cfe)
}

// NewConditionFailedError is a convenience constructor.
func NewConditionFailedError(kind ConditionFailedKind, msg string) *ConditionFailedError {
	return &ConditionFailedError{Kind: kind, Msg: msg}
}

// WrapWithConditionFailed wraps an error with a ConditionFailedError, formatting the message with args
func WrapWithConditionFailed(err error, msg string, args ...interface{}) *ConditionFailedError {
	formattedMsg := fmt.Sprintf(msg, args...)
	if err != nil {
		formattedMsg = fmt.Sprintf("%s: %v", formattedMsg, err)
	}
	return NewConditionFailedError(ConditionFailedUnknown, formattedMsg)
}
