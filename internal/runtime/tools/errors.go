package tools

import (
	"errors"

	runtimerterr "github.com/division-sh/swarm/internal/runtime/rterrors"
)

type RuntimeError = runtimerterr.RuntimeError

var (
	ErrToolNotAllowed     = errors.New("tools: tool not allowed for agent")
	ErrUnknownEntityType  = errors.New("tools: unknown entity type")
	ErrUnknownEntityField = errors.New("tools: unknown entity field")
)

func WrapRuntimeError(code, component, operation string, retryable bool, cause error, format string, args ...any) error {
	return runtimerterr.WrapRuntimeError(code, component, operation, retryable, cause, format, args...)
}

func NewRuntimeError(code, component, operation string, retryable bool, format string, args ...any) error {
	return runtimerterr.NewRuntimeError(code, component, operation, retryable, format, args...)
}

func AsRuntimeError(err error) (*RuntimeError, bool) {
	return runtimerterr.AsRuntimeError(err)
}

func FormatRuntimeError(err error) string {
	return runtimerterr.FormatRuntimeError(err)
}
