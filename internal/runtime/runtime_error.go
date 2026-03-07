package runtime

import runtimerterr "empireai/internal/runtime/rterrors"

func WrapRuntimeError(code, component, operation string, retryable bool, cause error, format string, args ...any) error {
	return runtimerterr.WrapRuntimeError(code, component, operation, retryable, cause, format, args...)
}

func NewRuntimeError(code, component, operation string, retryable bool, format string, args ...any) error {
	return runtimerterr.NewRuntimeError(code, component, operation, retryable, format, args...)
}

func AsRuntimeError(err error) (*runtimerterr.RuntimeError, bool) {
	return runtimerterr.AsRuntimeError(err)
}

func FormatRuntimeError(err error) string {
	return runtimerterr.FormatRuntimeError(err)
}
