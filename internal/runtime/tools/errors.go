package tools

import runtimerterr "empireai/internal/runtime/rterrors"

type RuntimeError = runtimerterr.RuntimeError

func WrapRuntimeError(code, component, operation string, retryable bool, cause error, format string, args ...any) error {
	return runtimerterr.WrapRuntimeError(code, component, operation, retryable, cause, format, args...)
}

func NewRuntimeError(code, component, operation string, retryable bool, format string, args ...any) error {
	return runtimerterr.NewRuntimeError(code, component, operation, retryable, format, args...)
}

func FormatRuntimeError(err error) string {
	return runtimerterr.FormatRuntimeError(err)
}
