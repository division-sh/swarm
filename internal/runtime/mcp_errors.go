package runtime

import (
	runtimemcp "empireai/internal/runtime/mcp"
)

const (
	ErrCodeMCPAuthMissingBearer = runtimemcp.ErrCodeAuthMissingBearer
	ErrCodeMCPAuthInvalidBearer = runtimemcp.ErrCodeAuthInvalidBearer
	ErrCodeMCPContextMissing    = runtimemcp.ErrCodeContextMissing
	ErrCodeMCPContextNotFound   = runtimemcp.ErrCodeContextNotFound
	ErrCodeMCPContextStale      = runtimemcp.ErrCodeContextStale
	ErrCodeMCPActorMissing      = runtimemcp.ErrCodeActorMissing
	ErrCodeMCPToolNotAllowed    = runtimemcp.ErrCodeToolNotAllowed
	ErrCodeMCPToolExecFailed    = runtimemcp.ErrCodeToolExecFailed
	ErrCodeMCPInvalidRequest    = runtimemcp.ErrCodeInvalidRequest
	ErrCodeMCPStallDetected     = runtimemcp.ErrCodeStallDetected
)

func newMCPRuntimeError(code, operation string, retryable bool, cause error, format string, args ...any) error {
	return WrapRuntimeError(code, "mcp-gateway", operation, retryable, cause, format, args...)
}

func runtimeErrorCodeFromText(raw string) string {
	return runtimemcp.RuntimeErrorCodeFromText(raw)
}

func runtimeErrorEnvelope(raw string) string {
	return runtimemcp.RuntimeErrorEnvelope(raw)
}
