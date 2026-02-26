package runtime

import (
	"fmt"
	"strings"
)

const (
	ErrCodeMCPAuthMissingBearer = "mcp_auth_missing_bearer"
	ErrCodeMCPAuthInvalidBearer = "mcp_auth_invalid_bearer"
	ErrCodeMCPContextMissing    = "mcp_context_token_missing"
	ErrCodeMCPContextNotFound   = "mcp_context_token_not_found"
	ErrCodeMCPContextStale      = "mcp_context_token_stale_epoch"
	ErrCodeMCPActorMissing      = "mcp_actor_missing"
	ErrCodeMCPToolNotAllowed    = "mcp_tool_not_allowed"
	ErrCodeMCPToolExecFailed    = "mcp_tool_execution_failed"
	ErrCodeMCPInvalidRequest    = "mcp_invalid_request"
	ErrCodeMCPStallDetected     = "mcp_stall_detected"
)

func newMCPRuntimeError(code, operation string, retryable bool, cause error, format string, args ...any) error {
	return WrapRuntimeError(code, "mcp-gateway", operation, retryable, cause, format, args...)
}

func runtimeErrorCodeFromText(raw string) string {
	meta := strings.TrimSpace(raw)
	if meta == "" || !strings.HasPrefix(meta, "runtime_error") {
		return ""
	}
	if idx := strings.Index(meta, ":"); idx >= 0 {
		meta = strings.TrimSpace(meta[:idx])
	}
	for _, token := range strings.Fields(meta) {
		if !strings.HasPrefix(token, "code=") {
			continue
		}
		return strings.TrimSpace(strings.TrimPrefix(token, "code="))
	}
	return ""
}

func runtimeErrorEnvelope(raw string) string {
	code := runtimeErrorCodeFromText(raw)
	if strings.TrimSpace(code) == "" {
		return strings.TrimSpace(raw)
	}
	return fmt.Sprintf("runtime_error code=%s", code)
}
