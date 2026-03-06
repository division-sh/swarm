package mcp

import (
	"fmt"
	"strings"
)

const (
	ErrCodeAuthMissingBearer = "mcp_auth_missing_bearer"
	ErrCodeAuthInvalidBearer = "mcp_auth_invalid_bearer"
	ErrCodeContextMissing    = "mcp_context_token_missing"
	ErrCodeContextNotFound   = "mcp_context_token_not_found"
	ErrCodeContextStale      = "mcp_context_token_stale_epoch"
	ErrCodeActorMissing      = "mcp_actor_missing"
	ErrCodeToolNotAllowed    = "mcp_tool_not_allowed"
	ErrCodeToolExecFailed    = "mcp_tool_execution_failed"
	ErrCodeInvalidRequest    = "mcp_invalid_request"
	ErrCodeStallDetected     = "mcp_stall_detected"
)

func RuntimeErrorCodeFromText(raw string) string {
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

func RuntimeErrorEnvelope(raw string) string {
	code := RuntimeErrorCodeFromText(raw)
	if strings.TrimSpace(code) == "" {
		return strings.TrimSpace(raw)
	}
	return fmt.Sprintf("runtime_error code=%s", code)
}
