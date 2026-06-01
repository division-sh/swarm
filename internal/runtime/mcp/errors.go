package mcp

import (
	"encoding/json"
	"fmt"
	"strings"

	runtimerterr "github.com/division-sh/swarm/internal/runtime/rterrors"
)

const (
	ErrCodeAuthUnconfigured  = "mcp_auth_unconfigured"
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

type RuntimeErrorPayload struct {
	Code      string               `json:"code"`
	Component string               `json:"component,omitempty"`
	Operation string               `json:"operation,omitempty"`
	Retryable bool                 `json:"retryable"`
	Message   string               `json:"message,omitempty"`
	Cause     *RuntimeErrorPayload `json:"cause,omitempty"`
}

func RuntimeErrorPayloadFromError(err error) *RuntimeErrorPayload {
	if err == nil {
		return nil
	}
	runtimeErr, ok := runtimerterr.AsRuntimeError(err)
	if !ok || runtimeErr == nil {
		return nil
	}
	payload := &RuntimeErrorPayload{
		Code:      strings.TrimSpace(runtimeErr.Code),
		Component: strings.TrimSpace(runtimeErr.Component),
		Operation: strings.TrimSpace(runtimeErr.Operation),
		Retryable: runtimeErr.Retryable,
		Message:   strings.TrimSpace(runtimeErr.Message),
	}
	if payload.Message == "" && runtimeErr.Cause != nil {
		payload.Message = strings.TrimSpace(runtimeErr.Cause.Error())
	}
	if cause := RuntimeErrorPayloadFromError(runtimeErr.Cause); cause != nil {
		payload.Cause = cause
	}
	return payload
}

func DecodeRuntimeErrorPayload(raw any) (*RuntimeErrorPayload, error) {
	if raw == nil {
		return nil, fmt.Errorf("runtimeError payload is required")
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode runtimeError payload: %w", err)
	}
	var payload RuntimeErrorPayload
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return nil, fmt.Errorf("decode runtimeError payload: %w", err)
	}
	if strings.TrimSpace(payload.Code) == "" {
		return nil, fmt.Errorf("runtimeError.code is required")
	}
	return &payload, nil
}
