package mcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/failures"
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

type ProtocolErrorPayload struct {
	Code      string         `json:"code"`
	Operation string         `json:"operation,omitempty"`
	Message   string         `json:"message"`
	Detail    map[string]any `json:"detail,omitempty"`
}

type ProtocolError struct {
	Payload ProtocolErrorPayload
	cause   error
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Payload.Code + ": " + e.Payload.Message)
}

func (e *ProtocolError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func NewProtocolError(code, operation, message string, detail map[string]any, cause error) error {
	return &ProtocolError{
		Payload: ProtocolErrorPayload{
			Code:      strings.TrimSpace(code),
			Operation: strings.TrimSpace(operation),
			Message:   strings.TrimSpace(message),
			Detail:    detail,
		},
		cause: cause,
	}
}

type RuntimeErrorPayload struct {
	Failure  *failures.Envelope    `json:"failure,omitempty"`
	Protocol *ProtocolErrorPayload `json:"protocol_error,omitempty"`
}

func RuntimeErrorPayloadFromError(err error) *RuntimeErrorPayload {
	if err == nil {
		return nil
	}
	if envelope, ok := failures.EnvelopeFromError(err); ok {
		return &RuntimeErrorPayload{Failure: &envelope}
	}
	var protocolErr *ProtocolError
	if errors.As(err, &protocolErr) && protocolErr != nil {
		payload := protocolErr.Payload
		return &RuntimeErrorPayload{Protocol: &payload}
	}
	return nil
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
	if (payload.Failure == nil) == (payload.Protocol == nil) {
		return nil, fmt.Errorf("runtimeError must contain exactly one of failure or protocol_error")
	}
	if payload.Failure != nil {
		if err := failures.ValidateEnvelope(*payload.Failure); err != nil {
			return nil, fmt.Errorf("runtimeError.failure: %w", err)
		}
	}
	if payload.Protocol != nil && strings.TrimSpace(payload.Protocol.Code) == "" {
		return nil, fmt.Errorf("runtimeError.protocol_error.code is required")
	}
	return &payload, nil
}
