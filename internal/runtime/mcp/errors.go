package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	var wire struct {
		Failure  json.RawMessage `json:"failure"`
		Protocol json.RawMessage `json:"protocol_error"`
	}
	if err := decodeStrictJSON(encoded, &wire); err != nil {
		return nil, fmt.Errorf("decode runtimeError payload: %w", err)
	}
	if (len(wire.Failure) == 0) == (len(wire.Protocol) == 0) {
		return nil, fmt.Errorf("runtimeError must contain exactly one of failure or protocol_error")
	}
	payload := RuntimeErrorPayload{}
	if len(wire.Failure) != 0 {
		envelope, err := failures.UnmarshalEnvelope(wire.Failure)
		if err != nil {
			return nil, fmt.Errorf("runtimeError.failure: %w", err)
		}
		payload.Failure = &envelope
	}
	if len(wire.Protocol) != 0 {
		var protocol ProtocolErrorPayload
		if err := decodeStrictJSON(wire.Protocol, &protocol); err != nil {
			return nil, fmt.Errorf("runtimeError.protocol_error: %w", err)
		}
		if strings.TrimSpace(protocol.Code) == "" {
			return nil, fmt.Errorf("runtimeError.protocol_error.code is required")
		}
		if strings.TrimSpace(protocol.Message) == "" {
			return nil, fmt.Errorf("runtimeError.protocol_error.message is required")
		}
		payload.Protocol = &protocol
	}
	return &payload, nil
}

func decodeStrictJSON(raw []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("trailing JSON value is not allowed")
		}
		return err
	}
	return nil
}
