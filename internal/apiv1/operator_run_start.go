package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimebus "swarm/internal/runtime/bus"
	"swarm/internal/store"
)

const runStartIDempotencyTTL = 24 * time.Hour

var sha256FingerprintPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

type runStartResult struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

func OperatorRunStartHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if !runStartConfigured(opts) {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"run.start": func(ctx context.Context, req Request) (any, error) {
			return executeRunStart(ctx, req, opts, now().UTC())
		},
	}
}

func runStartConfigured(opts OperatorReadOptions) bool {
	return opts.Source != nil &&
		opts.Events != nil &&
		opts.Idempotency != nil &&
		strings.TrimSpace(opts.Bundle.Fingerprint) != ""
}

func executeRunStart(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	cfg := eventPublicationConfig{
		sourceAgent:                    func(Request) string { return "api.v1" },
		rootInputOnly:                  true,
		injectRunIDEntityIDWhenMissing: true,
		buildCompletion: func(_ context.Context, _ OperatorReadOptions, params eventPublicationParams) (any, string, error) {
			return runStartResult{RunID: params.RunID, Status: "running"}, params.RunID, nil
		},
	}
	completion, replay, err := executeOperatorEventPublication(ctx, req, opts, now, cfg)
	if err != nil {
		return nil, runStartIdempotencyError(err)
	}
	var stored runStartResult
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		if replay {
			return nil, fmt.Errorf("decode run.start idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode run.start response: %w", err)
	}
	return stored, nil
}

func bundleFingerprintParam(params map[string]any) (string, error) {
	if params == nil {
		return "", nil
	}
	raw, ok := params["bundle_ref"]
	if !ok || isEmptyParam(raw) {
		return "", nil
	}
	ref, ok := raw.(map[string]any)
	if !ok {
		return "", NewApplicationError(UnsupportedBundleRefCode, false, map[string]any{"reason": "bundle_ref must be an object"})
	}
	if len(ref) != 1 {
		return "", NewApplicationError(UnsupportedBundleRefCode, false, map[string]any{"reason": "bundle_ref supports fingerprint only"})
	}
	rawFingerprint, ok := ref["fingerprint"].(string)
	fingerprint := strings.TrimSpace(rawFingerprint)
	if !ok || fingerprint == "" {
		return "", NewApplicationError(UnsupportedBundleRefCode, false, map[string]any{"reason": "bundle_ref.fingerprint is required"})
	}
	if !sha256FingerprintPattern.MatchString(fingerprint) {
		return "", NewApplicationError(UnsupportedBundleRefCode, false, map[string]any{"reason": "bundle_ref.fingerprint must be sha256:<64 lowercase hex>"})
	}
	return fingerprint, nil
}

func runStartPayloadEntityID(value any) (string, bool, error) {
	if value == nil {
		return "", false, nil
	}
	text, ok := value.(string)
	if !ok {
		return "", false, NewInvalidParamsError(map[string]any{"field": "payload.entity_id", "reason": "must be a UUID string"})
	}
	entityID := strings.TrimSpace(text)
	if entityID == "" {
		return "", false, nil
	}
	parsed, err := uuid.Parse(entityID)
	if err != nil {
		return "", false, NewInvalidParamsError(map[string]any{"field": "payload.entity_id", "reason": "must be a UUID string"})
	}
	return parsed.String(), true, nil
}

func runStartIdempotencyError(err error) error {
	var conflict *store.APIIdempotencyConflictError
	if errors.As(err, &conflict) {
		return NewApplicationError(IdempotencyConflictCode, false, map[string]any{
			"original_request_hash":    conflict.OriginalRequestHash,
			"conflicting_request_hash": conflict.ConflictingRequestHash,
			"original_response_ref": map[string]any{
				"method":      conflict.Method,
				"resource_id": conflict.ResourceID,
			},
		})
	}
	return err
}

func runStartPublishError(eventName string, err error) error {
	if errors.Is(err, runtimebus.ErrPayloadValidation) || strings.Contains(err.Error(), "validate event payload") {
		return NewApplicationError(PayloadValidationFailedCode, false, map[string]any{
			"violations": []map[string]any{{
				"field_path": "$",
				"rule":       "event_payload_schema",
				"message":    strings.TrimSpace(err.Error()),
			}},
		})
	}
	if strings.Contains(err.Error(), "invalid event type") {
		return NewApplicationError(EventNotDeclaredCode, false, map[string]any{"event_name": eventName})
	}
	return err
}
