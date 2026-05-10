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
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimerunstart "swarm/internal/runtime/runstart"
	"swarm/internal/store"
)

const runStartIDempotencyTTL = 24 * time.Hour

var sha256FingerprintPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

type runStartResult struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type runStartParams struct {
	BundleFingerprint string
	EventName         string
	Payload           json.RawMessage
	EntityID          string
	RunID             string
	IdempotencyKey    string
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
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		TTL:            runStartIDempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		params, err := runStartParamsFromRequest(req, opts.Bundle.Fingerprint)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		set, err := runtimerunstart.ValidateInputEvents(opts.Source, []string{params.EventName})
		if err != nil {
			return store.APIIdempotencyCompletion{}, NewApplicationError(EventNotDeclaredCode, false, map[string]any{
				"event_name":      params.EventName,
				"declared_events": set.Declared,
			})
		}
		result := runStartResult{RunID: params.RunID, Status: "running"}
		if err := opts.Events.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			RunID:       params.RunID,
			Type:        events.EventType(params.EventName),
			SourceAgent: "api.v1",
			Payload:     params.Payload,
			CreatedAt:   now,
		}.WithEntityID(params.EntityID)); err != nil {
			return store.APIIdempotencyCompletion{}, runStartPublishError(params.EventName, err)
		}
		response, err := json.Marshal(result)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{
			ResourceID: params.RunID,
			Response:   response,
		}, nil
	})
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

func runStartParamsFromRequest(req Request, bootFingerprint string) (runStartParams, error) {
	eventName := stringParam(req.Params, "event_name")
	if eventName == "" {
		return runStartParams{}, NewInvalidParamsError(map[string]any{"field": "event_name", "reason": "required parameter is missing"})
	}
	fingerprint, err := bundleFingerprintParam(req.Params)
	if err != nil {
		return runStartParams{}, err
	}
	if fingerprint != "" && fingerprint != strings.TrimSpace(bootFingerprint) {
		return runStartParams{}, NewApplicationError(BundleMismatchCode, false, map[string]any{
			"provided_fingerprint": fingerprint,
			"boot_fingerprint":     strings.TrimSpace(bootFingerprint),
		})
	}
	runID, _, err := optionalStringParam(req.Params, "run_id")
	if err != nil {
		return runStartParams{}, err
	}
	if runID == "" {
		runID = uuid.NewString()
	} else if parsed, err := uuid.Parse(runID); err != nil {
		return runStartParams{}, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "must be a UUID"})
	} else {
		runID = parsed.String()
	}
	payload, entityID, err := runStartPayload(req.Params, runID)
	if err != nil {
		return runStartParams{}, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return runStartParams{}, err
	}
	return runStartParams{
		BundleFingerprint: fingerprint,
		EventName:         eventName,
		Payload:           payload,
		EntityID:          entityID,
		RunID:             runID,
		IdempotencyKey:    idempotencyKey,
	}, nil
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

func runStartPayload(params map[string]any, runID string) (json.RawMessage, string, error) {
	if params == nil {
		return nil, "", NewInvalidParamsError(map[string]any{"field": "payload", "reason": "required parameter is missing"})
	}
	raw, ok := params["payload"]
	if !ok || isEmptyParam(raw) {
		return nil, "", NewInvalidParamsError(map[string]any{"field": "payload", "reason": "required parameter is missing"})
	}
	payload, ok := raw.(map[string]any)
	if !ok {
		return nil, "", NewInvalidParamsError(map[string]any{"field": "payload", "reason": "must be an object"})
	}
	cloned := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		cloned[key] = value
	}
	entityID := strings.TrimSpace(runStartPayloadString(cloned["entity_id"]))
	if entityID == "" {
		entityID = strings.TrimSpace(runID)
		cloned["entity_id"] = entityID
	}
	encoded, err := json.Marshal(cloned)
	if err != nil {
		return nil, "", err
	}
	return encoded, entityID, nil
}

func runStartPayloadString(value any) string {
	text, _ := value.(string)
	return text
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
