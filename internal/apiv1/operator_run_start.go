package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

const runStartIDempotencyTTL = 24 * time.Hour

var sha256FingerprintPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var bundleHashPattern = regexp.MustCompile(`^bundle-v1:sha256:[a-f0-9]{64}$`)

type runStartResult struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type bundleIdentityParam struct {
	BundleHash        string
	LegacyFingerprint string
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
	if opts.Idempotency == nil {
		return false
	}
	if runtimeContextManager(opts) != nil {
		return true
	}
	return opts.Source != nil &&
		opts.Events != nil &&
		strings.TrimSpace(opts.Bundle.Fingerprint) != ""
}

func executeRunStart(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	cfg := eventPublicationConfig{
		sourceAgent:                    func(Request) string { return "api.v1" },
		rootInputOnly:                  true,
		injectRunIDEntityIDWhenMissing: true,
		publishError:                   eventPublishPublishError,
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

func bundleIdentityInputParam(params map[string]any) (bundleIdentityParam, error) {
	if params == nil {
		return bundleIdentityParam{}, nil
	}
	rawHash, hashSet := params["bundle_hash"]
	rawRef, refSet := params["bundle_ref"]
	if hashSet && refSet {
		return bundleIdentityParam{}, NewApplicationError(UnsupportedBundleHashCode, false, map[string]any{"reason": "bundle_hash cannot be combined with legacy bundle_ref"})
	}
	if hashSet {
		hash, ok := rawHash.(string)
		hash = strings.TrimSpace(hash)
		if !ok || hash == "" {
			return bundleIdentityParam{}, NewApplicationError(UnsupportedBundleHashCode, false, map[string]any{"reason": "bundle_hash must be bundle-v1:sha256:<64 lowercase hex>"})
		}
		if !bundleHashPattern.MatchString(hash) {
			return bundleIdentityParam{}, NewApplicationError(UnsupportedBundleHashCode, false, map[string]any{"reason": "bundle_hash must be bundle-v1:sha256:<64 lowercase hex>"})
		}
		return bundleIdentityParam{BundleHash: hash}, nil
	}
	fingerprint, err := legacyBundleFingerprintParam(rawRef, refSet)
	if err != nil {
		return bundleIdentityParam{}, err
	}
	return bundleIdentityParam{LegacyFingerprint: fingerprint}, nil
}

func legacyBundleFingerprintParam(raw any, ok bool) (string, error) {
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

func (p bundleIdentityParam) mismatchDetails(bootFingerprint string) map[string]any {
	return map[string]any{
		"boot_fingerprint":     strings.TrimSpace(bootFingerprint),
		"provided_fingerprint": p.LegacyFingerprint,
	}
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
	var bundleUnavailable *storerunlifecycle.PersistedBundleUnavailableError
	if errors.As(err, &bundleUnavailable) || errors.Is(err, storerunlifecycle.ErrPersistedBundleUnavailable) {
		details := map[string]any{"event_name": eventName}
		if bundleUnavailable != nil {
			details["bundle_hash"] = bundleUnavailable.BundleHash
			details["bundle_source"] = bundleUnavailable.BundleSource
			details["cause"] = bundleUnavailable.Cause
		}
		return NewApplicationError(BundleUnavailableCode, false, details)
	}
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
