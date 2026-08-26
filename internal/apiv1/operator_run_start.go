package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

const runStartIDempotencyTTL = 24 * time.Hour

type runStartResult struct {
	RunID       string                  `json:"run_id"`
	Status      string                  `json:"status"`
	DataBinding durabledata.DataBinding `json:"data_binding"`
}

type bundleIdentityParam struct {
	BundleHash string
}

func OperatorRunStartHandlers(opts RunStartHandlerOptions) map[string]MethodHandler {
	if !runStartConfigured(opts) {
		return nil
	}
	now := opts.Publication.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"run.start": func(ctx context.Context, req Request) (any, error) {
			return executeRunStart(ctx, req, opts.Publication, now().UTC())
		},
	}
}

func runStartConfigured(opts RunStartHandlerOptions) bool {
	publication := opts.Publication
	if publication.Idempotency == nil {
		return false
	}
	if runtimeContextManager(publication.RuntimeContexts) != nil {
		return true
	}
	return publication.Source != nil &&
		publication.Events != nil &&
		strings.TrimSpace(publication.Bundle.BundleHash) != ""
}

func executeRunStart(ctx context.Context, req Request, opts EventPublicationOptions, now time.Time) (any, error) {
	cfg := eventPublicationConfig{
		sourceAgent:                    func(Request) string { return "api.v1" },
		rootInputOnly:                  true,
		injectRunIDEntityIDWhenMissing: true,
		durablePublishAck:              true,
		atomicAPICompletion:            true,
		publishError:                   runStartEventPublishError,
		buildCompletion: func(_ context.Context, _ EventPublicationOptions, params eventPublicationParams) (any, string, error) {
			return runStartResult{RunID: params.RunID, Status: "running", DataBinding: durabledata.DataBinding{State: "none"}}, params.RunID, nil
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
	if hashSet {
		hash, ok := rawHash.(string)
		hash = strings.TrimSpace(hash)
		if !ok || hash == "" {
			return bundleIdentityParam{}, NewApplicationError(UnsupportedBundleHashCode, false, map[string]any{"reason": "bundle_hash must be bundle-v1:sha256:<64 lowercase hex>"})
		}
		if err := runtimecontracts.ValidateBundleHash(hash); err != nil {
			return bundleIdentityParam{}, NewApplicationError(UnsupportedBundleHashCode, false, map[string]any{"reason": "bundle_hash must be bundle-v1:sha256:<64 lowercase hex>"})
		}
		return bundleIdentityParam{BundleHash: hash}, nil
	}
	return bundleIdentityParam{}, nil
}

func runStartIdempotencyError(err error) error {
	var conflict *apiidempotency.ConflictError
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

func publicationApplicationError(eventName string, err error) error {
	var dataDomain *durabledata.DomainError
	if errors.As(err, &dataDomain) {
		return dataApplicationError(err)
	}
	var bundleUnavailable *runtimerunlifecycle.PersistedBundleUnavailableError
	if errors.As(err, &bundleUnavailable) || errors.Is(err, runtimerunlifecycle.ErrPersistedBundleUnavailable) {
		details := map[string]any{"event_name": eventName}
		if bundleUnavailable != nil {
			details["bundle_hash"] = bundleUnavailable.BundleHash
			details["bundle_source"] = bundleUnavailable.BundleSource
			details["cause"] = bundleUnavailable.Cause
		}
		return NewApplicationError(BundleUnavailableCode, false, details)
	}
	if errors.Is(err, runtimebus.ErrPayloadValidation) {
		return NewApplicationError(PayloadValidationFailedCode, false, map[string]any{
			"violations": []map[string]any{{
				"field_path": "$",
				"rule":       "event_payload_schema",
				"message":    strings.TrimSpace(err.Error()),
			}},
		})
	}
	return err
}

func eventCatalogPublishError(eventName string, err error) error {
	mapped := publicationApplicationError(eventName, err)
	var appErr *ApplicationError
	if errors.As(mapped, &appErr) {
		return mapped
	}
	if errors.Is(err, runtimebus.ErrInvalidEventType) {
		return NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name": eventName,
			"reason":     "event_not_admitted_by_publisher",
		})
	}
	return mapped
}
