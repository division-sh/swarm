package apiv1

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const runStartIDempotencyTTL = 24 * time.Hour

type runStartResult struct {
	RunID       string                  `json:"run_id"`
	EventID     string                  `json:"event_id,omitempty"`
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
	if lookup, ok := publication.Acknowledged.(publicPublicationCompletionLookup); ok && lookup != nil {
		return true
	}
	if runtimeContextManager(publication.RuntimeContexts) != nil {
		return true
	}
	return publication.Source != nil &&
		publication.Events != nil &&
		strings.TrimSpace(publication.Bundle.BundleHash) != ""
}

func executeRunStart(ctx context.Context, req Request, opts EventPublicationOptions, now time.Time) (any, error) {
	if _, rows := req.Params["data"]; rows {
		return nil, NewApplicationError(string(durabledata.CodeRunDataImmutable), false, map[string]any{
			"operation": "run.start", "reason": "live_run_rows_binding_unavailable",
			"message": "run start cannot publish rows into a live run with immutable data bindings",
		})
	}
	cfg := eventPublicationConfig{
		sourceAgent:                    func(Request) string { return "api.v1" },
		rootInputOnly:                  true,
		requireExistingExplicitRun:     true,
		injectRunIDEntityIDWhenMissing: true,
		injectRunIDEntityIDOnlyNewRun:  true,
		durablePublishAck:              true,
		atomicAPICompletion:            true,
		publishError:                   runStartEventPublishError,
		buildCompletion: func(ctx context.Context, selected EventPublicationOptions, params eventPublicationParams) (any, string, error) {
			header, err := selected.Runs.LoadRunHeader(ctx, params.RunID)
			if err != nil {
				return nil, "", err
			}
			return runStartResult{RunID: params.RunID, EventID: params.EventID, Status: header.Status, DataBinding: durabledata.DataBinding{State: "none"}}, params.EventID, nil
		},
	}
	completion, _, err := executeOperatorEventPublication(ctx, req, opts, now, cfg)
	if err != nil {
		return nil, runStartIdempotencyError(err)
	}
	return recordedPublicCompletionResult(completion)
}

func admitRunStartDeploymentFeeds(source semanticview.Source, data durabledata.RunCreationDataEnvelope) ([]runtimepinrouting.SourceEvent, error) {
	var selected []runtimepinrouting.SourceEvent
	admit := func(ref durabledata.DeclarationRef) error {
		routingSource, err := events.NewDeploymentFeedRoutingSource(ref.FlowPath)
		var feed runtimepinrouting.SourceEvent
		if err == nil {
			feed, err = runtimepinrouting.AdmitDeploymentFeedDeclaration(source, events.EventType(ref.EventName), routingSource)
		}
		if err != nil {
			return NewInvalidParamsError(map[string]any{"field": "data", "declaration": ref.EventName, "reason": err.Error()})
		}
		selected = append(selected, feed)
		return nil
	}
	for _, item := range data.Imports {
		if err := admit(item.Declaration); err != nil {
			return nil, err
		}
	}
	for _, item := range data.Pins {
		if err := admit(item.Declaration); err != nil {
			return nil, err
		}
	}
	return selected, nil
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
			return bundleIdentityParam{}, NewApplicationError(UnsupportedBundleHashCode, false, map[string]any{"reason": "bundle_hash must be bundle-v2:sha256:<64 lowercase hex>"})
		}
		if err := runtimecontracts.ValidateBundleHash(hash); err != nil {
			return bundleIdentityParam{}, NewApplicationError(UnsupportedBundleHashCode, false, map[string]any{"reason": "bundle_hash must be bundle-v2:sha256:<64 lowercase hex>"})
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
	var bundleUnavailable *runtimerunlifecycle.SourceArtifactUnavailableError
	if errors.As(err, &bundleUnavailable) || errors.Is(err, runtimerunlifecycle.ErrSourceArtifactUnavailable) {
		details := map[string]any{"event_name": eventName}
		if bundleUnavailable != nil {
			details["bundle_hash"] = bundleUnavailable.BundleHash
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
	if runtimefailures.OnlyBranches(err, func(branch error) bool { return branch == runtimebus.ErrAPIEventLiveRunChanged }) {
		return NewApplicationError(EventPublishRejectedCode, false, map[string]any{
			"event_name": eventName, "reason": "live_run_changed", "remediation": "select the live run again",
		})
	}
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
