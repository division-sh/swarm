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
	"github.com/google/uuid"
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

// The selected-store owner must commit the run, pins, feed obligations and
// permanent run-creation receipt in one transaction. A transport cache is not
// an implementation of this port.
type deploymentRunStartOwner interface {
	StartDeploymentRunAcknowledged(context.Context, durabledata.RunCreationCommand, apiidempotency.Request) (durabledata.RunCreationOperationRecord, error)
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
	_, eventPresent := req.Params["event_name"]
	_, payloadPresent := req.Params["payload"]
	if !eventPresent && !payloadPresent {
		return executeDeploymentRunStart(ctx, req, opts, now)
	}
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

func executeDeploymentRunStart(ctx context.Context, req Request, opts EventPublicationOptions, now time.Time) (runStartResult, error) {
	data, present, err := runCreationDataEnvelopeParam(req.Method, req.Params)
	if err != nil {
		return runStartResult{}, err
	}
	if !present || len(data.Imports)+len(data.Pins) == 0 {
		return runStartResult{}, NewInvalidParamsError(map[string]any{"field": "data", "reason": "feed-only run creation requires at least one import or pin"})
	}
	rawRunID, provided, err := optionalStringParam(req.Params, "run_id")
	if err != nil {
		return runStartResult{}, err
	}
	parsedRunID, parseErr := uuid.Parse(rawRunID)
	if !provided || parseErr != nil || parsedRunID == uuid.Nil || parsedRunID.String() != rawRunID {
		return runStartResult{}, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "feed-only creation requires one canonical non-zero UUID"})
	}
	identity, err := bundleIdentityInputParam(req.Params)
	if err != nil {
		return runStartResult{}, err
	}
	params := eventPublicationParams{RunID: rawRunID, RunIDProvided: true, Data: data, DataPresent: true}
	ctx, selectedOpts, params, err := resolveEventPublicationBundleScope(ctx, opts, params, identity, eventPublicationConfig{rootInputOnly: true})
	if err != nil {
		return runStartResult{}, err
	}
	selector, err := scenarioExecutionSelectorParam(req.Params)
	if err != nil {
		return runStartResult{}, err
	}
	ctx, err = admitScenarioExecutionSelector(ctx, selectedOpts, rawRunID, params.NewRunCreated, selector)
	if err != nil {
		return runStartResult{}, err
	}
	command := durabledata.RunCreationCommand{
		RunID: rawRunID, Actor: req.ActorTokenID, BundleHash: params.SourceArtifactFact.BundleHash(), Data: data,
	}
	if _, _, command, err = command.RequestHash(); err != nil {
		return runStartResult{}, NewInvalidParamsError(map[string]any{"field": "data", "reason": err.Error()})
	}
	owner, ok := selectedOpts.Acknowledged.(deploymentRunStartOwner)
	if !ok || owner == nil {
		return runStartResult{}, fmt.Errorf("selected store does not support atomic deployment run creation")
	}
	key, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return runStartResult{}, err
	}
	record, err := owner.StartDeploymentRunAcknowledged(ctx, command, apiidempotency.Request{
		Method: req.Method, Actor: apiidempotency.BearerActor(req.ActorTokenID), IdempotencyKey: key,
		RequestHash: req.RequestHash, TTL: runStartIDempotencyTTL, Now: now,
	})
	if err != nil {
		return runStartResult{}, runStartIdempotencyError(dataApplicationError(err))
	}
	if err := durabledata.ValidateRunCreationReceiptForCommand(record, command); err != nil {
		return runStartResult{}, fmt.Errorf("deployment run creation returned contradictory receipt: %w", err)
	}
	if record.Summary.Outcome != "created" {
		code := durabledata.CodeRunDataRejected
		if record.Summary.Outcome == "head_conflict" {
			code = durabledata.CodeRunHeadConflict
		}
		return runStartResult{}, dataApplicationError(durabledata.NewDomainErrorWithDetails(code,
			map[string]any{"run_id": record.Summary.RunID, "operation": record.Summary},
			"run creation %s", record.Summary.Outcome))
	}
	return runStartResult{RunID: record.Summary.RunID, Status: record.Summary.Status, DataBinding: record.Binding}, nil
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
