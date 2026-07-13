package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

const standingServiceIdempotencyTTL = 24 * time.Hour

type standingServiceResult struct {
	ServiceID      string `json:"service_id"`
	RunID          string `json:"run_id"`
	Generation     int64  `json:"generation"`
	EffectiveState string `json:"effective_state"`
	Transition     string `json:"transition"`
}

func OperatorStandingServiceHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.StandingServices == nil || opts.Idempotency == nil {
		return nil
	}
	return map[string]MethodHandler{
		"standing.suspend": func(ctx context.Context, req Request) (any, error) {
			return executeStandingServiceOperation(ctx, req, opts, "suspend")
		},
		"standing.resume": func(ctx context.Context, req Request) (any, error) {
			return executeStandingServiceOperation(ctx, req, opts, "resume")
		},
		"standing.reset": func(ctx context.Context, req Request) (any, error) {
			return executeStandingServiceOperation(ctx, req, opts, "reset")
		},
	}
}

func executeStandingServiceOperation(ctx context.Context, req Request, opts OperatorReadOptions, action string) (any, error) {
	serviceID := strings.TrimSpace(stringParam(req.Params, "service_id"))
	parsed, err := uuid.Parse(serviceID)
	if err != nil {
		return nil, NewInvalidParamsError(map[string]any{"field": "service_id", "reason": "must be a UUID"})
	}
	serviceID = parsed.String()
	reason, _, err := optionalStringParam(req.Params, "reason")
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	completion, _, err := opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method: req.Method, ActorTokenID: req.ActorTokenID, IdempotencyKey: idempotencyKey,
		RequestHash: req.RequestHash, ResourceID: serviceID, TTL: standingServiceIdempotencyTTL, Now: now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		operation := runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "api.v1", Reason: reason}
		var reconciled runtimepipeline.StandingServiceReconciliation
		var operationErr error
		switch action {
		case "suspend":
			reconciled, operationErr = opts.StandingServices.SuspendStandingService(ctx, operation)
		case "resume":
			reconciled, operationErr = opts.StandingServices.ResumeStandingService(ctx, operation)
		case "reset":
			reconciled, operationErr = opts.StandingServices.ResetStandingService(ctx, operation)
		default:
			operationErr = fmt.Errorf("unsupported standing service action %q", action)
		}
		if operationErr != nil {
			return store.APIIdempotencyCompletion{}, standingServiceOperationError(serviceID, operationErr)
		}
		response, err := json.Marshal(standingServiceResult{
			ServiceID: reconciled.ServiceID, RunID: reconciled.RunID, Generation: reconciled.Generation,
			EffectiveState: reconciled.EffectiveState, Transition: reconciled.Transition,
		})
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: serviceID, Response: response}, nil
	})
	if err != nil {
		return nil, standingServiceOperationError(serviceID, err)
	}
	var result standingServiceResult
	if err := json.Unmarshal(completion.Response, &result); err != nil {
		return nil, fmt.Errorf("decode %s response: %w", req.Method, err)
	}
	return result, nil
}

func standingServiceOperationError(serviceID string, err error) error {
	var conflict *store.APIIdempotencyConflictError
	if errors.As(err, &conflict) {
		return NewApplicationError(IdempotencyConflictCode, false, map[string]any{
			"original_request_hash": conflict.OriginalRequestHash, "conflicting_request_hash": conflict.ConflictingRequestHash,
			"original_response_ref": map[string]any{"method": conflict.Method, "resource_id": conflict.ResourceID},
		})
	}
	if errors.Is(err, runtimepipeline.ErrStandingServiceNotFound) {
		return NewApplicationError(StandingServiceNotFoundCode, false, map[string]any{"service_id": serviceID})
	}
	return err
}
