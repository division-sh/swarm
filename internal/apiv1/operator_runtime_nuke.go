package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"swarm/internal/runtime/destructivereset"
	"swarm/internal/store"
)

const runtimeNukeIdempotencyTTL = 24 * time.Hour

type DestructiveResetCoordinator interface {
	BuildPlanWithLock(context.Context, destructivereset.Request, func(context.Context, destructivereset.Result) error) (destructivereset.Result, bool, error)
}

type DestructiveResetQuiescer interface {
	Apply(context.Context, destructivereset.QuiescenceRequest) (destructivereset.QuiescenceResult, error)
}

type DestructiveResetCleaner interface {
	Apply(context.Context, destructivereset.CleanupRequest) (destructivereset.CleanupResult, error)
}

type DestructiveResetContainerStopper interface {
	Apply(context.Context, destructivereset.ContainerResetRequest) (destructivereset.ContainerResetResult, error)
}

type runtimeNukeResult struct {
	OK             bool                                  `json:"ok"`
	Status         string                                `json:"status"`
	DryRun         bool                                  `json:"dry_run"`
	OperationName  string                                `json:"operation_name"`
	Plan           destructivereset.Result               `json:"plan"`
	Quiescence     destructivereset.QuiescenceResult     `json:"quiescence"`
	Cleanup        destructivereset.CleanupResult        `json:"cleanup"`
	Containers     destructivereset.ContainerResetResult `json:"containers"`
	PartialFailure bool                                  `json:"partial_failure"`
	Errors         []runtimeNukePartialError             `json:"errors,omitempty"`
}

type runtimeNukePartialError struct {
	Scope   string `json:"scope"`
	Message string `json:"message"`
}

func OperatorRuntimeNukeHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.ResetCoordinator == nil || opts.ResetQuiescer == nil || opts.ResetCleaner == nil || opts.ResetContainers == nil || opts.Idempotency == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"runtime.nuke": func(ctx context.Context, req Request) (any, error) {
			return executeRuntimeNuke(ctx, req, opts, now().UTC())
		},
	}
}

func executeRuntimeNuke(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	dryRun, err := optionalBoolParam(req.Params, "dry_run", false)
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     destructivereset.DefaultOperationName,
		TTL:            runtimeNukeIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		result, err := performRuntimeNuke(ctx, req, opts, dryRun, now)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		response, err := json.Marshal(result)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{
			ResourceID: result.OperationName,
			Response:   response,
		}, nil
	})
	if err != nil {
		return nil, runtimeNukeError(err)
	}
	var stored runtimeNukeResult
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		if replay {
			return nil, fmt.Errorf("decode runtime.nuke idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode runtime.nuke response: %w", err)
	}
	return stored, nil
}

func performRuntimeNuke(ctx context.Context, req Request, opts OperatorReadOptions, dryRun bool, now time.Time) (runtimeNukeResult, error) {
	var quiescence destructivereset.QuiescenceResult
	var cleanup destructivereset.CleanupResult
	var containers destructivereset.ContainerResetResult
	planResult, _, err := opts.ResetCoordinator.BuildPlanWithLock(ctx, destructivereset.Request{
		ActorTokenID: req.ActorTokenID,
		RequestHash:  req.RequestHash,
		DryRun:       dryRun,
		RequestedAt:  now,
	}, func(ctx context.Context, planResult destructivereset.Result) error {
		var err error
		quiescence, err = opts.ResetQuiescer.Apply(ctx, destructivereset.QuiescenceRequest{
			Result:       planResult,
			ActorTokenID: req.ActorTokenID,
			RequestedAt:  now,
		})
		if err != nil {
			return err
		}
		cleanup, err = opts.ResetCleaner.Apply(ctx, destructivereset.CleanupRequest{
			Result:       planResult,
			Quiescence:   quiescence,
			ActorTokenID: req.ActorTokenID,
			RequestedAt:  now,
		})
		if err != nil {
			return err
		}
		containers, err = opts.ResetContainers.Apply(ctx, destructivereset.ContainerResetRequest{
			Result:       planResult,
			Cleanup:      cleanup,
			ActorTokenID: req.ActorTokenID,
			RequestedAt:  now,
		})
		return err
	})
	if err != nil {
		return runtimeNukeResult{}, err
	}
	result := runtimeNukeResult{
		OK:            true,
		Status:        "completed",
		DryRun:        dryRun,
		OperationName: strings.TrimSpace(planResult.OperationName),
		Plan:          planResult,
		Quiescence:    quiescence,
		Cleanup:       cleanup,
		Containers:    containers,
	}
	if result.OperationName == "" {
		result.OperationName = destructivereset.DefaultOperationName
	}
	if dryRun {
		result.Status = "dry_run"
	}
	if len(containers.Failed) > 0 {
		result.OK = false
		result.Status = "partial_failure"
		result.PartialFailure = true
		for _, failure := range containers.Failed {
			result.Errors = append(result.Errors, runtimeNukePartialError{
				Scope:   "managed_containers",
				Message: strings.TrimSpace(failure.Error),
			})
		}
	}
	return result, nil
}

func runtimeNukeError(err error) error {
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
	var resetConflict *destructivereset.IdempotencyConflictError
	if errors.As(err, &resetConflict) {
		return NewApplicationError(IdempotencyConflictCode, false, map[string]any{
			"original_request_hash":    resetConflict.OriginalRequestHash,
			"conflicting_request_hash": resetConflict.ConflictingRequestHash,
			"original_response_ref": map[string]any{
				"method":      "runtime.nuke",
				"resource_id": resetConflict.Key.OperationName,
			},
		})
	}
	if errors.Is(err, destructivereset.ErrOperationInProgress) {
		return NewApplicationError(RuntimeNukeInProgressCode, true, map[string]any{
			"operation_name": destructivereset.DefaultOperationName,
		})
	}
	return err
}
