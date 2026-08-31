package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/google/uuid"
)

const runtimeNukeIdempotencyTTL = 24 * time.Hour

type DestructiveResetCoordinator interface {
	Execute(context.Context, destructivereset.Request) (destructivereset.ExecutionResult, error)
}

type runtimeNukeResult struct {
	OK                     bool                                  `json:"ok"`
	Status                 string                                `json:"status"`
	DryRun                 bool                                  `json:"dry_run"`
	IncludeSourceArtifacts bool                                  `json:"include_source_artifacts"`
	OperationName          string                                `json:"operation_name"`
	Plan                   destructivereset.Result               `json:"plan"`
	Quiescence             destructivereset.QuiescenceResult     `json:"quiescence"`
	Cleanup                destructivereset.CleanupResult        `json:"cleanup"`
	Containers             destructivereset.ContainerResetResult `json:"containers"`
	PartialFailure         bool                                  `json:"partial_failure"`
	Errors                 []runtimeNukePartialError             `json:"errors,omitempty"`
}

type runtimeNukePartialError struct {
	Scope   string `json:"scope"`
	Message string `json:"message"`
}

func OperatorRuntimeNukeHandlers(opts RuntimeNukeHandlerOptions) map[string]MethodHandler {
	if opts.Coordinator == nil || opts.Idempotency == nil {
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

func executeRuntimeNuke(ctx context.Context, req Request, opts RuntimeNukeHandlerOptions, now time.Time) (any, error) {
	dryRun, err := optionalBoolParam(req.Params, "dry_run", false)
	if err != nil {
		return nil, err
	}
	includeSourceArtifacts, err := optionalBoolParam(req.Params, "include_source_artifacts", true)
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	operationID := uuid.NewString()
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     destructivereset.DefaultOperationName,
		TTL:            runtimeNukeIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		result, err := performRuntimeNuke(ctx, req, opts, operationID, dryRun, includeSourceArtifacts, now)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		response, err := json.Marshal(result)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{
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

func performRuntimeNuke(ctx context.Context, req Request, opts RuntimeNukeHandlerOptions, operationID string, dryRun, includeSourceArtifacts bool, now time.Time) (runtimeNukeResult, error) {
	execution, err := opts.Coordinator.Execute(ctx, destructivereset.Request{
		OperationID:               operationID,
		ActorTokenID:              req.ActorTokenID,
		RequestHash:               req.RequestHash,
		DryRun:                    dryRun,
		IncludeSourceArtifacts:    includeSourceArtifacts,
		IncludeSourceArtifactsSet: true,
		RequestedAt:               now,
	})
	if err != nil {
		return runtimeNukeResult{}, err
	}
	planResult := execution.Plan
	result := runtimeNukeResult{
		OK:                     true,
		Status:                 "completed",
		DryRun:                 dryRun,
		IncludeSourceArtifacts: planResult.IncludeSourceArtifacts,
		OperationName:          strings.TrimSpace(planResult.OperationName),
		Plan:                   planResult,
		Quiescence:             execution.Quiescence,
		Cleanup:                execution.Cleanup,
		Containers:             execution.Containers,
	}
	if result.OperationName == "" {
		result.OperationName = destructivereset.DefaultOperationName
	}
	if dryRun {
		result.Status = "dry_run"
	}
	if len(execution.Containers.Failed) > 0 {
		result.OK = false
		result.Status = "partial_failure"
		result.PartialFailure = true
		for _, failure := range execution.Containers.Failed {
			result.Errors = append(result.Errors, runtimeNukePartialError{
				Scope:   "managed_containers",
				Message: strings.TrimSpace(failure.Error),
			})
		}
	}
	return result, nil
}

func runtimeNukeError(err error) error {
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
	if errors.Is(err, destructivereset.ErrOperationInProgress) {
		return NewApplicationError(RuntimeNukeInProgressCode, true, map[string]any{
			"operation_name": destructivereset.DefaultOperationName,
		})
	}
	return err
}
