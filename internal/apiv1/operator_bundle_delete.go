package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	swruntime "swarm/internal/runtime"
	"swarm/internal/runtime/bundledelete"
	"swarm/internal/runtime/destructivereset"
	"swarm/internal/store"
)

const bundleDeleteIdempotencyTTL = 24 * time.Hour

func OperatorBundleDeleteHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.BundleDelete == nil || opts.Idempotency == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"bundle.delete": func(ctx context.Context, req Request) (any, error) {
			return executeBundleDelete(ctx, req, opts, now().UTC())
		},
	}
}

func executeBundleDelete(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	bundleHash, err := requiredBundleHashParam(req.Params, "bundle_hash")
	if err != nil {
		return nil, err
	}
	force, err := optionalBoolParam(req.Params, "force", false)
	if err != nil {
		return nil, err
	}
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
		ResourceID:     bundleHash,
		TTL:            bundleDeleteIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		result, err := opts.BundleDelete.Execute(ctx, bundledelete.Request{
			ActorTokenID: req.ActorTokenID,
			RequestHash:  req.RequestHash,
			BundleHash:   bundleHash,
			Force:        force,
			DryRun:       dryRun,
			RequestedAt:  now,
		})
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		response, err := json.Marshal(result)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{
			ResourceID: bundleHash,
			Response:   response,
		}, nil
	})
	if err != nil {
		return nil, bundleDeleteError(bundleHash, err)
	}
	var stored bundledelete.Result
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		if replay {
			return nil, fmt.Errorf("decode bundle.delete idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode bundle.delete response: %w", err)
	}
	deactivateRuntimeContextAfterBundleDelete(opts, stored)
	return stored, nil
}

func deactivateRuntimeContextAfterBundleDelete(opts OperatorReadOptions, result bundledelete.Result) {
	if opts.RuntimeContexts == nil || result.DryRun || strings.TrimSpace(result.BundleHash) == "" {
		return
	}
	if !bundleDeleteMadeRuntimeContextUnavailable(result) {
		return
	}
	cause := swruntime.RuntimeContextCauseUnloaded
	if result.PartialFailure && !result.Deleted {
		cause = swruntime.RuntimeContextCauseUnavailable
	}
	opts.RuntimeContexts.DeactivateBundleHash(result.BundleHash, cause)
}

func bundleDeleteMadeRuntimeContextUnavailable(result bundledelete.Result) bool {
	if result.Deleted {
		return true
	}
	if !result.Force || !result.PartialFailure {
		return false
	}
	if !result.Cleanup.AppliedAt.IsZero() {
		return true
	}
	if len(result.Cleanup.Runs) > 0 || len(result.Cleanup.Deliveries) > 0 || len(result.Cleanup.Sessions) > 0 || len(result.Cleanup.Timers) > 0 {
		return true
	}
	if !result.Containers.AppliedAt.IsZero() || len(result.Containers.Selected) > 0 || len(result.Containers.Stopped) > 0 || len(result.Containers.Failed) > 0 {
		return true
	}
	if !result.FinalMutation.AppliedAt.IsZero() || result.FinalMutation.RunsMarkedDeleted > 0 || result.FinalMutation.BundleRowsDeleted > 0 {
		return true
	}
	return false
}

func bundleDeleteError(bundleHash string, err error) error {
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
	if errors.Is(err, store.ErrBundleNotFound) || errors.Is(err, bundledelete.ErrBundleNotFound) {
		return NewApplicationError(BundleNotFoundCode, false, map[string]any{"bundle_hash": bundleHash})
	}
	if errors.Is(err, bundledelete.ErrOperationInProgress) || errors.Is(err, destructivereset.ErrOperationInProgress) {
		return NewApplicationError(BundleDeleteInProgressCode, true, map[string]any{
			"operation_name": bundledelete.DefaultOperationName,
		})
	}
	var active *bundledelete.ActiveRunsRemainError
	if errors.As(err, &active) {
		return NewApplicationError(BundleHasActiveRunsCode, false, bundleDeleteActiveRunsDetails(bundleHash, active.ActiveRuns))
	}
	if errors.Is(err, bundledelete.ErrActiveRunsRemain) {
		return NewApplicationError(BundleHasActiveRunsCode, false, bundleDeleteActiveRunsDetails(bundleHash, nil))
	}
	if errors.Is(err, bundledelete.ErrNonForceSplit) {
		return NewInvalidParamsError(map[string]any{
			"field":  "force",
			"reason": "bundle.delete mode is not supported by the configured bundle-delete owner",
		})
	}
	return err
}

func bundleDeleteActiveRunsDetails(bundleHash string, activeRuns []bundledelete.RunRef) map[string]any {
	details := map[string]any{
		"bundle_hash":    bundleHash,
		"active_run_ids": bundledelete.ActiveRunIDList(activeRuns),
	}
	if len(activeRuns) > 0 {
		details["active_runs"] = activeRuns
	}
	return details
}
