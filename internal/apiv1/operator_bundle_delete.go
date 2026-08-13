package apiv1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/runtime/bundledelete"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/google/uuid"
)

const bundleDeleteIdempotencyTTL = bundledelete.FinalMutationReplayWindow

func OperatorBundleDeleteHandlers(opts BundleDeleteHandlerOptions) map[string]MethodHandler {
	if opts.Executor == nil || opts.Idempotency == nil {
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

func executeBundleDelete(ctx context.Context, req Request, opts BundleDeleteHandlerOptions, now time.Time) (any, error) {
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
	operationID := uuid.NewString()
	replayKeyHash := bundleDeleteReplayKeyHash(req, idempotencyKey)
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     bundleHash,
		TTL:            bundleDeleteIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		result, err := opts.Executor.Execute(ctx, bundledelete.Request{
			OperationID:   operationID,
			ActorTokenID:  req.ActorTokenID,
			RequestHash:   req.RequestHash,
			ReplayKeyHash: replayKeyHash,
			BundleHash:    bundleHash,
			Force:         force,
			DryRun:        dryRun,
			RequestedAt:   now,
		})
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		response, err := json.Marshal(result)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{
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
	return stored, nil
}

func bundleDeleteReplayKeyHash(req Request, idempotencyKey string) string {
	if idempotencyKey == "" {
		return ""
	}
	identity := req.Method + "\x00" + req.ActorTokenID + "\x00" + idempotencyKey
	digest := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(digest[:])
}

func bundleDeleteError(bundleHash string, err error) error {
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
	if errors.Is(err, bundlecatalog.ErrNotFound) || errors.Is(err, bundledelete.ErrBundleNotFound) {
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
