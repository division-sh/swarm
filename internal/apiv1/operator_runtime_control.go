package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
)

const runtimeControlIdempotencyTTL = 24 * time.Hour

type RuntimeIngressController interface {
	Pause(context.Context, runtimeingress.TransitionRequest) (runtimeingress.TransitionResult, error)
	Resume(context.Context, runtimeingress.TransitionRequest) (runtimeingress.TransitionResult, error)
}

type okResult struct {
	OK bool `json:"ok"`
}

func OperatorRuntimeControlHandlers(opts RuntimeControlHandlerOptions) map[string]MethodHandler {
	if opts.Ingress == nil || opts.Idempotency == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"runtime.pause": func(ctx context.Context, req Request) (any, error) {
			return executeRuntimeIngressControl(ctx, req, opts, now().UTC(), true)
		},
		"runtime.resume": func(ctx context.Context, req Request) (any, error) {
			return executeRuntimeIngressControl(ctx, req, opts, now().UTC(), false)
		},
	}
}

func executeRuntimeIngressControl(ctx context.Context, req Request, opts RuntimeControlHandlerOptions, now time.Time, pause bool) (any, error) {
	if multiRuntimeContextMode(opts.RuntimeContexts) {
		return nil, runtimeContextRequiredError(req.Method, "runtime ingress control is not supported in multi-context DB-loaded mode without an explicit runtime context")
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		TTL:            runtimeControlIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		var result runtimeingress.TransitionResult
		var err error
		controlReq := runtimeingress.TransitionRequest{
			Reason:       "operator_request",
			ControlledBy: "api.v1",
			Now:          now,
		}
		if pause {
			result, err = opts.Ingress.Pause(ctx, controlReq)
		} else {
			result, err = opts.Ingress.Resume(ctx, controlReq)
		}
		if err != nil {
			return apiidempotency.Completion{}, runtimeIngressControlError(err)
		}
		response, err := json.Marshal(okResult{OK: true})
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{
			ResourceID: string(result.Status),
			Response:   response,
		}, nil
	})
	if err != nil {
		return nil, runtimeIngressControlError(err)
	}
	var stored okResult
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		if replay {
			return nil, fmt.Errorf("decode %s idempotency response: %w", req.Method, err)
		}
		return nil, fmt.Errorf("decode %s response: %w", req.Method, err)
	}
	return stored, nil
}

func runtimeIngressControlError(err error) error {
	switch {
	case errors.Is(err, runtimeingress.ErrAlreadyPaused):
		return NewApplicationError(RuntimeAlreadyPausedCode, false, nil)
	case errors.Is(err, runtimeingress.ErrNotPaused):
		return NewApplicationError(RuntimeNotPausedCode, false, nil)
	}
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
