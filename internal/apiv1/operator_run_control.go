package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimeruncontrol "swarm/internal/runtime/runcontrol"
	"swarm/internal/store"
)

const runControlIdempotencyTTL = 24 * time.Hour

type RunControlController interface {
	Stop(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.TransitionResult, error)
	Pause(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.TransitionResult, error)
	Continue(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.TransitionResult, error)
}

func OperatorRunControlHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.RunControl == nil || opts.Idempotency == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"run.stop": func(ctx context.Context, req Request) (any, error) {
			return executeRunControl(ctx, req, opts, now().UTC(), "stop")
		},
		"run.pause": func(ctx context.Context, req Request) (any, error) {
			return executeRunControl(ctx, req, opts, now().UTC(), "pause")
		},
		"run.continue": func(ctx context.Context, req Request) (any, error) {
			return executeRunControl(ctx, req, opts, now().UTC(), "continue")
		},
	}
}

func executeRunControl(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time, action string) (any, error) {
	runID, err := runControlRunIDParam(req.Params)
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
		ResourceID:     runID,
		TTL:            runControlIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		selectedOpts := opts
		if runtimeContextManager(opts) != nil {
			var err error
			ctx, selectedOpts, _, err = runtimeBundleContextByRun(ctx, opts, runID)
			if err != nil {
				return store.APIIdempotencyCompletion{}, err
			}
		}
		controlReq := runtimeruncontrol.TransitionRequest{
			RunID:        runID,
			Reason:       "operator_request",
			ControlledBy: "api.v1",
			Now:          now,
		}
		var result runtimeruncontrol.TransitionResult
		var err error
		switch action {
		case "stop":
			result, err = selectedOpts.RunControl.Stop(ctx, controlReq)
		case "pause":
			result, err = selectedOpts.RunControl.Pause(ctx, controlReq)
		case "continue":
			result, err = selectedOpts.RunControl.Continue(ctx, controlReq)
		default:
			err = fmt.Errorf("unsupported run control action %q", action)
		}
		if err != nil {
			return store.APIIdempotencyCompletion{}, runControlError(runID, err)
		}
		response, err := json.Marshal(okResult{OK: true})
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: result.RunID, Response: response}, nil
	})
	if err != nil {
		return nil, runControlError(runID, err)
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

func runControlRunIDParam(params map[string]any) (string, error) {
	runID := stringParam(params, "run_id")
	if runID == "" {
		return "", NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "required parameter is missing"})
	}
	parsed, err := uuid.Parse(runID)
	if err != nil {
		return "", NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "must be a UUID"})
	}
	return parsed.String(), nil
}

func runControlError(runID string, err error) error {
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
	var stateErr *runtimeruncontrol.StateError
	if errors.As(err, &stateErr) && stateErr != nil {
		if candidate := strings.TrimSpace(stateErr.RunID); candidate != "" {
			runID = candidate
		}
		currentStatus := strings.TrimSpace(stateErr.CurrentStatus)
		switch {
		case errors.Is(stateErr.Err, runtimeruncontrol.ErrRunNotFound):
			return NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
		case errors.Is(stateErr.Err, runtimeruncontrol.ErrAlreadyTerminal):
			return NewApplicationError(RunAlreadyTerminalCode, false, map[string]any{"run_id": runID, "current_status": currentStatus})
		case errors.Is(stateErr.Err, runtimeruncontrol.ErrAlreadyPaused):
			return NewApplicationError(RunAlreadyPausedCode, false, map[string]any{"run_id": runID})
		case errors.Is(stateErr.Err, runtimeruncontrol.ErrNotPaused):
			return NewApplicationError(RunNotPausedCode, false, map[string]any{"run_id": runID, "current_status": currentStatus})
		}
	}
	switch {
	case errors.Is(err, runtimeruncontrol.ErrRunNotFound):
		return NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
	case errors.Is(err, runtimeruncontrol.ErrAlreadyTerminal):
		return NewApplicationError(RunAlreadyTerminalCode, false, map[string]any{"run_id": runID})
	case errors.Is(err, runtimeruncontrol.ErrAlreadyPaused):
		return NewApplicationError(RunAlreadyPausedCode, false, map[string]any{"run_id": runID})
	case errors.Is(err, runtimeruncontrol.ErrNotPaused):
		return NewApplicationError(RunNotPausedCode, false, map[string]any{"run_id": runID})
	default:
		return err
	}
}
