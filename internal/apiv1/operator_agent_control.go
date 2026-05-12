package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	runtimeagentcontrol "swarm/internal/runtime/agentcontrol"
	"swarm/internal/store"
)

const agentControlIdempotencyTTL = 24 * time.Hour

type AgentControlController interface {
	runtimeagentcontrol.Controller
}

type agentDirectiveResult struct {
	OK       bool   `json:"ok"`
	Response string `json:"response,omitempty"`
}

type agentReplayBacklogResult struct {
	OK            bool `json:"ok"`
	ReplayedCount int  `json:"replayed_count"`
}

func OperatorAgentControlHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.AgentControl == nil || opts.Idempotency == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"agent.send_directive": func(ctx context.Context, req Request) (any, error) {
			return executeAgentSendDirective(ctx, req, opts, now().UTC())
		},
		"agent.restart": func(ctx context.Context, req Request) (any, error) {
			return executeAgentRestart(ctx, req, opts, now().UTC())
		},
		"agent.replay_backlog": func(ctx context.Context, req Request) (any, error) {
			return executeAgentReplayBacklog(ctx, req, opts, now().UTC())
		},
	}
}

func executeAgentSendDirective(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	agentID, err := requiredStringParam(req.Params, "agent_id")
	if err != nil {
		return nil, err
	}
	directive, err := requiredStringParam(req.Params, "directive")
	if err != nil {
		return nil, err
	}
	killPrevious, err := optionalBoolParam(req.Params, "kill_previous")
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
		ResourceID:     agentID,
		TTL:            agentControlIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		result, err := opts.AgentControl.SendDirective(ctx, runtimeagentcontrol.SendDirectiveRequest{
			AgentID:      agentID,
			Directive:    directive,
			KillPrevious: killPrevious,
		})
		if err != nil {
			return store.APIIdempotencyCompletion{}, agentControlError(req.Method, agentID, err)
		}
		response, err := json.Marshal(agentDirectiveResult{OK: true, Response: result.Response})
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: result.AgentID, Response: response}, nil
	})
	if err != nil {
		return nil, agentControlError(req.Method, agentID, err)
	}
	var stored agentDirectiveResult
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		if replay {
			return nil, fmt.Errorf("decode %s idempotency response: %w", req.Method, err)
		}
		return nil, fmt.Errorf("decode %s response: %w", req.Method, err)
	}
	return stored, nil
}

func executeAgentRestart(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	agentID, err := requiredStringParam(req.Params, "agent_id")
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
		ResourceID:     agentID,
		TTL:            agentControlIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		result, err := opts.AgentControl.Restart(ctx, runtimeagentcontrol.RestartRequest{AgentID: agentID})
		if err != nil {
			return store.APIIdempotencyCompletion{}, agentControlError(req.Method, agentID, err)
		}
		response, err := json.Marshal(okResult{OK: true})
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: result.AgentID, Response: response}, nil
	})
	if err != nil {
		return nil, agentControlError(req.Method, agentID, err)
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

func executeAgentReplayBacklog(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	agentID, err := requiredStringParam(req.Params, "agent_id")
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
		ResourceID:     agentID,
		TTL:            agentControlIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		result, err := opts.AgentControl.ReplayBacklog(ctx, runtimeagentcontrol.ReplayBacklogRequest{AgentID: agentID})
		if err != nil {
			return store.APIIdempotencyCompletion{}, agentControlError(req.Method, agentID, err)
		}
		response, err := json.Marshal(agentReplayBacklogResult{OK: true, ReplayedCount: result.ReplayedCount})
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: result.AgentID, Response: response}, nil
	})
	if err != nil {
		return nil, agentControlError(req.Method, agentID, err)
	}
	var stored agentReplayBacklogResult
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		if replay {
			return nil, fmt.Errorf("decode %s idempotency response: %w", req.Method, err)
		}
		return nil, fmt.Errorf("decode %s response: %w", req.Method, err)
	}
	return stored, nil
}

func optionalBoolParam(params map[string]any, name string) (bool, error) {
	raw, ok := params[name]
	if !ok || isEmptyParam(raw) {
		return false, nil
	}
	value, ok := raw.(bool)
	if !ok {
		return false, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be a boolean"})
	}
	return value, nil
}

func agentControlError(method, agentID string, err error) error {
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
	var stateErr *runtimeagentcontrol.StateError
	if errors.As(err, &stateErr) && stateErr != nil {
		if candidate := stateErr.AgentID; candidate != "" {
			agentID = candidate
		}
		switch {
		case errors.Is(stateErr.Err, runtimeagentcontrol.ErrAgentNotFound):
			return NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
		case errors.Is(stateErr.Err, runtimeagentcontrol.ErrAgentNotRunning) && method == "agent.send_directive":
			return NewApplicationError(AgentNotRunningCode, false, map[string]any{
				"agent_id":       agentID,
				"current_status": stateErr.CurrentStatus,
			})
		}
	}
	switch {
	case errors.Is(err, runtimeagentcontrol.ErrAgentNotFound):
		return NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
	case errors.Is(err, runtimeagentcontrol.ErrAgentNotRunning) && method == "agent.send_directive":
		return NewApplicationError(AgentNotRunningCode, false, map[string]any{
			"agent_id":       agentID,
			"current_status": runtimeagentcontrol.StatusTerminated,
		})
	default:
		return err
	}
}
