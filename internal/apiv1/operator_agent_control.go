package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

const agentControlIdempotencyTTL = 24 * time.Hour

type AgentControlController interface {
	runtimeagentcontrol.Controller
}

type agentDirectiveResult struct {
	OK                 bool   `json:"ok"`
	OperationID        string `json:"operation_id"`
	Response           string `json:"response,omitempty"`
	RunID              string `json:"run_id"`
	RunIDResolution    string `json:"run_id_resolution"`
	DirectiveEventID   string `json:"directive_event_id"`
	DirectiveEventType string `json:"directive_event_type"`
}

func OperatorAgentControlHandlers(opts AgentControlHandlerOptions) map[string]MethodHandler {
	if opts.Controller == nil || opts.Idempotency == nil {
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
	}
}

func executeAgentSendDirective(ctx context.Context, req Request, opts AgentControlHandlerOptions, now time.Time) (any, error) {
	_ = now
	agentID, err := requiredStringParam(req.Params, "agent_id")
	if err != nil {
		return nil, err
	}
	flowInstance, _, err := optionalStringParam(req.Params, "flow_instance")
	if err != nil {
		return nil, err
	}
	directive, err := requiredStringParam(req.Params, "directive")
	if err != nil {
		return nil, err
	}
	runID, _, err := optionalStringParam(req.Params, "run_id")
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	controller := opts.Controller
	if runtimeContextManager(opts.RuntimeContexts) != nil {
		if strings.TrimSpace(runID) == "" && multiRuntimeContextMode(opts.RuntimeContexts) {
			return nil, runtimeContextRequiredError(req.Method, "run_id is required to select a runtime context in multi-context DB-loaded mode")
		}
		if strings.TrimSpace(runID) != "" {
			var err error
			var selected selectedRuntimeContext
			ctx, selected, _, err = runtimeBundleContextByRun(ctx, opts.RuntimeContexts, runID)
			if err != nil {
				return nil, err
			}
			if selected.Runtime == nil || selected.Runtime.Manager == nil {
				return nil, fmt.Errorf("agent control owner is required for the selected runtime")
			}
			controller = selected.Runtime.Manager
		}
	}
	result, err := controller.SendDirective(ctx, runtimeagentcontrol.SendDirectiveRequest{
		AgentID:        agentID,
		FlowInstance:   flowInstance,
		Directive:      directive,
		RunID:          runID,
		Source:         runtimeagentcontrol.DirectiveSourceV1RPC,
		OperatorID:     req.ActorTokenID,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
	})
	if err != nil {
		return nil, agentControlError(req.Method, agentID, err)
	}
	return agentDirectiveResult{
		OK:                 true,
		OperationID:        result.OperationID,
		Response:           result.Response,
		RunID:              result.RunID,
		RunIDResolution:    result.RunIDResolution,
		DirectiveEventID:   result.DirectiveEventID,
		DirectiveEventType: result.DirectiveEventType,
	}, nil
}

func executeAgentRestart(ctx context.Context, req Request, opts AgentControlHandlerOptions, now time.Time) (any, error) {
	if multiRuntimeContextMode(opts.RuntimeContexts) {
		return nil, runtimeContextRequiredError(req.Method, "agent restart is not supported in multi-context DB-loaded mode without an explicit runtime context")
	}
	agentID, err := requiredStringParam(req.Params, "agent_id")
	if err != nil {
		return nil, err
	}
	flowInstance, _, err := optionalStringParam(req.Params, "flow_instance")
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	operationID := ""
	if strings.TrimSpace(idempotencyKey) != "" {
		operationID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{req.Method, req.ActorTokenID, idempotencyKey}, "\x00"))).String()
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     agentControlResourceID(agentID, flowInstance),
		TTL:            agentControlIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		result, err := opts.Controller.Restart(ctx, runtimeagentcontrol.RestartRequest{AgentID: agentID, FlowInstance: flowInstance, OperationID: operationID})
		if err != nil {
			return apiidempotency.Completion{}, agentControlError(req.Method, agentID, err)
		}
		response, err := json.Marshal(okResult{OK: true})
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{ResourceID: result.AgentID, Response: response}, nil
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

func agentControlResourceID(agentID, flowInstance string) string {
	return strings.TrimSpace(agentID) + "\x00" + strings.Trim(strings.TrimSpace(flowInstance), "/")
}

func agentControlError(method, agentID string, err error) error {
	var directiveConflict *runtimeagentcontrol.DirectiveIdempotencyConflictError
	if errors.As(err, &directiveConflict) {
		return NewApplicationError(IdempotencyConflictCode, false, map[string]any{
			"original_request_hash":    directiveConflict.OriginalRequestHash,
			"conflicting_request_hash": directiveConflict.ConflictingRequestHash,
			"original_response_ref": map[string]any{
				"method":      runtimeagentcontrol.DirectiveOperationMethod,
				"resource_id": directiveConflict.OperationID,
			},
		})
	}
	var operationErr *runtimeagentcontrol.DirectiveOperationError
	if errors.As(err, &operationErr) && operationErr != nil {
		details, detailsErr := directiveOperationApplicationDetails(operationErr.Operation)
		if detailsErr != nil {
			return detailsErr
		}
		switch {
		case errors.Is(operationErr.Err, runtimeagentcontrol.ErrDirectiveInProgress):
			return NewApplicationError(AgentDirectiveInProgressCode, true, details)
		case errors.Is(operationErr.Err, runtimeagentcontrol.ErrDirectiveCompletionPending):
			return NewApplicationError(AgentDirectiveCompletionPendingCode, true, details)
		case errors.Is(operationErr.Err, runtimeagentcontrol.ErrDirectiveExecutionFailed):
			return NewApplicationError(AgentDirectiveExecutionFailedCode, false, details)
		case errors.Is(operationErr.Err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate):
			return NewApplicationError(AgentDirectiveOutcomeIndeterminateCode, false, details)
		}
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
		case errors.Is(stateErr.Err, runtimeagentcontrol.ErrRunNotFound) && method == "agent.send_directive":
			return NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": stateErr.RunID})
		case errors.Is(stateErr.Err, runtimeagentcontrol.ErrRunAlreadyTerminal) && method == "agent.send_directive":
			return NewApplicationError(RunAlreadyTerminalCode, false, map[string]any{
				"run_id":         stateErr.RunID,
				"current_status": stateErr.CurrentStatus,
			})
		case errors.Is(stateErr.Err, runtimeagentcontrol.ErrAmbiguousRunTarget) && method == "agent.send_directive":
			details := map[string]any{
				"agent_id":        agentID,
				"active_sessions": activeSessionDetails(stateErr.ActiveSessions),
			}
			if runID := strings.TrimSpace(stateErr.RunID); runID != "" {
				details["run_id"] = runID
			}
			return NewApplicationError(AmbiguousRunTargetCode, false, details)
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
	case errors.Is(err, runtimeagentcontrol.ErrRunNotFound) && method == "agent.send_directive":
		return NewApplicationError(RunNotFoundCode, false, nil)
	case errors.Is(err, runtimeagentcontrol.ErrRunAlreadyTerminal) && method == "agent.send_directive":
		return NewApplicationError(RunAlreadyTerminalCode, false, nil)
	case errors.Is(err, runtimeagentcontrol.ErrAmbiguousRunTarget) && method == "agent.send_directive":
		return NewApplicationError(AmbiguousRunTargetCode, false, map[string]any{"agent_id": agentID})
	default:
		return err
	}
}

func directiveOperationApplicationDetails(op runtimeagentcontrol.DirectiveOperation) (map[string]any, error) {
	op = op.Normalized()
	if err := runtimeagentcontrol.ValidateDirectiveOperationEvidence(op); err != nil {
		return nil, err
	}
	details := map[string]any{
		"operation_id":       op.OperationID,
		"directive_event_id": op.DirectiveEventID,
		"run_id":             op.ResolvedRunID,
		"state":              string(op.State),
	}
	if !op.ExecutionLeaseExpiresAt.IsZero() {
		details["lease_expires_at"] = op.ExecutionLeaseExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	if op.Failure != nil {
		details["failure"] = runtimefailures.CloneEnvelope(op.Failure)
	}
	return details, nil
}

func activeSessionDetails(sessions []runtimeagentcontrol.ActiveSessionTarget) []map[string]any {
	out := make([]map[string]any, 0, len(sessions))
	for _, session := range sessions {
		item := map[string]any{}
		if sessionID := strings.TrimSpace(session.SessionID); sessionID != "" {
			item["session_id"] = sessionID
		}
		if runID := strings.TrimSpace(session.RunID); runID != "" {
			item["run_id"] = runID
		}
		if len(item) > 0 {
			out = append(out, item)
		}
	}
	return out
}
