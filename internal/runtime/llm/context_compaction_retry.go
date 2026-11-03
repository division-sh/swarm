package llm

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func confirmedContextWindowRejection(dispatch *completionDispatch, raw []byte) bool {
	if dispatch == nil || dispatch.handle == nil || dispatch.mutationErr != nil || dispatch.invocation != completionProviderInvocationStarted || dispatch.state != runtimeeffects.StateTerminalFailure || dispatch.evidence["status"] != 400 {
		return false
	}
	canonical, err := canonicaljson.Canonicalize(raw)
	if err != nil {
		return false
	}
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
		Usage json.RawMessage `json:"usage"`
	}
	return json.Unmarshal(canonical, &response) == nil && response.Error.Code == "context_length_exceeded" && (len(response.Usage) == 0 || string(response.Usage) == "null")
}

func (adapter contextCompactionAdapter) retryRejectedContext(ctx context.Context, session *Session, lease *sessions.Lease, input Message, managed *managedProviderCall, model llmselection.ResolvedModel, raw []byte, dispatch *completionDispatch) ([]byte, bool, error) {
	if !session.Memory.Enabled || managed == nil || managed.resume != nil || managed.frame.Turn.Kind == agentframe.TurnToolContinuation || !confirmedContextWindowRejection(dispatch, raw) {
		return nil, false, nil
	}
	failure := runtimefailures.New(runtimefailures.ClassSchemaInvalid, "provider_context_window_exceeded", "llm-context", "provider_rejected_context", map[string]any{"status": 400})
	envelope := runtimefailures.FromError(failure, "llm-context", "provider_rejected_context")
	// A confirmed invalid request produced no model turn or spend. Retire its
	// physical attempt before a single fresh, normally admitted request.
	if err := dispatch.handle.Settle(context.WithoutCancel(ctx), runtimeeffects.StateTerminalFailure, &envelope.Failure, map[string]any{"context_window_rejected": true, "response_fingerprint": runtimeeffects.Fingerprint(raw)}); err != nil {
		return nil, true, errors.Join(failure, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	if err := compactContext(ctx, session, lease, input, managed, model, adapter, true); err != nil {
		return nil, true, err
	}
	if err := bindFrozenContextCheckpoint(session, managed); err != nil {
		return nil, true, err
	}
	payload, err := adapter.build(session, input, model.ConcreteModel)
	return payload, true, err
}
