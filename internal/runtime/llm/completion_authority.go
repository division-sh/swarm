package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/google/uuid"
)

type completionDispatch struct {
	handle   *runtimeeffects.Handle
	state    runtimeeffects.State
	evidence map[string]any
}

const (
	completionAttemptHeartbeatInterval = 30 * time.Second
	completionAttemptHeartbeatLease    = 2 * time.Minute
)

type completionAttemptHeartbeat struct {
	cancel  context.CancelCauseFunc
	done    chan struct{}
	mu      sync.Mutex
	err     error
	stopped bool
}

type completionAttemptHeartbeatContextKey struct{}

func startCompletionAttemptHeartbeat(ctx context.Context, handle *runtimeeffects.Handle) (context.Context, *completionAttemptHeartbeat, error) {
	return startCompletionAttemptHeartbeatWithTiming(ctx, handle, completionAttemptHeartbeatInterval, completionAttemptHeartbeatLease)
}

func startCompletionAttemptHeartbeatWithTiming(ctx context.Context, handle *runtimeeffects.Handle, interval, lease time.Duration) (context.Context, *completionAttemptHeartbeat, error) {
	if handle == nil {
		return ctx, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_effect_handle_missing", "llm-completion-authority", "heartbeat_attempt", nil)
	}
	if interval <= 0 || lease <= 0 {
		return ctx, nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "completion_heartbeat_timing_invalid", "llm-completion-authority", "heartbeat_attempt", nil)
	}
	if err := handle.Heartbeat(ctx, lease); err != nil {
		return ctx, nil, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_attempt_heartbeat_failed", "llm-completion-authority", "heartbeat_attempt", map[string]any{"stage": "prelaunch"}, err)
	}
	heartbeatCtx, cancel := context.WithCancelCause(ctx)
	heartbeat := &completionAttemptHeartbeat{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(heartbeat.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := handle.Heartbeat(heartbeatCtx, lease); err != nil {
					if heartbeatCtx.Err() != nil {
						return
					}
					heartbeat.mu.Lock()
					heartbeat.err = err
					heartbeat.mu.Unlock()
					cancel(err)
					return
				}
			}
		}
	}()
	return context.WithValue(heartbeatCtx, completionAttemptHeartbeatContextKey{}, heartbeat), heartbeat, nil
}

func requireCompletionAttemptHeartbeat(ctx context.Context) error {
	if ctx == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_attempt_heartbeat_missing", "llm-completion-authority", "launch_attempt", nil)
	}
	heartbeat, ok := ctx.Value(completionAttemptHeartbeatContextKey{}).(*completionAttemptHeartbeat)
	if !ok || heartbeat == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_attempt_heartbeat_missing", "llm-completion-authority", "launch_attempt", nil)
	}
	return nil
}

func (h *completionAttemptHeartbeat) Stop() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if !h.stopped {
		h.stopped = true
		h.cancel(nil)
	}
	h.mu.Unlock()
	<-h.done
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func finishCompletionAttemptHeartbeat(heartbeat *completionAttemptHeartbeat, prior error) error {
	if heartbeat == nil {
		return prior
	}
	heartbeatErr := heartbeat.Stop()
	if heartbeatErr == nil {
		return prior
	}
	return errors.Join(prior, completionAttemptHeartbeatLoss(heartbeatErr))
}

func finishCompletionDispatchHeartbeat(dispatch *completionDispatch, heartbeat *completionAttemptHeartbeat, prior error) error {
	if heartbeat == nil {
		return prior
	}
	heartbeatErr := heartbeat.Stop()
	if heartbeatErr == nil {
		return prior
	}
	if dispatch != nil {
		dispatch.state = runtimeeffects.StateOutcomeUncertain
	}
	return errors.Join(prior, completionAttemptHeartbeatLoss(heartbeatErr))
}

func completionAttemptHeartbeatLoss(err error) error {
	return runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "completion_attempt_heartbeat_lost", "llm-completion-authority", "heartbeat_attempt", map[string]any{"stage": "provider_execution"}, err)
}

func prepareCompletionContext(ctx context.Context, controller *runtimeeffects.Controller, cfg *config.Config, session *Session, entityID string) (context.Context, string, error) {
	if controller == nil || !controller.Enabled() {
		return ctx, "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_execution_controller_missing", "llm-completion-authority", "prepare_completion", nil)
	}
	if session == nil {
		return ctx, "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_session_missing", "llm-completion-authority", "prepare_completion", nil)
	}
	ctx = runtimeeffects.WithLogicalOperationIdentitySegment(ctx, fmt.Sprintf("completion:%s:%d", strings.TrimSpace(session.ID), session.TurnCount+1))
	ctx = runtimeeffects.WithController(ctx, controller)
	authority, ok := runtimeeffects.CompletionAuthorityFromContext(ctx)
	if !ok {
		return ctx, "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_execution_authority_missing", "llm-completion-authority", "prepare_completion", nil)
	}
	var target runtimeeffects.UsageTarget
	if authority.Kind == runtimeeffects.AuthorityConversationForkChat {
		ordinal := 1
		if session != nil {
			ordinal = session.TurnCount + 1
		}
		target = runtimeeffects.UsageTarget{Kind: runtimeeffects.UsageTargetConversationForkCompletion, ID: authority.ForkChat.ForkTurnID, Ordinal: ordinal}
	} else {
		actor, _ := runtimeactors.ActorFromContext(ctx)
		target = runtimeeffects.UsageTarget{
			Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(),
			RunID: strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx)), AgentID: strings.TrimSpace(session.AgentID),
			SessionID: strings.TrimSpace(session.ID), Memory: session.Memory,
			FlowInstance: strings.TrimSpace(actor.CanonicalFlowPath()), EntityID: strings.TrimSpace(actor.EffectiveEntityID()),
		}
		if session.Memory.Enabled {
			target.RunID = session.MemoryIdentity.RunID
			target.FlowInstance = session.MemoryIdentity.FlowInstance
		}
	}
	ctx = runtimeeffects.WithUsageTarget(ctx, target)
	ctx = runtimeeffects.WithBudgetAdmissionScopes(ctx, completionBudgetScopes(cfg, entityID))
	authority, ok = runtimeeffects.CompletionAuthorityFromContext(ctx)
	if !ok || !authority.Target.Valid() {
		return ctx, "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_usage_target_missing", "llm-completion-authority", "prepare_completion", nil)
	}
	return ctx, target.ID, nil
}

func completionBudgetScopes(cfg *config.Config, entityID string) []runtimeeffects.BudgetAdmissionScope {
	if cfg == nil {
		return nil
	}
	budget := cfg.Budget()
	scopes := make([]runtimeeffects.BudgetAdmissionScope, 0, 2)
	if budget.SystemMonthlyCap > 0 {
		scopes = append(scopes, runtimeeffects.BudgetAdmissionScope{Kind: "system", CapUSD: float64(budget.SystemMonthlyCap)})
	}
	if entityID = strings.TrimSpace(entityID); entityID != "" && budget.PerEntityMonthlyCap > 0 {
		scopes = append(scopes, runtimeeffects.BudgetAdmissionScope{Kind: "entity", Key: entityID, CapUSD: float64(budget.PerEntityMonthlyCap)})
	} else if entityID == "" && budget.GlobalMonthlyCap > 0 {
		scopes = append(scopes, runtimeeffects.BudgetAdmissionScope{Kind: "global", CapUSD: float64(budget.GlobalMonthlyCap)})
	}
	return scopes
}

func settleCompletionTurn(ctx context.Context, dispatch *completionDispatch, targetID string, turn AgentTurnRecord, response *Response, profile llmselection.Profile, usage runtimeeffects.CompletionUsage, state runtimeeffects.State, failure *runtimefailures.Envelope, evidence map[string]any) error {
	return settleCompletionTurnWithProviderHead(ctx, dispatch, targetID, turn, response, profile, usage, state, failure, evidence, nil)
}

func settleCompletionTurnWithProviderHead(ctx context.Context, dispatch *completionDispatch, targetID string, turn AgentTurnRecord, response *Response, profile llmselection.Profile, usage runtimeeffects.CompletionUsage, state runtimeeffects.State, failure *runtimefailures.Envelope, evidence map[string]any, providerHead *runtimeeffects.CompletionProviderHead) error {
	if dispatch == nil || dispatch.handle == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_effect_handle_missing", "llm-completion-authority", "settle_completion", nil)
	}
	// The dispatch state can narrow a provider-call failure to a proven
	// prelaunch failure. A successful transport does not make later response
	// conversion, usage validation, or target persistence successful.
	if dispatch.state != "" && dispatch.state != runtimeeffects.StateSettled {
		state = dispatch.state
	}
	if len(dispatch.evidence) > 0 {
		if evidence == nil {
			evidence = map[string]any{}
		}
		for key, value := range dispatch.evidence {
			evidence[key] = value
		}
	}
	turn = enrichTurnRecord(ctx, nil, turn, response)
	turn = CanonicalizeTurnForPersistence(turn)
	if usage.ResolvedModel == "" {
		usage.ResolvedModel = "unknown"
	}
	if state != runtimeeffects.StateSettled && failure == nil {
		envelope := runtimefailures.FromError(fmt.Errorf("completion failed without provider failure detail"), "llm-completion-authority", "settle_completion")
		failure = &envelope.Failure
	}
	turn.Failure = failure
	settlement := runtimeeffects.CompletionSettlement{
		Settlement:   runtimeeffects.Settlement{State: state, Failure: failure, Evidence: evidence},
		Usage:        usage,
		Spend:        completionSpendForContext(ctx, profile, turn, usage),
		ProviderHead: providerHead,
		Now:          time.Now().UTC(),
	}
	if authority := dispatch.handle.Attempt().Authority; authority.Target.Kind == runtimeeffects.UsageTargetAgentTurn {
		settlement.AgentTurn = completionAgentTurn(targetID, turn)
	}
	return dispatch.handle.SettleCompletion(ctx, settlement)
}

func completionAgentTurn(targetID string, turn AgentTurnRecord) *runtimeeffects.CompletionAgentTurn {
	latency := int(turn.Latency / time.Millisecond)
	if latency < 0 {
		latency = 0
	}
	return &runtimeeffects.CompletionAgentTurn{
		TurnID:           targetID,
		RunID:            strings.TrimSpace(turn.RunID),
		AgentID:          strings.TrimSpace(turn.AgentID),
		SessionID:        strings.TrimSpace(turn.SessionID),
		Memory:           turn.Memory,
		FlowInstance:     strings.TrimSpace(turn.FlowInstance),
		EntityID:         strings.TrimSpace(turn.EntityID),
		TriggerEventID:   strings.TrimSpace(turn.TriggerEventID),
		TriggerEventType: strings.TrimSpace(turn.TriggerEventType),
		TaskID:           strings.TrimSpace(turn.TaskID),
		AvailableTools:   completionMarshal(turn.AvailableTools, `[]`),
		ToolCalls:        completionMarshal(turn.ToolCalls, `[]`),
		EmittedEvents:    completionMarshal(turn.EmittedEvents, `[]`),
		MCPServers:       completionMarshal(turn.MCPServers, `{}`),
		MCPToolsListed:   completionMarshal(turn.MCPToolsListed, `[]`),
		MCPToolsVisible:  completionMarshal(turn.MCPToolsVisible, `[]`),
		RequestPayload:   completionRaw(turn.RequestPayload),
		ResponsePayload:  completionRaw(turn.ResponseRaw),
		TurnBlocks:       completionMarshal(turn.TurnBlocks, `[]`),
		ParseOK:          turn.ParseOK,
		LatencyMS:        latency,
		RetryCount:       turn.RetryCount,
		Failure:          turn.Failure,
	}
}

func completionSpendForContext(ctx context.Context, profile llmselection.Profile, turn AgentTurnRecord, usage runtimeeffects.CompletionUsage) runtimeeffects.CompletionSpend {
	meta := usageMetadataForContext(ctx, profile, usage.ResolvedModel)
	actor, _ := runtimeactors.ActorFromContext(ctx)
	flowInstance := strings.TrimSpace(turn.FlowInstance)
	if flowInstance == "" {
		flowInstance = strings.TrimSpace(actor.CanonicalFlowPath())
	}
	if flowInstance == "" {
		flowInstance = "global"
	}
	input, output := int64(0), int64(0)
	if usage.InputTokens != nil {
		input = *usage.InputTokens
	}
	if usage.OutputTokens != nil {
		output = *usage.OutputTokens
	}
	cost := estimateCompletionCostUSD(usage.ResolvedModel, input, output)
	if usage.ProviderReportedCostUSD != nil {
		cost = *usage.ProviderReportedCostUSD
	}
	return runtimeeffects.CompletionSpend{
		EntityID:       strings.TrimSpace(turn.EntityID),
		FlowInstance:   flowInstance,
		AgentID:        strings.TrimSpace(turn.AgentID),
		Model:          usage.ResolvedModel,
		ModelAlias:     mapString(meta, "model_alias"),
		BackendProfile: coalesce(mapString(meta, "backend_profile"), profile.ID),
		Provider:       coalesce(mapString(meta, "provider"), profile.Provider),
		Transport:      coalesce(mapString(meta, "transport"), profile.Transport),
		ResolvedModel:  coalesce(mapString(meta, "resolved_model"), usage.ResolvedModel),
		CostUSD:        cost,
		InvocationType: profile.ID,
	}
}

func completionUsage(input, output int, model string, exactness runtimeeffects.CompletionUsageExactness) runtimeeffects.CompletionUsage {
	in, out := int64(input), int64(output)
	return runtimeeffects.CompletionUsage{ResolvedModel: strings.TrimSpace(model), Exactness: exactness, InputTokens: &in, OutputTokens: &out}
}

func unavailableCompletionUsage(model string) runtimeeffects.CompletionUsage {
	return runtimeeffects.CompletionUsage{ResolvedModel: coalesce(strings.TrimSpace(model), "unknown"), Exactness: runtimeeffects.CompletionUsageUnavailable}
}

func claudeCompletionUsageFromRaw(raw []byte, fallbackModel string) (runtimeeffects.CompletionUsage, error) {
	type resultMessage struct {
		Type         string   `json:"type"`
		Model        string   `json:"model"`
		TotalCostUSD *float64 `json:"total_cost_usd"`
		Usage        struct {
			InputTokens              *int64 `json:"input_tokens"`
			OutputTokens             *int64 `json:"output_tokens"`
			CacheReadInputTokens     *int64 `json:"cache_read_input_tokens"`
			CacheCreationInputTokens *int64 `json:"cache_creation_input_tokens"`
			CacheCreation            struct {
				Ephemeral5mInputTokens *int64 `json:"ephemeral_5m_input_tokens"`
				Ephemeral1hInputTokens *int64 `json:"ephemeral_1h_input_tokens"`
			} `json:"cache_creation"`
		} `json:"usage"`
	}
	decode := func(line []byte) (resultMessage, bool) {
		var result resultMessage
		if json.Unmarshal(bytes.TrimSpace(line), &result) != nil || strings.TrimSpace(strings.ToLower(result.Type)) != "result" {
			return resultMessage{}, false
		}
		return result, true
	}
	var terminal resultMessage
	found := false
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
		if result, ok := decode(line); ok {
			terminal, found = result, true
		}
	}
	if !found {
		if result, ok := decode(raw); ok {
			terminal, found = result, true
		}
	}
	if !found || terminal.Usage.InputTokens == nil || terminal.Usage.OutputTokens == nil {
		return runtimeeffects.CompletionUsage{}, fmt.Errorf("claude ResultMessage missing exact usage")
	}
	model := strings.TrimSpace(coalesce(terminal.Model, fallbackModel))
	if model == "" {
		return runtimeeffects.CompletionUsage{}, fmt.Errorf("claude ResultMessage completion model is unavailable")
	}
	usage := runtimeeffects.CompletionUsage{
		ResolvedModel:              model,
		Exactness:                  runtimeeffects.CompletionUsageExact,
		InputTokens:                terminal.Usage.InputTokens,
		OutputTokens:               terminal.Usage.OutputTokens,
		CacheReadInputTokens:       terminal.Usage.CacheReadInputTokens,
		CacheCreationInputTokens:   terminal.Usage.CacheCreationInputTokens,
		CacheCreation5mInputTokens: terminal.Usage.CacheCreation.Ephemeral5mInputTokens,
		CacheCreation1hInputTokens: terminal.Usage.CacheCreation.Ephemeral1hInputTokens,
		ProviderReportedCostUSD:    terminal.TotalCostUSD,
	}
	if err := usage.Validate(); err != nil {
		return runtimeeffects.CompletionUsage{}, fmt.Errorf("claude ResultMessage usage is invalid: %w", err)
	}
	return usage, nil
}

func completionFailure(err error, component, operation string) *runtimefailures.Envelope {
	if err == nil {
		return nil
	}
	envelope := runtimefailures.FromError(err, component, operation)
	return &envelope.Failure
}

func completionRaw(raw []byte) json.RawMessage {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func completionMarshal(value any, fallback string) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil || !json.Valid(raw) {
		return json.RawMessage(fallback)
	}
	return raw
}

func mapString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}

func estimateCompletionCostUSD(model string, input, output int64) float64 {
	inRate, outRate := 3.0, 15.0
	lower := strings.ToLower(strings.TrimSpace(model))
	if strings.Contains(lower, "haiku") {
		inRate, outRate = 0.8, 4.0
	} else if strings.Contains(lower, "opus") {
		inRate, outRate = 15.0, 75.0
	}
	if input < 0 {
		input = 0
	}
	if output < 0 {
		output = 0
	}
	return float64(input)/1_000_000*inRate + float64(output)/1_000_000*outRate
}

func completionTurnBase(ctx context.Context, session *Session, request, response []byte, parseOK bool, latency time.Duration, failure *runtimefailures.Envelope) AgentTurnRecord {
	actor, _ := runtimeactors.ActorFromContext(ctx)
	return AgentTurnRecord{
		AgentID:        session.AgentID,
		SessionID:      session.ID,
		RunID:          strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx)),
		EntityID:       actor.EffectiveEntityID(),
		FlowInstance:   actor.CanonicalFlowPath(),
		RequestPayload: request,
		ResponseRaw:    response,
		ParseOK:        parseOK,
		Latency:        latency,
		Failure:        failure,
	}
}
