package llm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

type contextCompactionAdapter struct {
	profile     llmselection.Profile
	destination string
	cfg         *config.Config
	controller  *runtimeeffects.Controller
	store       ConversationPersistence
	admission   *ProviderAdmissionRegistry
	publisher   EventPublisher
	build       func(*Session, Message, string) ([]byte, error)
	compact     func(*Session, string, string) ([]byte, error)
	send        func(context.Context, []byte, *managedProviderCall) ([]byte, *completionDispatch, error)
}

type contextCompactionPlan struct {
	prepared ContextCompactionPreparation
	payload  []byte
}

func compactContext(ctx context.Context, session *Session, lease *sessions.Lease, input Message, managed *managedProviderCall, model llmselection.ResolvedModel, adapter contextCompactionAdapter, force bool) error {
	if !session.Memory.Enabled || len(session.Messages) == 0 || managed == nil || managed.resume != nil || managed.frame.Turn.Kind == agentframe.TurnToolContinuation {
		return nil
	}
	if err := validateContextCompactionCheckpoints(session.ID, session.Messages, session.ContextCheckpoints); err != nil {
		return err
	}
	window, output := llmselection.ContextCompactionBudget(adapter.profile.ID, adapter.destination, model.ConcreteModel)
	budget, err := resolveContextCompactionBudget(window, output)
	if err != nil {
		if window == 0 && output == 0 && !force && (len(session.ContextCheckpoints) == 0 || session.ContextCheckpoints[len(session.ContextCheckpoints)-1].Method == "platform_summary") {
			logPublisherRuntime(ctx, adapter.publisher, "warn", "context_compaction_unavailable", "Automatic compaction has no documented window or output reservation for this route", session.AgentID, session.ID, managed.frame.Turn.Event.EntityID, map[string]any{"backend": adapter.profile.ID, "model": model.ConcreteModel}, err)
			return nil
		}
		return err
	}
	plan, err := planContextCompaction(session, input, managed, model, adapter, budget, force)
	if err != nil || plan == nil {
		return err
	}
	store, ok := adapter.store.(ContextCompactionPersistence)
	if !ok {
		return contextCompactionLimitation("checkpoint_owner_unavailable")
	}
	result, found, err := store.LoadContextCompaction(ctx, session.MemoryIdentity, plan.prepared)
	if err != nil {
		return err
	}
	if !found {
		result, err = executeContextCompaction(ctx, session, lease, managed, model, adapter, plan.prepared, plan.payload)
		if err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := store.InstallContextCompaction(ctx, lease, result); err != nil {
		return err
	}
	record := ConversationRecord{SessionID: session.ID, Messages: session.Messages[:session.acquiredCanonicalMessages], ContextCheckpoints: session.ContextCheckpoints}
	checkpoints, _, err := ContextCompactionInstallation(record, result)
	if err != nil {
		return err
	}
	session.ContextCheckpoints = checkpoints
	return nil
}

func planContextCompaction(session *Session, input Message, managed *managedProviderCall, model llmselection.ResolvedModel, adapter contextCompactionAdapter, budget contextCompactionBudget, force bool) (*contextCompactionPlan, error) {
	system, err := canonicaljson.Hash(session.SystemPrompt)
	if err != nil {
		return nil, err
	}
	tools, err := canonicaljson.Hash(session.Tools)
	if err != nil {
		return nil, err
	}
	parent, firstKept, rebind := "", 0, false
	priceBase := *session
	if len(session.ContextCheckpoints) != 0 {
		last := session.ContextCheckpoints[len(session.ContextCheckpoints)-1]
		parent, firstKept = last.ID, last.FirstKeptMessage
		rebind = last.Method != "platform_summary" && (last.Route != contextCompactionRoute(adapter.profile.ID, adapter.destination) || last.Model != model.ConcreteModel || last.SystemFingerprint != system || last.ToolsFingerprint != tools)
		if rebind {
			priceBase.ContextCheckpoints = nil
		}
	}
	request, err := adapter.build(&priceBase, input, model.ConcreteModel)
	if err != nil {
		return nil, err
	}
	pressure, err := contextCompactionRequestTokens(request)
	if err != nil {
		return nil, err
	}
	if !force && !rebind && pressure < budget.Trigger {
		return nil, nil
	}
	canonicalMessages := session.Messages[:session.acquiredCanonicalMessages]
	keep := budget.Keep - contextCompactionMessageTokens(input)
	for _, staged := range session.Messages[session.acquiredCanonicalMessages:] {
		keep -= contextCompactionMessageTokens(staged)
	}
	keep = max(0, keep)
	if force {
		keep = 0
	}
	cut, err := contextCompactionCut(canonicalMessages, 0, keep)
	if err != nil {
		return nil, err
	}
	cut = max(cut, firstKept)
	method := llmselection.NativeCompactionMethod(adapter.profile.ID, adapter.destination, model.ConcreteModel)
	if method == "" {
		method = "platform_summary"
	}
	prefix := priceBase
	prefix.Messages = append([]Message(nil), session.Messages[:cut]...)
	prefix.ContextCheckpoints = nil
	if !rebind && len(session.ContextCheckpoints) != 0 && session.ContextCheckpoints[len(session.ContextCheckpoints)-1].CoveredMessages <= cut {
		prefix.ContextCheckpoints = cloneContextCompactionCheckpoints(session.ContextCheckpoints)
	}
	payload, err := adapter.compact(&prefix, model.ConcreteModel, method)
	if err != nil {
		return nil, err
	}
	canonical, err := canonicaljson.Hash(session.Messages[:cut])
	if err != nil {
		return nil, err
	}
	base, err := canonicaljson.Hash(canonicalMessages)
	if err != nil {
		return nil, err
	}
	checkpoint := ContextCompactionCheckpoint{Version: 1, SessionID: session.ID, ParentID: parent, Method: method,
		Route: contextCompactionRoute(adapter.profile.ID, adapter.destination), Model: model.ConcreteModel,
		CoveredMessages: cut, FirstKeptMessage: cut, CanonicalFingerprint: canonical, SystemFingerprint: system, ToolsFingerprint: tools}
	key, err := canonicaljson.Hash([]any{checkpoint, base, managed.frame.Turn.Event.ID, runtimeeffects.Fingerprint(payload)})
	if err != nil {
		return nil, err
	}
	checkpoint.OperationID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("context-compaction:v1:"+key)).String()
	prepared := ContextCompactionPreparation{Checkpoint: checkpoint, ConversationFingerprint: base, RequestFingerprint: runtimeeffects.Fingerprint(payload)}
	return &contextCompactionPlan{prepared: prepared, payload: payload}, nil
}

func executeContextCompaction(ctx context.Context, session *Session, lease *sessions.Lease, managed *managedProviderCall, model llmselection.ResolvedModel, adapter contextCompactionAdapter, prepared ContextCompactionPreparation, payload []byte) (ContextCompactionResult, error) {
	ctx, _, err := prepareCompletionContext(ctx, adapter.controller, adapter.cfg, session, lease, managed.frame.Turn.Event.EntityID)
	if err != nil {
		return ContextCompactionResult{}, err
	}
	ctx = withCompletionRequest(ctx, adapter.profile, model)
	release, err := admitProviderRequest(ctx, adapter.admission, adapter.profile, model)
	if err != nil {
		return ContextCompactionResult{}, err
	}
	defer release()
	call := *managed
	call.compactionOperationID, call.compactionMethod = prepared.Checkpoint.OperationID, prepared.Checkpoint.Method
	raw, dispatch, callErr := adapter.send(ctx, payload, &call)
	if dispatch == nil {
		return ContextCompactionResult{}, callErr
	}
	dispatch.providerModel = model
	usage := contextCompactionUsage(adapter.profile.ID, prepared.Checkpoint.Method, raw)
	result := ContextCompactionResult{Preparation: prepared, Checkpoint: prepared.Checkpoint}
	if callErr == nil {
		if result.Checkpoint.Method == "platform_summary" {
			result.Checkpoint.Summary, callErr = contextCompactionSummaryResult(adapter.profile.ID, raw)
		} else {
			result.Checkpoint.Native, callErr = contextCompactionNativeResult(result.Checkpoint.Method, raw)
		}
	}
	result.Checkpoint.AttemptID = dispatch.handle.Attempt().AttemptID
	result.Checkpoint.ResultFingerprint = runtimeeffects.Fingerprint(raw)
	result.Checkpoint.ID, err = result.Checkpoint.Fingerprint()
	callErr = errors.Join(callErr, err)
	state := runtimeeffects.StateSettled
	var failure *runtimefailures.Envelope
	if callErr != nil {
		state = runtimeeffects.StateOutcomeUncertain
		if dispatch.state == runtimeeffects.StateTerminalFailure || len(raw) != 0 {
			state = runtimeeffects.StateTerminalFailure
		}
		classified := runtimefailures.FromError(callErr, "llm-context", "compact")
		failure = &classified.Failure
	}
	if dispatch.invocation == completionProviderInvocationNotStarted {
		return ContextCompactionResult{}, errors.Join(callErr, dispatch.handle.Settle(ctx, runtimeeffects.StateTerminalFailure, failure, nil))
	}
	usage, err = bindCompletionUsageEvidence(dispatch.handle.Attempt().Authority.CompletionRequest, usage)
	if err != nil {
		return ContextCompactionResult{}, errors.Join(callErr, err)
	}
	target := dispatch.handle.Attempt().Authority.Target
	inputTokens, outputTokens := int64(0), int64(0)
	if usage.InputTokens != nil {
		inputTokens = *usage.InputTokens
	}
	if usage.OutputTokens != nil {
		outputTokens = *usage.OutputTokens
	}
	cost := estimateCompletionCostUSD(model.ConcreteModel, inputTokens, outputTokens)
	if usage.ProviderReportedCostUSD != nil {
		cost = *usage.ProviderReportedCostUSD
	}
	settled, settleErr := dispatch.handle.SettleCompletion(ctx, runtimeeffects.CompletionSettlement{
		MaintenanceSurfaceID: managed.frame.Turn.Capability.SurfaceID,
		Settlement:           runtimeeffects.Settlement{State: state, Failure: failure, Evidence: map[string]any{"context_compaction": result}}, Usage: usage,
		Spend: runtimeeffects.CompletionSpend{EntityID: target.EntityID, FlowInstance: target.FlowInstance, AgentID: target.AgentID, AgentIdentity: target.AgentIdentity,
			Model: model.ConcreteModel, ModelAlias: model.ModelAlias, BackendProfile: adapter.profile.ID, Provider: adapter.profile.Provider, Transport: adapter.profile.Transport,
			ResolvedModel: usage.ResolvedModel, InvocationType: runtimeeffects.ContextCompactionPurpose, CostUSD: cost}, Now: time.Now().UTC(),
	})
	if !settled.Committed {
		return ContextCompactionResult{}, errors.Join(callErr, unacknowledgedCompletionError(settleErr))
	}
	if settled.NoCurrentProjection() {
		return ContextCompactionResult{}, errors.Join(callErr, settleErr, contextCompactionLimitation("maintenance_has_no_current_projection"))
	}
	return result, errors.Join(callErr, settleErr, dispatch.mutationErr)
}

func contextCompactionSummaryResult(backend string, raw []byte) (string, error) {
	var response Response
	var err error
	switch backend {
	case llmselection.BackendAnthropic:
		var parsed anthropicResponse
		err = json.Unmarshal(raw, &parsed)
		response = convertAnthropicResponse(parsed, nil)
	case llmselection.BackendOpenAICompatible:
		var parsed openAICompatibleResponse
		err = json.Unmarshal(raw, &parsed)
		if err == nil {
			response, err = convertOpenAICompatibleResponse(parsed, nil)
		}
	case llmselection.BackendOpenAIResponses:
		var parsed openAIResponsesResponse
		err = json.Unmarshal(raw, &parsed)
		if err == nil {
			response, err = convertOpenAIResponsesResponse(parsed, nil)
		}
	default:
		return "", contextCompactionLimitation("unsupported_summary_transport")
	}
	var terminal struct {
		StopReason string `json:"stop_reason"`
		Status     string `json:"status"`
		Choices    []struct {
			FinishReason string `json:"finish_reason"`
		}
	}
	if json.Unmarshal(raw, &terminal) != nil {
		return "", contextCompactionLimitation("invalid_summary_result")
	}
	if backend == llmselection.BackendAnthropic && terminal.StopReason != "" && terminal.StopReason != "end_turn" && terminal.StopReason != "stop_sequence" ||
		backend == llmselection.BackendOpenAIResponses && terminal.Status != "" && terminal.Status != "completed" ||
		backend == llmselection.BackendOpenAICompatible && len(terminal.Choices) != 0 && terminal.Choices[0].FinishReason != "" && terminal.Choices[0].FinishReason != "stop" {
		return "", contextCompactionLimitation("summary_output_not_complete")
	}
	if err != nil || len(response.ToolCalls) != 0 || strings.TrimSpace(response.Message.Content) == "" {
		return "", errors.Join(err, contextCompactionLimitation("summary_requires_text_without_tools"))
	}
	return response.Message.Content, nil
}
