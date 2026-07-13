package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type OpenAIResponsesRuntime struct {
	cfg                  *config.Config
	sessions             sessions.Registry
	conversations        ConversationPersistence
	lockOwner            string
	httpClient           *http.Client
	baseURL              string
	apiKey               string
	events               EventPublisher
	providerAdmission    *ProviderAdmissionRegistry
	credentials          ProviderCredentialResolver
	completionController *runtimeeffects.Controller
}

func NewOpenAIResponsesRuntime(cfg *config.Config, sessions sessions.Registry, lockOwner string, conversations ConversationPersistence, publisher EventPublisher) *OpenAIResponsesRuntime {
	return NewOpenAIResponsesRuntimeWithProviderCredentials(cfg, sessions, lockOwner, conversations, publisher, NewProviderCredentialResolver(nil))
}

func NewOpenAIResponsesRuntimeWithProviderCredentials(cfg *config.Config, sessions sessions.Registry, lockOwner string, conversations ConversationPersistence, publisher EventPublisher, credentials ProviderCredentialResolver) *OpenAIResponsesRuntime {
	if cfg == nil {
		cfg = &config.Config{}
	}
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAIResponses)
	baseURL, _ := llmselection.ResolveBaseURL(profile, cfg.LLM.OpenAIResponses.BaseURL)
	return &OpenAIResponsesRuntime{
		cfg:           cfg,
		sessions:      sessions,
		conversations: conversations,
		lockOwner:     lockOwner,
		httpClient: &http.Client{
			Timeout: 120 * time.Second,
		},
		baseURL:           baseURL,
		events:            publisher,
		providerAdmission: NewProviderAdmissionRegistry(cfg),
		credentials:       credentials,
	}
}

func (r *OpenAIResponsesRuntime) ProviderContract() ProviderContract {
	return OpenAIResponsesProviderContract()
}

func OpenAIResponsesProviderContract() ProviderContract {
	return ProviderContract{
		RuntimeMode: llmselection.ProviderContractRuntimeModeOpenAIResponses,
		Provider:    llmselection.ProviderOpenAI,
		Transport:   ProviderTransportAPI,
		ToolSchema: ProviderToolSchemaContract{
			ValidatesInputSchemas: true,
			TranslatesTools:       true,
			ReturnsToolResults:    true,
		},
		SessionLifecycle: ProviderSessionLifecycleContract{
			StartsSessions:            true,
			ContinuesSessions:         true,
			SupportsConversationModes: true,
			ProviderSessionIDStrategy: "platform_managed_provider_response_metadata",
			RotatesSessions:           true,
			PreservesRetryLineage:     true,
		},
		Response: ProviderResponseContract{
			NormalizesMessages:   true,
			NormalizesToolCalls:  true,
			PreservesRawResponse: true,
			StreamingParser:      "openai_responses_sse",
		},
		NativeTools: ProviderNativeToolContract{
			FallbackToolsAllowed: true,
		},
		Persistence: ProviderPersistenceContract{
			PersistsTurns:                 true,
			PersistsConversationSnapshots: true,
			PersistsTaskModeAudit:         true,
		},
		Budget: ProviderBudgetContract{
			UsageAccounting: BudgetUsageExact,
		},
	}
}

func (r *OpenAIResponsesRuntime) PersistConversationSnapshot(ctx context.Context, s *Session) error {
	if r.conversations == nil || s == nil {
		return nil
	}
	mode, err := sessions.ParseConversationRuntimeMode(s.ConversationMode)
	if err != nil {
		return err
	}
	if !shouldPersistConversationMode(mode) {
		return nil
	}
	return r.conversations.UpsertConversation(ctx, ConversationRecord{
		SessionID:    s.ID,
		AgentID:      s.AgentID,
		SessionScope: strings.TrimSpace(s.SessionScope),
		ScopeKey:     strings.TrimSpace(s.ScopeKey),
		Watchdog:     s.Watchdog,
		RunID:        strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx)),
		Mode:         mode.String(),
		Messages:     s.Messages,
		Summary:      BuildSessionSummary(s),
		TurnCount:    s.TurnCount,
		Status:       "active",
	})
}

func (r *OpenAIResponsesRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	scope := sessions.ScopeFromContext(ctx)
	actor, _ := runtimeactors.ActorFromContext(ctx)
	resolved, err := resolvedSessionScope(ctx, sessions.NormalizeConversationRuntimeMode(scope.ConversationMode), sessions.NormalizeSessionScope(scope.SessionScope), scope.ScopeKey)
	if err != nil {
		return nil, err
	}

	var lease *sessions.Lease
	if !resolved.Stateless {
		lease, err = r.sessions.Acquire(ctx, agentID, resolved.RuntimeMode, resolved.Scope, r.lockOwner, resolved.ScopeKey)
		if err != nil {
			return nil, err
		}
		if err := r.sessions.Release(ctx, lease); err != nil {
			return nil, err
		}
	}

	s := &Session{
		ID: ensurePlatformSessionID(func() string {
			if lease != nil {
				return lease.SessionID
			}
			return ""
		}()),
		AgentID:          agentID,
		RuntimeMode:      resolved.RuntimeMode.String(),
		ConversationMode: resolved.RuntimeMode.String(),
		SessionScope:     resolved.Scope.String(),
		ScopeKey:         resolved.ScopeKey,
		RetryReason: func() string {
			if lease != nil {
				return lease.RetryReason
			}
			return ""
		}(),
		RetriesFromSessionID: func() string {
			if lease != nil {
				return lease.RetriesFromSessionID
			}
			return ""
		}(),
		ProviderSessionID: func() string {
			if lease != nil {
				return lease.ProviderSessionID
			}
			return ""
		}(),
		SystemPrompt: augmentAgentSystemPrompt(systemPrompt, actor, tools),
		Tools:        tools,
		Messages:     nil,
	}
	if r.conversations != nil && !resolved.Stateless {
		if rec, ok, err := r.conversations.LoadActiveConversation(ctx, agentID, resolved.RuntimeMode.String(), resolved.Scope.String(), resolved.ScopeKey); err == nil && ok {
			s.Messages = rec.Messages
			s.TurnCount = rec.TurnCount
			s.RetryReason = strings.TrimSpace(rec.RetryReason)
			s.RetriesFromSessionID = strings.TrimSpace(rec.RetriesFromSessionID)
			s.Watchdog = rec.Watchdog
		}
	}
	publishAgentStarted(ctx, r.events, s, events.EventType("platform.agent_started"))
	return s, nil
}

func (r *OpenAIResponsesRuntime) ContinueSession(ctx context.Context, s *Session, message Message) (*Response, error) {
	if s == nil {
		return nil, errors.New("nil session")
	}
	actor, _ := runtimeactors.ActorFromContext(ctx)
	entityID := actor.EffectiveEntityID()

	resolved, err := resolvedSessionScope(ctx, sessions.NormalizeConversationRuntimeMode(coalesce(s.ConversationMode, s.RuntimeMode)), sessions.NormalizeSessionScope(coalesce(s.SessionScope, "")), s.ScopeKey)
	if err != nil {
		return nil, err
	}
	var lease *sessions.Lease
	if !resolved.Stateless {
		lease, err = r.sessions.Acquire(ctx, s.AgentID, resolved.RuntimeMode, resolved.Scope, r.lockOwner, resolved.ScopeKey)
		if err != nil {
			return nil, sessionAcquireFailure(err, s.AgentID)
		}
		defer func() { _ = r.sessions.Release(ctx, lease) }()
		stopLeaseHeartbeat := sessions.StartLeaseHeartbeatWithErrorHandler(ctx, r.sessions, lease, resolved.RuntimeMode, func(heartbeatErr error) {
			logPublisherRuntime(ctx, r.events, "warn", "session_lease_heartbeat_failed", "Refreshing the OpenAI Responses session lease heartbeat failed", s.AgentID, s.ID, entityID, map[string]any{
				"runtime_mode": resolved.RuntimeMode.String(),
				"scope_key":    resolved.ScopeKey,
			}, heartbeatErr)
		})
		defer stopLeaseHeartbeat()

		if lease.SessionID != s.ID {
			LogSessionAdoptedForRun(ctx, r.events, s.AgentID, resolved.RuntimeMode.String(), s.ID, lease.SessionID, resolved.ScopeKey)
			s.ID = lease.SessionID
		}
	}
	if err := requireInboundDeliveryActiveForSession(ctx, r.events, s, "error", "Marking the reused agent delivery in progress failed", map[string]any{
		"runtime_mode": resolved.RuntimeMode.String(),
		"scope_key":    resolved.ScopeKey,
	}, entityID); err != nil {
		return nil, fmt.Errorf("mark inbound delivery active for reused openai-responses session: %w", err)
	}

	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAIResponses)
	if strings.TrimSpace(r.apiKey) == "" {
		credential, err := r.credentials.Resolve(ctx, profile)
		if err != nil {
			return nil, err
		}
		r.apiKey = credential.Value
	}
	if strings.TrimSpace(r.baseURL) == "" {
		baseURL, err := llmselection.ResolveBaseURL(profile, r.cfg.LLM.OpenAIResponses.BaseURL)
		if err != nil {
			return nil, err
		}
		r.baseURL = baseURL
	}

	reqBody, err := r.buildRequest(ctx, s, message)
	if err != nil {
		return nil, err
	}
	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal openai-responses request: %w", err)
	}
	resolvedModel, err := resolveProviderAdmissionModel(ctx, r.cfg, r.providerAdmission, profile)
	if err != nil {
		return nil, err
	}
	ctx, completionTargetID, err := prepareCompletionContext(ctx, r.completionController, r.cfg, s, entityID)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	rawResp, parsed, dispatch, err := r.sendAdmittedRequest(ctx, profile, resolvedModel, reqJSON)
	latency := time.Since(start)
	if err != nil {
		turn := enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    resolved.RuntimeMode.String(),
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			Failure:        agentTurnFailure(err, "openai_responses_turn"),
		}, nil)
		if dispatch != nil {
			if settleErr := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, nil, profile, unavailableCompletionUsage(reqBody.Model), runtimeeffects.StateOutcomeUncertain, turn.Failure, map[string]any{"stage": "provider_call"}); settleErr != nil {
				return nil, errors.Join(err, settleErr)
			}
		}
		projectionErr := requireCurrentProviderProjection(ctx, s.AgentID)
		if projectionErr == nil {
			s.ParseFailures++
		}
		if projectionErr == nil && !resolved.Stateless {
			if rotated, rotateErr := MaybeRotateAfterParseFailures(ctx, s, resolved.RuntimeMode, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateOnParseFailures, r.events); rotateErr == nil && rotated != nil {
				lease = rotated
			}
		}
		if projectionErr != nil {
			return nil, projectionErr
		}
		return nil, err
	}

	usage, ok := openAIResponsesUsage(parsed)
	if !ok {
		err := errors.New("openai-responses response missing usage")
		turn := enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    resolved.RuntimeMode.String(),
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			Failure:        agentTurnFailure(err, "openai_responses_usage"),
		}, nil)
		if settleErr := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, nil, profile, unavailableCompletionUsage(reqBody.Model), runtimeeffects.StateOutcomeUncertain, turn.Failure, map[string]any{"stage": "usage_decode"}); settleErr != nil {
			return nil, errors.Join(err, settleErr)
		}
		if projectionErr := requireCurrentProviderProjection(ctx, s.AgentID); projectionErr != nil {
			return nil, projectionErr
		}
		s.ParseFailures++
		return nil, err
	}

	resp, err := convertOpenAIResponsesResponse(parsed)
	if err != nil {
		turn := enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    resolved.RuntimeMode.String(),
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			Failure:        agentTurnFailure(err, "openai_responses_decode"),
		}, nil)
		usage.Model = strings.TrimSpace(coalesce(usage.Model, reqBody.Model))
		if settleErr := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, nil, profile, completionUsage(usage.InputTokens, usage.OutputTokens, usage.Model, runtimeeffects.CompletionUsageExact), runtimeeffects.StateOutcomeUncertain, turn.Failure, map[string]any{"stage": "response_conversion"}); settleErr != nil {
			return nil, errors.Join(err, settleErr)
		}
		if projectionErr := requireCurrentProviderProjection(ctx, s.AgentID); projectionErr != nil {
			return nil, projectionErr
		}
		s.ParseFailures++
		return nil, err
	}
	resp.Raw = rawResp

	turn := enrichTurnRecord(ctx, s, AgentTurnRecord{
		AgentID:        s.AgentID,
		RuntimeMode:    resolved.RuntimeMode.String(),
		SessionID:      s.ID,
		RequestPayload: reqJSON,
		ResponseRaw:    rawResp,
		ParseOK:        true,
		Latency:        latency,
	}, &resp)
	usage.Model = strings.TrimSpace(coalesce(usage.Model, reqBody.Model))
	if err := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, &resp, profile, completionUsage(usage.InputTokens, usage.OutputTokens, usage.Model, runtimeeffects.CompletionUsageExact), runtimeeffects.StateSettled, nil, map[string]any{"stage": "complete"}); err != nil {
		return nil, err
	}

	if err := requireCurrentProviderProjection(ctx, s.AgentID); err != nil {
		return nil, err
	}
	s.Messages = append(s.Messages, message, resp.Message)
	s.TurnCount++
	s.ParseFailures = 0
	if !resolved.Stateless {
		if err := r.sessions.IncrementTurn(ctx, s.AgentID, resolved.RuntimeMode, resolved.Scope, s.ID, resolved.ScopeKey); err != nil {
			return nil, err
		}
	}
	r.persistConversation(ctx, s)

	if !resolved.Stateless {
		if rotated, rotateErr := MaybeRotateAfterTurn(ctx, s, resolved.RuntimeMode, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns, r.events); rotateErr == nil && rotated != nil {
			lease = rotated
		}
	}

	return &resp, nil
}

func (r *OpenAIResponsesRuntime) sendAdmittedRequest(ctx context.Context, profile llmselection.Profile, model llmselection.ResolvedModel, payload []byte) ([]byte, openAIResponsesResponse, *completionDispatch, error) {
	release, err := admitProviderRequest(ctx, r.providerAdmission, profile, model)
	if err != nil {
		return nil, openAIResponsesResponse{}, nil, err
	}
	defer release()
	return r.sendRequest(ctx, payload)
}

func (r *OpenAIResponsesRuntime) persistConversation(ctx context.Context, s *Session) {
	if r.conversations == nil || s == nil {
		return
	}
	mode, err := sessions.ParseConversationRuntimeMode(coalesce(s.ConversationMode, s.RuntimeMode))
	if err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_openai_responses_conversation_invalid_mode", "Persisting the OpenAI Responses conversation was skipped because the session mode was invalid", s.AgentID, s.ID, "", map[string]any{
			"mode":         strings.TrimSpace(s.ConversationMode),
			"runtime_mode": strings.TrimSpace(s.RuntimeMode),
			"scope_key":    strings.TrimSpace(s.ScopeKey),
		}, err)
		return
	}
	if !shouldPersistConversationMode(mode) {
		return
	}
	if err := r.conversations.UpsertConversation(ctx, ConversationRecord{
		SessionID:    s.ID,
		AgentID:      s.AgentID,
		SessionScope: strings.TrimSpace(s.SessionScope),
		ScopeKey:     strings.TrimSpace(s.ScopeKey),
		Watchdog:     s.Watchdog,
		Mode:         mode.String(),
		Messages:     s.Messages,
		Summary:      BuildSessionSummary(s),
		TurnCount:    s.TurnCount,
		Status:       "active",
	}); err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_openai_responses_conversation_failed", "Persisting the OpenAI Responses conversation failed", s.AgentID, s.ID, "", map[string]any{
			"mode":      mode.String(),
			"scope_key": strings.TrimSpace(s.ScopeKey),
		}, err)
	}
}

func (r *OpenAIResponsesRuntime) buildRequest(ctx context.Context, s *Session, input Message) (openAIResponsesRequest, error) {
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAIResponses)
	items := make([]any, 0, len(s.Messages)+2)
	for _, m := range s.Messages {
		items = append(items, toOpenAIResponsesInputItems(m)...)
	}
	items = append(items, toOpenAIResponsesInputItems(input)...)
	if len(items) == 0 {
		return openAIResponsesRequest{}, errors.New("at least one input item is required")
	}

	tools := make([]openAIResponsesTool, 0, len(s.Tools))
	for _, t := range s.Tools {
		schema := t.Schema
		if schema == nil {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		if err := ValidateProviderToolSchema(t.Name, schema); err != nil {
			return openAIResponsesRequest{}, err
		}
		tools = append(tools, openAIResponsesTool{
			Type:        "function",
			Name:        strings.TrimSpace(t.Name),
			Description: DeliveredToolDescription(t),
			Parameters:  schema,
		})
	}

	modelReq := llmselection.ModelResolution{
		Models: r.cfg.LLM.Models,
	}
	if actor, ok := runtimeactors.ActorFromContext(ctx); ok {
		modelReq.Model = actor.Model
	}
	resolvedModel, err := llmselection.ResolveModel(profile, modelReq)
	if err != nil {
		return openAIResponsesRequest{}, err
	}
	return openAIResponsesRequest{
		Model:        resolvedModel.ConcreteModel,
		Instructions: strings.TrimSpace(s.SystemPrompt),
		Input:        items,
		Tools:        tools,
	}, nil
}

func (r *OpenAIResponsesRuntime) sendRequest(ctx context.Context, payload []byte) ([]byte, openAIResponsesResponse, *completionDispatch, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, openAIResponsesURL(r.baseURL), bytes.NewReader(payload))
	if err != nil {
		return nil, openAIResponsesResponse{}, nil, fmt.Errorf("build openai-responses request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+r.apiKey)
	attempt, err := runtimeeffects.BeginCompletion(ctx, "openai_responses", payload, nil)
	if err != nil {
		return nil, openAIResponsesResponse{}, nil, err
	}
	dispatch := &completionDispatch{handle: attempt, state: runtimeeffects.StateOutcomeUncertain}
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		dispatch.state = runtimeeffects.StateTerminalFailure
		return nil, openAIResponsesResponse{}, dispatch, err
	}
	req = req.WithContext(heartbeatCtx)
	if err := attempt.MarkLaunched(heartbeatCtx); err != nil {
		dispatch.state = runtimeeffects.StateTerminalFailure
		return nil, openAIResponsesResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}

	httpResp, err := r.httpClient.Do(req)
	if err != nil {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "openai-responses-adapter", "send_request", map[string]any{"stage": "transport"}, err)
		return nil, openAIResponsesResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "openai-responses-adapter", "read_response", map[string]any{"stage": "read_response"}, err)
		return nil, openAIResponsesResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	dispatch.evidence = map[string]any{"status": httpResp.StatusCode, "response_fingerprint": runtimeeffects.Fingerprint(body)}
	if err := attempt.MarkResponseObserved(heartbeatCtx, dispatch.evidence); err != nil {
		return body, openAIResponsesResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}

	var parsed openAIResponsesResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "openai-responses-adapter", "decode_response", map[string]any{"stage": "decode_response"}, err)
		return body, openAIResponsesResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	if httpResp.StatusCode >= 300 {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_http_status_effect_outcome_unconfirmed", "openai-responses-adapter", "send_request", map[string]any{"status": httpResp.StatusCode}, providerStatusFailure("openai_responses", httpResp.StatusCode))
		return body, parsed, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	if parsed.Error.Message != "" {
		err = runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "provider_error_effect_outcome_unconfirmed", "openai-responses-adapter", "decode_response", nil)
		return body, parsed, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	dispatch.state = runtimeeffects.StateSettled
	return body, parsed, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, nil)
}

func openAIResponsesURL(baseURL string) string {
	normalized := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	return normalized + "/responses"
}

func toOpenAIResponsesInputItems(m Message) []any {
	role := strings.ToLower(strings.TrimSpace(m.Role))
	content := strings.TrimSpace(m.Content)
	switch role {
	case "assistant":
		items := make([]any, 0, 1+len(m.ToolCalls))
		if content != "" {
			items = append(items, openAIResponsesMessageInput{Role: "assistant", Content: content})
		}
		for _, call := range openAIResponsesFunctionCallInputs(m.ToolCalls) {
			items = append(items, call)
		}
		return items
	case "tool":
		if items := openAIResponsesToolResultItems(content); len(items) > 0 {
			return items
		}
		if content == "" {
			return nil
		}
		return []any{openAIResponsesMessageInput{Role: "user", Content: "Tool result:\n" + content}}
	case "system":
		if content == "" {
			return nil
		}
		return []any{openAIResponsesMessageInput{Role: "system", Content: content}}
	default:
		if content == "" {
			return nil
		}
		return []any{openAIResponsesMessageInput{Role: "user", Content: content}}
	}
}

func openAIResponsesFunctionCallInputs(calls []ToolCall) []openAIResponsesFunctionCallInput {
	if len(calls) == 0 {
		return nil
	}
	out := make([]openAIResponsesFunctionCallInput, 0, len(calls))
	for _, call := range calls {
		name := strings.TrimSpace(call.Name)
		if name == "" {
			continue
		}
		args := "{}"
		if call.Arguments != nil {
			if b, err := json.Marshal(call.Arguments); err == nil {
				args = string(b)
			}
		}
		out = append(out, openAIResponsesFunctionCallInput{
			Type:      "function_call",
			CallID:    strings.TrimSpace(call.ID),
			Name:      name,
			Arguments: args,
		})
	}
	return out
}

func openAIResponsesToolResultItems(content string) []any {
	if strings.TrimSpace(content) == "" {
		return nil
	}
	var entries []map[string]any
	if err := json.Unmarshal([]byte(content), &entries); err != nil {
		return nil
	}
	items := make([]any, 0, len(entries))
	for _, entry := range entries {
		id, _ := entry["tool_call_id"].(string)
		id = strings.TrimSpace(id)
		raw, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		if id == "" {
			items = append(items, openAIResponsesMessageInput{
				Role:    "user",
				Content: "Tool result:\n" + string(raw),
			})
			continue
		}
		items = append(items, openAIResponsesFunctionCallOutputInput{
			Type:   "function_call_output",
			CallID: id,
			Output: string(raw),
		})
	}
	return items
}

func convertOpenAIResponsesResponse(parsed openAIResponsesResponse) (Response, error) {
	if len(parsed.Output) == 0 {
		return Response{}, errors.New("openai-responses response missing output")
	}
	var content []string
	resp := Response{
		Message: Message{
			Role: "assistant",
		},
	}
	for _, item := range parsed.Output {
		switch strings.TrimSpace(item.Type) {
		case "message":
			for _, part := range item.Content {
				if strings.TrimSpace(part.Text) != "" {
					content = append(content, strings.TrimSpace(part.Text))
				}
			}
		case "function_call":
			name := strings.TrimSpace(item.Name)
			if name == "" {
				continue
			}
			id := strings.TrimSpace(item.CallID)
			if id == "" {
				id = strings.TrimSpace(item.ID)
			}
			resp.ToolCalls = append(resp.ToolCalls, ToolCall{
				ID:        id,
				Name:      name,
				Arguments: parseOpenAIResponsesToolArguments(item.Arguments),
			})
		}
	}
	resp.Message.Content = strings.Join(content, "\n")
	resp.Message.ToolCalls = append([]ToolCall(nil), resp.ToolCalls...)
	if resp.Message.Content == "" && len(resp.ToolCalls) == 0 {
		return Response{}, errors.New("openai-responses response missing message or function call output")
	}
	return resp, nil
}

func parseOpenAIResponsesToolArguments(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{}
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return raw
	}
	return decoded
}

func openAIResponsesUsage(parsed openAIResponsesResponse) (UsageTokens, bool) {
	if parsed.Usage.InputTokens == nil || parsed.Usage.OutputTokens == nil {
		return UsageTokens{}, false
	}
	return UsageTokens{
		InputTokens:  *parsed.Usage.InputTokens,
		OutputTokens: *parsed.Usage.OutputTokens,
		Model:        strings.TrimSpace(parsed.Model),
	}, true
}

func parseOpenAIResponsesSSE(raw []byte) (Response, error) {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var dataLines []string
	var text strings.Builder
	var calls []ToolCall
	pendingCalls := map[string]*openAIResponsesPendingFunctionCall{}
	var pendingCallOrder []string
	var completed *openAIResponsesResponse
	ensurePendingCall := func(key string) (*openAIResponsesPendingFunctionCall, error) {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, errors.New("openai-responses stream function call event missing item_id or call_id")
		}
		pending, ok := pendingCalls[key]
		if !ok {
			pending = &openAIResponsesPendingFunctionCall{ItemID: key}
			pendingCalls[key] = pending
			pendingCallOrder = append(pendingCallOrder, key)
		}
		return pending, nil
	}
	recordFunctionItem := func(item openAIResponsesOutputItem) error {
		if strings.TrimSpace(item.Type) != "function_call" {
			return nil
		}
		key := openAIResponsesFunctionCallStreamKey(item.ID, item.CallID)
		pending, err := ensurePendingCall(key)
		if err != nil {
			return err
		}
		pending.mergeItem(item)
		return nil
	}
	process := func() error {
		if len(dataLines) == 0 {
			return nil
		}
		payload := strings.TrimSpace(strings.Join(dataLines, "\n"))
		dataLines = nil
		if payload == "" || payload == "[DONE]" {
			return nil
		}
		var evt openAIResponsesStreamEvent
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			return fmt.Errorf("decode openai-responses stream event: %w", err)
		}
		switch evt.Type {
		case "response.output_text.delta":
			text.WriteString(evt.Delta)
		case "response.output_text.done":
			if text.Len() == 0 {
				text.WriteString(evt.Text)
			}
		case "response.output_item.added":
			if err := recordFunctionItem(evt.Item); err != nil {
				return err
			}
		case "response.function_call_arguments.delta":
			pending, err := ensurePendingCall(openAIResponsesFunctionCallStreamKey(evt.ItemID, evt.CallID))
			if err != nil {
				return err
			}
			pending.mergeEvent(evt)
			pending.Arguments += evt.Delta
		case "response.function_call_arguments.done":
			pending, err := ensurePendingCall(openAIResponsesFunctionCallStreamKey(evt.ItemID, evt.CallID))
			if err != nil {
				return err
			}
			pending.mergeEvent(evt)
			if strings.TrimSpace(evt.Arguments) != "" {
				pending.Arguments = evt.Arguments
			}
		case "response.output_item.done":
			if err := recordFunctionItem(evt.Item); err != nil {
				return err
			}
		case "response.completed":
			resp := evt.Response
			completed = &resp
		case "response.failed":
			return runtimefailures.New(runtimefailures.ClassConnectorFailure, "openai_responses_stream_failed", "openai-responses-adapter", "read_stream", nil)
		}
		return nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			if err := process(); err != nil {
				return Response{}, err
			}
			continue
		}
		if after, ok := strings.CutPrefix(line, "data:"); ok {
			dataLines = append(dataLines, strings.TrimSpace(after))
		}
	}
	if err := scanner.Err(); err != nil {
		return Response{}, runtimefailures.Wrap(runtimefailures.ClassConnectorFailure, "openai_responses_stream_read_failed", "openai-responses-adapter", "read_stream", nil, err)
	}
	if err := process(); err != nil {
		return Response{}, err
	}
	if completed != nil && len(completed.Output) > 0 {
		mergeOpenAIResponsesStreamCalls(completed, pendingCallOrder, pendingCalls)
		return convertOpenAIResponsesResponse(*completed)
	}
	calls = append(calls, openAIResponsesPendingToolCalls(pendingCallOrder, pendingCalls)...)
	resp := Response{
		Message: Message{
			Role:    "assistant",
			Content: strings.TrimSpace(text.String()),
		},
		ToolCalls: calls,
	}
	resp.Message.ToolCalls = append([]ToolCall(nil), resp.ToolCalls...)
	if resp.Message.Content == "" && len(resp.ToolCalls) == 0 {
		return Response{}, errors.New("openai-responses stream missing message or function call output")
	}
	return resp, nil
}

func openAIResponsesFunctionCallStreamKey(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

type openAIResponsesPendingFunctionCall struct {
	ItemID    string
	CallID    string
	Name      string
	Arguments string
}

func (p *openAIResponsesPendingFunctionCall) mergeEvent(evt openAIResponsesStreamEvent) {
	if p == nil {
		return
	}
	if strings.TrimSpace(evt.ItemID) != "" {
		p.ItemID = strings.TrimSpace(evt.ItemID)
	}
	if strings.TrimSpace(evt.CallID) != "" {
		p.CallID = strings.TrimSpace(evt.CallID)
	}
	if strings.TrimSpace(evt.Name) != "" {
		p.Name = strings.TrimSpace(evt.Name)
	}
}

func (p *openAIResponsesPendingFunctionCall) mergeItem(item openAIResponsesOutputItem) {
	if p == nil {
		return
	}
	if strings.TrimSpace(item.ID) != "" {
		p.ItemID = strings.TrimSpace(item.ID)
	}
	if strings.TrimSpace(item.CallID) != "" {
		p.CallID = strings.TrimSpace(item.CallID)
	}
	if strings.TrimSpace(item.Name) != "" {
		p.Name = strings.TrimSpace(item.Name)
	}
	if strings.TrimSpace(item.Arguments) != "" {
		p.Arguments = item.Arguments
	}
}

func mergeOpenAIResponsesStreamCalls(completed *openAIResponsesResponse, order []string, pending map[string]*openAIResponsesPendingFunctionCall) {
	if completed == nil || len(pending) == 0 {
		return
	}
	seen := map[string]struct{}{}
	for i, item := range completed.Output {
		if strings.TrimSpace(item.Type) != "function_call" {
			continue
		}
		key := openAIResponsesFunctionCallStreamKey(item.ID, item.CallID)
		if key != "" {
			seen[key] = struct{}{}
		}
		if call, ok := pending[key]; ok {
			if strings.TrimSpace(item.CallID) == "" {
				item.CallID = call.CallID
			}
			if strings.TrimSpace(item.Name) == "" {
				item.Name = call.Name
			}
			if strings.TrimSpace(item.Arguments) == "" {
				item.Arguments = call.Arguments
			}
			completed.Output[i] = item
		}
	}
	for _, key := range order {
		if _, ok := seen[key]; ok {
			continue
		}
		call := pending[key]
		if call == nil || strings.TrimSpace(call.Name) == "" {
			continue
		}
		completed.Output = append(completed.Output, openAIResponsesOutputItem{
			ID:        call.ItemID,
			Type:      "function_call",
			CallID:    call.CallID,
			Name:      call.Name,
			Arguments: call.Arguments,
		})
	}
}

func openAIResponsesPendingToolCalls(order []string, pending map[string]*openAIResponsesPendingFunctionCall) []ToolCall {
	if len(pending) == 0 {
		return nil
	}
	calls := make([]ToolCall, 0, len(pending))
	for _, key := range order {
		call := pending[key]
		if call == nil || strings.TrimSpace(call.Name) == "" {
			continue
		}
		id := strings.TrimSpace(call.CallID)
		if id == "" {
			id = strings.TrimSpace(call.ItemID)
		}
		calls = append(calls, ToolCall{
			ID:        id,
			Name:      strings.TrimSpace(call.Name),
			Arguments: parseOpenAIResponsesToolArguments(call.Arguments),
		})
	}
	return calls
}

type openAIResponsesRequest struct {
	Model        string                `json:"model"`
	Instructions string                `json:"instructions,omitempty"`
	Input        []any                 `json:"input"`
	Tools        []openAIResponsesTool `json:"tools,omitempty"`
}

type openAIResponsesTool struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters"`
}

type openAIResponsesMessageInput struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIResponsesFunctionCallInput struct {
	Type      string `json:"type"`
	CallID    string `json:"call_id,omitempty"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type openAIResponsesFunctionCallOutputInput struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	Output string `json:"output"`
}

type openAIResponsesResponse struct {
	ID     string                      `json:"id"`
	Model  string                      `json:"model"`
	Output []openAIResponsesOutputItem `json:"output"`
	Usage  struct {
		InputTokens  *int `json:"input_tokens"`
		OutputTokens *int `json:"output_tokens"`
		TotalTokens  *int `json:"total_tokens"`
	} `json:"usage"`
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    any    `json:"code"`
	} `json:"error"`
}

type openAIResponsesOutputItem struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Role      string `json:"role"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
	Content   []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

type openAIResponsesStreamEvent struct {
	Type      string                    `json:"type"`
	Delta     string                    `json:"delta"`
	Text      string                    `json:"text"`
	ItemID    string                    `json:"item_id"`
	CallID    string                    `json:"call_id"`
	Name      string                    `json:"name"`
	Arguments string                    `json:"arguments"`
	Item      openAIResponsesOutputItem `json:"item"`
	Response  openAIResponsesResponse   `json:"response"`
}
