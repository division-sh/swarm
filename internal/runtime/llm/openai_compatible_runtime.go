package llm

import (
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
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type OpenAICompatibleRuntime struct {
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

func NewOpenAICompatibleRuntime(cfg *config.Config, sessions sessions.Registry, lockOwner string, conversations ConversationPersistence, publisher EventPublisher) *OpenAICompatibleRuntime {
	return NewOpenAICompatibleRuntimeWithProviderCredentials(cfg, sessions, lockOwner, conversations, publisher, NewProviderCredentialResolver(nil))
}

func NewOpenAICompatibleRuntimeWithProviderCredentials(cfg *config.Config, sessions sessions.Registry, lockOwner string, conversations ConversationPersistence, publisher EventPublisher, credentials ProviderCredentialResolver) *OpenAICompatibleRuntime {
	if cfg == nil {
		cfg = &config.Config{}
	}
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAICompatible)
	baseURL, _ := llmselection.ResolveBaseURL(profile, cfg.LLM.OpenAICompatible.BaseURL)
	return &OpenAICompatibleRuntime{
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

func (r *OpenAICompatibleRuntime) ProviderContract() ProviderContract {
	return OpenAICompatibleProviderContract()
}

func OpenAICompatibleProviderContract() ProviderContract {
	return ProviderContract{
		RuntimeMode: llmselection.BackendOpenAICompatible,
		Provider:    llmselection.ProviderOpenAICompatible,
		Transport:   ProviderTransportAPI,
		ToolSchema: ProviderToolSchemaContract{
			ValidatesInputSchemas: true,
			TranslatesTools:       true,
			ReturnsToolResults:    true,
		},
		SessionLifecycle: ProviderSessionLifecycleContract{
			StartsSessions:            true,
			ContinuesSessions:         true,
			SupportsMemoryPlans:       true,
			ProviderSessionIDStrategy: "platform_managed",
			RotatesSessions:           true,
			PreservesRetryLineage:     true,
		},
		Response: ProviderResponseContract{
			NormalizesMessages:   true,
			NormalizesToolCalls:  true,
			PreservesRawResponse: true,
			StreamingParser:      "openai_compatible_chat_completions_json",
		},
		NativeTools: ProviderNativeToolContract{
			FallbackToolsAllowed: true,
		},
		Persistence: ProviderPersistenceContract{
			PersistsTurns:                 true,
			PersistsConversationSnapshots: true,
			PersistsStatelessAudit:        true,
		},
		Budget: ProviderBudgetContract{
			UsageAccounting: BudgetUsageExact,
		},
	}
}

func (r *OpenAICompatibleRuntime) PersistConversationSnapshot(ctx context.Context, s *Session) error {
	if r.conversations == nil || s == nil {
		return nil
	}
	record, persist, err := memoryConversationRecord(s)
	if err != nil || !persist {
		return err
	}
	return r.conversations.UpsertConversation(ctx, record)
}

func (r *OpenAICompatibleRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	lease, hydrated, resolved, err := startMemory(ctx, r.sessions, r.conversations, agentID, r.lockOwner)
	if err != nil {
		return nil, err
	}
	if resolved.Enabled() {
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
		AgentID:        agentID,
		Memory:         resolved.Plan,
		MemoryIdentity: resolved.Identity,
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
		SystemPrompt: strings.TrimSpace(systemPrompt),
		Tools:        tools,
		Messages:     append([]Message(nil), hydrated.Messages...),
		TurnCount:    hydrated.TurnCount,
		Watchdog:     hydrated.Watchdog,
	}
	if resolved.Enabled() {
		s.RetryReason = strings.TrimSpace(hydrated.RetryReason)
		s.RetriesFromSessionID = strings.TrimSpace(hydrated.RetriesFromSessionID)
	}
	publishAgentStarted(ctx, r.events, s, events.EventType("platform.agent_started"))
	return s, nil
}

func (r *OpenAICompatibleRuntime) ContinueSession(ctx context.Context, s *Session, message Message) (*Response, error) {
	if s == nil {
		return nil, errors.New("nil session")
	}
	actor, _ := runtimeactors.ActorFromContext(ctx)
	entityID := actor.EffectiveEntityID()

	lease, resolved, err := acquireContinuedMemory(ctx, r.sessions, s, r.lockOwner)
	if err != nil {
		return nil, sessionAcquireFailure(err, s.AgentID)
	}
	if resolved.Enabled() {
		defer func() { _ = r.sessions.Release(ctx, lease) }()
		stopLeaseHeartbeat := sessions.StartLeaseHeartbeatWithErrorHandler(ctx, r.sessions, lease, func(heartbeatErr error) {
			logPublisherRuntime(ctx, r.events, "warn", "session_lease_heartbeat_failed", "Refreshing the OpenAI-compatible session lease heartbeat failed", s.AgentID, s.ID, entityID, map[string]any{
				"run_id": resolved.Identity.RunID, "flow_instance": resolved.Identity.FlowInstance,
			}, heartbeatErr)
		})
		defer stopLeaseHeartbeat()

		if lease.SessionID != s.ID {
			LogSessionAdoptedForRun(ctx, r.events, resolved.Identity, s.ID, lease.SessionID)
			s.ID = lease.SessionID
		}
	}
	if err := requireInboundDeliveryActiveForSession(ctx, r.events, s, "error", "Marking the reused agent delivery in progress failed", map[string]any{
		"memory_enabled": resolved.Enabled(), "run_id": resolved.Identity.RunID, "flow_instance": resolved.Identity.FlowInstance,
	}, entityID); err != nil {
		return nil, fmt.Errorf("mark inbound delivery active for reused openai-compatible session: %w", err)
	}

	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAICompatible)
	if strings.TrimSpace(r.apiKey) == "" {
		credential, err := r.credentials.Resolve(ctx, profile)
		if err != nil {
			return nil, err
		}
		r.apiKey = credential.Value
	}
	if strings.TrimSpace(r.baseURL) == "" {
		baseURL, err := llmselection.ResolveBaseURL(profile, r.cfg.LLM.OpenAICompatible.BaseURL)
		if err != nil {
			return nil, err
		}
		r.baseURL = baseURL
	}

	reqBody, err := r.buildRequest(ctx, s, message)
	if err != nil {
		return nil, err
	}
	deliveredTools := make([]ToolDefinition, 0, len(reqBody.Tools))
	for _, tool := range reqBody.Tools {
		deliveredTools = append(deliveredTools, ToolDefinition{Name: tool.Function.Name, Description: tool.Function.Description, Schema: tool.Function.Parameters})
	}
	ctx, _, err = withObservedAPIRequestCapabilitySurface(ctx, deliveredTools)
	if err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "managed_capability_api_request_mismatch", "openai-compatible-adapter", "build_request", nil, err)
	}
	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal openai-compatible request: %w", err)
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
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			Failure:        agentTurnFailure(err, "openai_compatible_turn"),
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
		if projectionErr == nil && resolved.Enabled() {
			if rotated, rotateErr := MaybeRotateAfterParseFailures(ctx, s, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateOnParseFailures, r.events); rotateErr == nil && rotated != nil {
				lease = rotated
			}
		}
		if projectionErr != nil {
			return nil, projectionErr
		}
		return nil, err
	}

	usage, ok := openAICompatibleUsage(parsed)
	if !ok {
		err := errors.New("openai-compatible response missing usage")
		turn := enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			Failure:        agentTurnFailure(err, "openai_compatible_usage"),
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

	resp, err := convertOpenAICompatibleResponse(parsed)
	if err != nil {
		turn := enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			Failure:        agentTurnFailure(err, "openai_compatible_decode"),
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
	if surface, ok := managedcapabilities.FromContext(ctx); ok {
		resp.CapabilitySurface = &surface
	}

	turn := enrichTurnRecord(ctx, s, AgentTurnRecord{
		AgentID:        s.AgentID,
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
	if resolved.Enabled() {
		if err := r.sessions.IncrementTurn(ctx, resolved.Identity, s.ID); err != nil {
			return nil, err
		}
	}
	r.persistConversation(ctx, s)

	if resolved.Enabled() {
		if rotated, rotateErr := MaybeRotateAfterTurn(ctx, s, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns, r.events); rotateErr == nil && rotated != nil {
			lease = rotated
		}
	}

	return &resp, nil
}

func (r *OpenAICompatibleRuntime) sendAdmittedRequest(ctx context.Context, profile llmselection.Profile, model llmselection.ResolvedModel, payload []byte) ([]byte, openAICompatibleResponse, *completionDispatch, error) {
	release, err := admitProviderRequest(ctx, r.providerAdmission, profile, model)
	if err != nil {
		return nil, openAICompatibleResponse{}, nil, err
	}
	defer release()
	return r.sendRequest(ctx, payload)
}

func (r *OpenAICompatibleRuntime) persistConversation(ctx context.Context, s *Session) {
	if r.conversations == nil || s == nil {
		return
	}
	record, persist, err := memoryConversationRecord(s)
	if err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_openai_compatible_conversation_invalid_memory", "Persisting the OpenAI-compatible conversation was skipped because the memory identity was invalid", s.AgentID, s.ID, "", nil, err)
		return
	}
	if !persist {
		return
	}
	if err := r.conversations.UpsertConversation(ctx, record); err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_openai_compatible_conversation_failed", "Persisting the OpenAI-compatible conversation failed", s.AgentID, s.ID, "", map[string]any{
			"run_id":        record.Identity.RunID,
			"flow_instance": record.Identity.FlowInstance,
		}, err)
	}
}

func (r *OpenAICompatibleRuntime) buildRequest(ctx context.Context, s *Session, input Message) (openAICompatibleRequest, error) {
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAICompatible)
	msgs := make([]openAICompatibleMessage, 0, len(s.Messages)+2)
	if system := strings.TrimSpace(s.SystemPrompt); system != "" {
		msgs = append(msgs, openAICompatibleMessage{Role: "system", Content: system})
	}
	for _, m := range s.Messages {
		msgs = append(msgs, toOpenAICompatibleMessages(m)...)
	}
	msgs = append(msgs, toOpenAICompatibleMessages(input)...)
	if len(msgs) == 0 {
		return openAICompatibleRequest{}, errors.New("at least one message is required")
	}

	tools := make([]openAICompatibleTool, 0, len(s.Tools))
	for _, t := range s.Tools {
		schema := t.Schema
		if schema == nil {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		if err := ValidateProviderToolSchema(t.Name, schema); err != nil {
			return openAICompatibleRequest{}, err
		}
		tools = append(tools, openAICompatibleTool{
			Type: "function",
			Function: openAICompatibleFunctionTool{
				Name:        strings.TrimSpace(t.Name),
				Description: DeliveredToolDescription(t),
				Parameters:  schema,
			},
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
		return openAICompatibleRequest{}, err
	}
	return openAICompatibleRequest{
		Model:    resolvedModel.ConcreteModel,
		Messages: msgs,
		Tools:    tools,
	}, nil
}

func (r *OpenAICompatibleRuntime) sendRequest(ctx context.Context, payload []byte) ([]byte, openAICompatibleResponse, *completionDispatch, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, openAICompatibleChatCompletionsURL(r.baseURL), bytes.NewReader(payload))
	if err != nil {
		return nil, openAICompatibleResponse{}, nil, fmt.Errorf("build openai-compatible request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+r.apiKey)
	attempt, err := runtimeeffects.BeginCompletion(ctx, "openai_compatible", payload, nil)
	if err != nil {
		return nil, openAICompatibleResponse{}, nil, err
	}
	dispatch := &completionDispatch{handle: attempt, state: runtimeeffects.StateOutcomeUncertain}
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		dispatch.state = runtimeeffects.StateTerminalFailure
		return nil, openAICompatibleResponse{}, dispatch, err
	}
	req = req.WithContext(heartbeatCtx)
	if err := attempt.MarkLaunched(heartbeatCtx); err != nil {
		dispatch.state = runtimeeffects.StateTerminalFailure
		return nil, openAICompatibleResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}

	httpResp, err := r.httpClient.Do(req)
	if err != nil {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "openai-compatible-adapter", "send_request", map[string]any{"stage": "transport"}, err)
		return nil, openAICompatibleResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "openai-compatible-adapter", "read_response", map[string]any{"stage": "read_response"}, err)
		return nil, openAICompatibleResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	dispatch.evidence = map[string]any{"status": httpResp.StatusCode, "response_fingerprint": runtimeeffects.Fingerprint(body)}
	if err := attempt.MarkResponseObserved(heartbeatCtx, dispatch.evidence); err != nil {
		return body, openAICompatibleResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}

	var parsed openAICompatibleResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "openai-compatible-adapter", "decode_response", map[string]any{"stage": "decode_response"}, err)
		return body, openAICompatibleResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	if httpResp.StatusCode >= 300 {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_http_status_effect_outcome_unconfirmed", "openai-compatible-adapter", "send_request", map[string]any{"status": httpResp.StatusCode}, providerStatusFailure("openai_compatible", httpResp.StatusCode))
		return body, parsed, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	if parsed.Error.Message != "" {
		err = runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "provider_error_effect_outcome_unconfirmed", "openai-compatible-adapter", "decode_response", nil)
		return body, parsed, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	dispatch.state = runtimeeffects.StateSettled
	return body, parsed, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, nil)
}

func openAICompatibleChatCompletionsURL(baseURL string) string {
	normalized := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if strings.HasSuffix(normalized, "/v1") {
		return normalized + "/chat/completions"
	}
	return normalized + "/v1/chat/completions"
}

func toOpenAICompatibleMessages(m Message) []openAICompatibleMessage {
	role := strings.ToLower(strings.TrimSpace(m.Role))
	content := strings.TrimSpace(m.Content)
	switch role {
	case "assistant":
		msg := openAICompatibleMessage{Role: "assistant", Content: content}
		msg.ToolCalls = openAICompatibleToolCalls(m.ToolCalls)
		return []openAICompatibleMessage{msg}
	case "tool":
		if msgs := openAICompatibleToolResultMessages(content); len(msgs) > 0 {
			return msgs
		}
		if content == "" {
			return nil
		}
		return []openAICompatibleMessage{{Role: "user", Content: "Tool result:\n" + content}}
	case "system":
		if content == "" {
			return nil
		}
		return []openAICompatibleMessage{{Role: "system", Content: content}}
	default:
		if content == "" {
			return nil
		}
		return []openAICompatibleMessage{{Role: "user", Content: content}}
	}
}

func openAICompatibleToolResultMessages(content string) []openAICompatibleMessage {
	if strings.TrimSpace(content) == "" {
		return nil
	}
	var entries []map[string]any
	if err := json.Unmarshal([]byte(content), &entries); err != nil {
		return nil
	}
	msgs := make([]openAICompatibleMessage, 0, len(entries))
	for _, entry := range entries {
		id, _ := entry["tool_call_id"].(string)
		id = strings.TrimSpace(id)
		raw, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		if id == "" {
			msgs = append(msgs, openAICompatibleMessage{
				Role:    "user",
				Content: "Tool result:\n" + string(raw),
			})
			continue
		}
		msgs = append(msgs, openAICompatibleMessage{
			Role:       "tool",
			ToolCallID: id,
			Content:    string(raw),
		})
	}
	return msgs
}

func openAICompatibleToolCalls(calls []ToolCall) []openAICompatibleToolCall {
	if len(calls) == 0 {
		return nil
	}
	out := make([]openAICompatibleToolCall, 0, len(calls))
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
		out = append(out, openAICompatibleToolCall{
			ID:   strings.TrimSpace(call.ID),
			Type: "function",
			Function: openAICompatibleToolCallFunction{
				Name:      name,
				Arguments: args,
			},
		})
	}
	return out
}

func convertOpenAICompatibleResponse(parsed openAICompatibleResponse) (Response, error) {
	if len(parsed.Choices) == 0 {
		return Response{}, errors.New("openai-compatible response missing choices")
	}
	msg := parsed.Choices[0].Message
	resp := Response{
		Message: Message{
			Role:    "assistant",
			Content: strings.TrimSpace(msg.Content),
		},
	}
	for _, tc := range msg.ToolCalls {
		if strings.TrimSpace(tc.Function.Name) == "" {
			continue
		}
		resp.ToolCalls = append(resp.ToolCalls, ToolCall{
			ID:        strings.TrimSpace(tc.ID),
			Name:      strings.TrimSpace(tc.Function.Name),
			Arguments: parseOpenAICompatibleToolArguments(tc.Function.Arguments),
		})
	}
	resp.Message.ToolCalls = append([]ToolCall(nil), resp.ToolCalls...)
	return resp, nil
}

func parseOpenAICompatibleToolArguments(raw string) any {
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

func openAICompatibleUsage(parsed openAICompatibleResponse) (UsageTokens, bool) {
	if parsed.Usage.PromptTokens == nil || parsed.Usage.CompletionTokens == nil {
		return UsageTokens{}, false
	}
	return UsageTokens{
		InputTokens:  *parsed.Usage.PromptTokens,
		OutputTokens: *parsed.Usage.CompletionTokens,
		Model:        strings.TrimSpace(parsed.Model),
	}, true
}

type openAICompatibleRequest struct {
	Model    string                    `json:"model"`
	Messages []openAICompatibleMessage `json:"messages"`
	Tools    []openAICompatibleTool    `json:"tools,omitempty"`
}

type openAICompatibleMessage struct {
	Role       string                     `json:"role"`
	Content    string                     `json:"content,omitempty"`
	ToolCallID string                     `json:"tool_call_id,omitempty"`
	ToolCalls  []openAICompatibleToolCall `json:"tool_calls,omitempty"`
}

type openAICompatibleTool struct {
	Type     string                       `json:"type"`
	Function openAICompatibleFunctionTool `json:"function"`
}

type openAICompatibleFunctionTool struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters"`
}

type openAICompatibleToolCall struct {
	ID       string                           `json:"id,omitempty"`
	Type     string                           `json:"type"`
	Function openAICompatibleToolCallFunction `json:"function"`
}

type openAICompatibleToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type openAICompatibleResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Role      string `json:"role"`
			Content   string `json:"content"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     *int `json:"prompt_tokens"`
		CompletionTokens *int `json:"completion_tokens"`
		TotalTokens      *int `json:"total_tokens"`
	} `json:"usage"`
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    any    `json:"code"`
	} `json:"error"`
}
