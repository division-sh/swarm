package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimecorrelation "swarm/internal/runtime/correlation"
	llmselection "swarm/internal/runtime/llm/selection"
	"swarm/internal/runtime/sessions"
)

type OpenAICompatibleRuntime struct {
	cfg           *config.Config
	sessions      sessions.Registry
	turns         TurnPersistence
	conversations ConversationPersistence
	budget        BudgetGuard
	lockOwner     string
	httpClient    *http.Client
	baseURL       string
	apiKey        string
	events        EventPublisher
}

func NewOpenAICompatibleRuntime(cfg *config.Config, sessions sessions.Registry, lockOwner string, turns TurnPersistence, conversations ConversationPersistence, budget BudgetGuard, publisher EventPublisher) *OpenAICompatibleRuntime {
	if cfg == nil {
		cfg = &config.Config{}
	}
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAICompatible)
	baseURL, _ := llmselection.ResolveBaseURL(profile, cfg.LLM.OpenAICompatible.BaseURL)
	return &OpenAICompatibleRuntime{
		cfg:           cfg,
		sessions:      sessions,
		turns:         turns,
		conversations: conversations,
		budget:        budget,
		lockOwner:     lockOwner,
		httpClient: &http.Client{
			Timeout: 120 * time.Second,
		},
		baseURL: baseURL,
		apiKey:  llmselection.CredentialValue(profile, os.LookupEnv),
		events:  publisher,
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
			SupportsConversationModes: true,
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
			PersistsTaskModeAudit:         true,
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

func (r *OpenAICompatibleRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
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

func (r *OpenAICompatibleRuntime) ContinueSession(ctx context.Context, s *Session, message Message) (*Response, error) {
	if s == nil {
		return nil, errors.New("nil session")
	}
	actor, _ := runtimeactors.ActorFromContext(ctx)
	entityID := actor.EffectiveEntityID()
	scopeKey := budgetExecutionScopeKey(actor)
	if r.budget != nil {
		unlockScope := r.budget.LockExecutionScope(scopeKey)
		defer unlockScope()
		if r.budget.IsEntityEmergency(entityID) {
			return nil, fmt.Errorf("budget emergency: refusing llm execution (entity=%s)", entityID)
		}
	}

	resolved, err := resolvedSessionScope(ctx, sessions.NormalizeConversationRuntimeMode(coalesce(s.ConversationMode, s.RuntimeMode)), sessions.NormalizeSessionScope(coalesce(s.SessionScope, "")), s.ScopeKey)
	if err != nil {
		return nil, err
	}
	var lease *sessions.Lease
	if !resolved.Stateless {
		lease, err = r.sessions.Acquire(ctx, s.AgentID, resolved.RuntimeMode, resolved.Scope, r.lockOwner, resolved.ScopeKey)
		if err != nil {
			return nil, err
		}
		defer func() { _ = r.sessions.Release(ctx, lease) }()
		stopLeaseHeartbeat := sessions.StartLeaseHeartbeatWithErrorHandler(ctx, r.sessions, lease, resolved.RuntimeMode, func(heartbeatErr error) {
			logPublisherRuntime(ctx, r.events, "warn", "session_lease_heartbeat_failed", "Refreshing the OpenAI-compatible session lease heartbeat failed", s.AgentID, s.ID, entityID, map[string]any{
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
		return nil, fmt.Errorf("mark inbound delivery active for reused openai-compatible session: %w", err)
	}

	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAICompatible)
	if strings.TrimSpace(r.apiKey) == "" {
		r.apiKey = llmselection.CredentialValue(profile, os.LookupEnv)
		if strings.TrimSpace(r.apiKey) == "" {
			return nil, llmselection.RequireCredential(profile, os.LookupEnv)
		}
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
	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal openai-compatible request: %w", err)
	}

	start := time.Now()
	rawResp, parsed, err := r.sendRequest(ctx, reqJSON)
	latency := time.Since(start)
	if err != nil {
		s.ParseFailures++
		r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    resolved.RuntimeMode.String(),
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			Error:          err.Error(),
		}, nil))
		if !resolved.Stateless {
			if rotated, rotateErr := MaybeRotateAfterParseFailures(ctx, s, resolved.RuntimeMode, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateOnParseFailures, r.events); rotateErr == nil && rotated != nil {
				lease = rotated
			}
		}
		return nil, err
	}

	usage, ok := openAICompatibleUsage(parsed)
	if !ok {
		err := errors.New("openai-compatible response missing usage")
		s.ParseFailures++
		r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    resolved.RuntimeMode.String(),
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			Error:          err.Error(),
		}, nil))
		return nil, err
	}

	resp, err := convertOpenAICompatibleResponse(parsed)
	if err != nil {
		s.ParseFailures++
		r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    resolved.RuntimeMode.String(),
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			Error:          err.Error(),
		}, nil))
		return nil, err
	}
	resp.Raw = rawResp

	s.Messages = append(s.Messages, message, resp.Message)
	s.TurnCount++
	s.ParseFailures = 0
	if !resolved.Stateless {
		if err := r.sessions.IncrementTurn(ctx, s.AgentID, resolved.RuntimeMode, resolved.Scope, s.ID, resolved.ScopeKey); err != nil {
			return nil, err
		}
	}

	r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
		AgentID:        s.AgentID,
		RuntimeMode:    resolved.RuntimeMode.String(),
		SessionID:      s.ID,
		RequestPayload: reqJSON,
		ResponseRaw:    rawResp,
		ParseOK:        true,
		Latency:        latency,
	}, &resp))
	r.persistConversation(ctx, s)

	if r.budget != nil {
		usage.Model = strings.TrimSpace(coalesce(usage.Model, reqBody.Model))
		if err := r.budget.RecordEntityLLMUsage(ctx, entityID, s.AgentID, llmselection.BackendOpenAICompatible, usage, true, map[string]any{
			"session_id":       s.ID,
			"backend_profile":  llmselection.BackendOpenAICompatible,
			"provider":         llmselection.ProviderOpenAICompatible,
			"transport":        llmselection.TransportAPI,
			"usage_accounting": string(BudgetUsageExact),
		}); err != nil {
			logPublisherRuntime(ctx, r.events, "warn", "record_openai_compatible_llm_usage_failed", "Recording OpenAI-compatible LLM usage failed", s.AgentID, s.ID, entityID, nil, err)
		}
	}

	if !resolved.Stateless {
		if rotated, rotateErr := MaybeRotateAfterTurn(ctx, s, resolved.RuntimeMode, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns, r.events); rotateErr == nil && rotated != nil {
			lease = rotated
		}
	}

	return &resp, nil
}

func (r *OpenAICompatibleRuntime) persistConversation(ctx context.Context, s *Session) {
	if r.conversations == nil || s == nil {
		return
	}
	mode, err := sessions.ParseConversationRuntimeMode(coalesce(s.ConversationMode, s.RuntimeMode))
	if err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_openai_compatible_conversation_invalid_mode", "Persisting the OpenAI-compatible conversation was skipped because the session mode was invalid", s.AgentID, s.ID, "", map[string]any{
			"conversation_mode": strings.TrimSpace(s.ConversationMode),
			"runtime_mode":      strings.TrimSpace(s.RuntimeMode),
			"scope_key":         strings.TrimSpace(s.ScopeKey),
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
		logPublisherRuntime(ctx, r.events, "error", "persist_openai_compatible_conversation_failed", "Persisting the OpenAI-compatible conversation failed", s.AgentID, s.ID, "", map[string]any{
			"conversation_mode": mode.String(),
			"scope_key":         strings.TrimSpace(s.ScopeKey),
		}, err)
	}
}

func (r *OpenAICompatibleRuntime) persistTurn(ctx context.Context, turn AgentTurnRecord) {
	if r.turns == nil {
		return
	}
	if err := r.turns.AppendAgentTurn(ctx, turn); err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_openai_compatible_turn_failed", "Persisting the OpenAI-compatible agent turn failed", turn.AgentID, turn.SessionID, turn.EntityID, nil, err)
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
		Models: llmselection.ModelMap{
			Default: r.cfg.LLM.OpenAICompatible.DefaultModel,
			LowCost: r.cfg.LLM.OpenAICompatible.LowCostModel,
		},
	}
	if actor, ok := runtimeactors.ActorFromContext(ctx); ok {
		modelReq.ModelTier = actor.ModelTier
		actorEntityID := actor.EffectiveEntityID()
		if r.budget != nil && r.budget.IsEntityThrottle(actorEntityID) {
			modelReq.ForceLowCost = true
		}
	}
	model, err := llmselection.ResolveModelName(profile, modelReq)
	if err != nil {
		return openAICompatibleRequest{}, err
	}
	return openAICompatibleRequest{
		Model:    model,
		Messages: msgs,
		Tools:    tools,
	}, nil
}

func (r *OpenAICompatibleRuntime) sendRequest(ctx context.Context, payload []byte) ([]byte, openAICompatibleResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, openAICompatibleChatCompletionsURL(r.baseURL), bytes.NewReader(payload))
	if err != nil {
		return nil, openAICompatibleResponse{}, fmt.Errorf("build openai-compatible request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+r.apiKey)

	httpResp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, openAICompatibleResponse{}, fmt.Errorf("openai-compatible request failed: %w", err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, openAICompatibleResponse{}, fmt.Errorf("read openai-compatible response: %w", err)
	}

	var parsed openAICompatibleResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return body, openAICompatibleResponse{}, fmt.Errorf("decode openai-compatible response: %w", err)
	}
	if httpResp.StatusCode >= 300 {
		msg := strings.TrimSpace(parsed.Error.Message)
		if msg == "" {
			msg = strings.TrimSpace(string(body))
		}
		return body, parsed, openAICompatibleHTTPError{StatusCode: httpResp.StatusCode, Message: msg}
	}
	if parsed.Error.Message != "" {
		return body, parsed, fmt.Errorf("openai-compatible error: %s", parsed.Error.Message)
	}
	return body, parsed, nil
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

type openAICompatibleHTTPError struct {
	StatusCode int
	Message    string
}

func (e openAICompatibleHTTPError) Error() string {
	msg := strings.TrimSpace(e.Message)
	if msg == "" {
		msg = "request failed"
	}
	return fmt.Sprintf("openai-compatible status %d: %s", e.StatusCode, msg)
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
