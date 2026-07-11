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
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

// AnthropicAPIRuntime provides production API-backed LLM execution.
type AnthropicAPIRuntime struct {
	cfg               *config.Config
	sessions          sessions.Registry
	turns             TurnPersistence
	conversations     ConversationPersistence
	budget            BudgetGuard
	lockOwner         string
	httpClient        *http.Client
	apiURL            string
	apiKey            string
	events            EventPublisher
	providerAdmission *ProviderAdmissionRegistry
	credentials       ProviderCredentialResolver
}

func NewAnthropicAPIRuntime(cfg *config.Config, sessions sessions.Registry, lockOwner string, turns TurnPersistence, conversations ConversationPersistence, budget BudgetGuard, publisher EventPublisher) *AnthropicAPIRuntime {
	return NewAnthropicAPIRuntimeWithProviderCredentials(cfg, sessions, lockOwner, turns, conversations, budget, publisher, NewProviderCredentialResolver(nil))
}

func NewAnthropicAPIRuntimeWithProviderCredentials(cfg *config.Config, sessions sessions.Registry, lockOwner string, turns TurnPersistence, conversations ConversationPersistence, budget BudgetGuard, publisher EventPublisher, credentials ProviderCredentialResolver) *AnthropicAPIRuntime {
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	_ = profile
	return &AnthropicAPIRuntime{
		cfg:           cfg,
		sessions:      sessions,
		turns:         turns,
		conversations: conversations,
		budget:        budget,
		lockOwner:     lockOwner,
		httpClient: &http.Client{
			Timeout: 120 * time.Second,
		},
		apiURL:            "https://api.anthropic.com/v1/messages",
		events:            publisher,
		providerAdmission: NewProviderAdmissionRegistry(cfg),
		credentials:       credentials,
	}
}

func (r *AnthropicAPIRuntime) ProviderContract() ProviderContract {
	return AnthropicAPIProviderContract()
}

func AnthropicAPIProviderContract() ProviderContract {
	return ProviderContract{
		RuntimeMode: llmselection.ProviderContractRuntimeModeAPI,
		Provider:    llmselection.ProviderAnthropic,
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
			StreamingParser:      "anthropic_messages_json",
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

func (r *AnthropicAPIRuntime) PersistConversationSnapshot(ctx context.Context, s *Session) error {
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

func (r *AnthropicAPIRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
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

func (r *AnthropicAPIRuntime) ContinueSession(ctx context.Context, s *Session, message Message) (*Response, error) {
	if s == nil {
		return nil, errors.New("nil session")
	}
	actor, _ := runtimeactors.ActorFromContext(ctx)
	entityID := actor.EffectiveEntityID()
	scopeKey := budgetExecutionScopeKey(actor)

	// Spec v2.0 budget cap enforcement: at 100% (budget.emergency) we hard-stop
	// LLM execution for the affected scope(s). This is treated as transient so
	// deliveries can be retried after budget resumes.
	if r.budget != nil {
		unlockScope := r.budget.LockExecutionScope(scopeKey)
		defer unlockScope()
		if r.budget.IsEntityEmergency(entityID) {
			return nil, budgetEmergencyFailure(entityID)
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
			return nil, sessionAcquireFailure(err, s.AgentID)
		}
		defer func() { _ = r.sessions.Release(ctx, lease) }()
		stopLeaseHeartbeat := sessions.StartLeaseHeartbeatWithErrorHandler(ctx, r.sessions, lease, resolved.RuntimeMode, func(heartbeatErr error) {
			logPublisherRuntime(ctx, r.events, "warn", "session_lease_heartbeat_failed", "Refreshing the API session lease heartbeat failed", s.AgentID, s.ID, entityID, map[string]any{
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
		return nil, fmt.Errorf("mark inbound delivery active for reused api session: %w", err)
	}

	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	if strings.TrimSpace(r.apiKey) == "" {
		credential, err := r.credentials.Resolve(ctx, profile)
		if err != nil {
			return nil, err
		}
		r.apiKey = credential.Value
	}

	reqBody, err := r.buildRequest(ctx, s, message)
	if err != nil {
		return nil, err
	}
	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal anthropic request: %w", err)
	}
	resolvedModel, err := resolveProviderAdmissionModel(ctx, r.cfg, r.providerAdmission, profile)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	rawResp, parsed, lastErr := r.sendAdmittedRequest(ctx, profile, resolvedModel, reqJSON)
	latency := time.Since(start)

	if lastErr != nil {
		r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    resolved.RuntimeMode.String(),
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			RetryCount:     0,
			Failure:        agentTurnFailure(lastErr, "anthropic_turn"),
		}, nil))
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
		return nil, lastErr
	}

	resp := convertAnthropicResponse(parsed)
	resp.Raw = rawResp
	var usage UsageTokens
	if r.budget != nil {
		var ok bool
		usage, ok = extractUsageTokensFromJSON(rawResp)
		if !ok {
			usageErr := fmt.Errorf("anthropic response missing usage")
			r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
				AgentID: s.AgentID, RuntimeMode: resolved.RuntimeMode.String(), SessionID: s.ID,
				RequestPayload: reqJSON, ResponseRaw: rawResp, ParseOK: false, Latency: latency,
				Failure: agentTurnFailure(usageErr, "anthropic_usage"),
			}, nil))
			if projectionErr := requireCurrentProviderProjection(ctx, s.AgentID); projectionErr != nil {
				return nil, projectionErr
			}
			s.ParseFailures++
			return nil, usageErr
		}
		usage.Model = strings.TrimSpace(coalesce(usage.Model, reqBody.Model))
	}

	r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
		AgentID:        s.AgentID,
		RuntimeMode:    resolved.RuntimeMode.String(),
		SessionID:      s.ID,
		RequestPayload: reqJSON,
		ResponseRaw:    rawResp,
		ParseOK:        true,
		Latency:        latency,
		RetryCount:     0,
	}, &resp))

	// Spend ledger: exact usage for API runtime when usage fields are present.
	if r.budget != nil {
		profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
		meta := usageMetadataForContext(ctx, profile, usage.Model)
		meta["session_id"] = s.ID
		meta["usage_accounting"] = string(BudgetUsageExact)
		if err := r.budget.RecordEntityLLMUsage(ctx, entityID, s.AgentID, profile.ID, usage, true, meta); err != nil {
			logPublisherRuntime(ctx, r.events, "warn", "record_api_llm_usage_failed", "Recording API LLM usage failed", s.AgentID, s.ID, entityID, nil, err)
		}
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

func (r *AnthropicAPIRuntime) sendAdmittedRequest(ctx context.Context, profile llmselection.Profile, model llmselection.ResolvedModel, payload []byte) ([]byte, anthropicResponse, error) {
	release, err := admitProviderRequest(ctx, r.providerAdmission, profile, model)
	if err != nil {
		return nil, anthropicResponse{}, err
	}
	defer release()
	return r.sendRequest(ctx, payload)
}

func (r *AnthropicAPIRuntime) persistConversation(ctx context.Context, s *Session) {
	if r.conversations == nil || s == nil {
		return
	}
	mode, err := sessions.ParseConversationRuntimeMode(coalesce(s.ConversationMode, s.RuntimeMode))
	if err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_api_conversation_invalid_mode", "Persisting the API conversation was skipped because the session mode was invalid", s.AgentID, s.ID, "", map[string]any{
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
		logPublisherRuntime(ctx, r.events, "error", "persist_api_conversation_failed", "Persisting the API conversation failed", s.AgentID, s.ID, "", map[string]any{
			"mode":      mode.String(),
			"scope_key": strings.TrimSpace(s.ScopeKey),
		}, err)
	}
}

func (r *AnthropicAPIRuntime) persistTurn(ctx context.Context, turn AgentTurnRecord) {
	if r.turns == nil {
		return
	}
	if err := r.turns.AppendAgentTurn(ctx, turn); err != nil {
		// Turn telemetry should not break runtime path.
		logPublisherRuntime(ctx, r.events, "error", "persist_api_turn_failed", "Persisting the API agent turn failed", turn.AgentID, turn.SessionID, turn.EntityID, nil, err)
	}
}

func (r *AnthropicAPIRuntime) buildRequest(ctx context.Context, s *Session, input Message) (anthropicRequest, error) {
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	msgs := make([]anthropicMessage, 0, len(s.Messages)+1)
	for _, m := range s.Messages {
		am, ok := toAnthropicMessage(m)
		if ok {
			msgs = append(msgs, am)
		}
	}
	if am, ok := toAnthropicMessage(input); ok {
		msgs = append(msgs, am)
	}

	if len(msgs) == 0 {
		return anthropicRequest{}, errors.New("at least one message is required")
	}

	tools := make([]anthropicTool, 0, len(s.Tools))
	for _, t := range s.Tools {
		tool := anthropicTool{
			Name:        t.Name,
			Description: DeliveredToolDescription(t),
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		}
		if t.Schema != nil {
			tool.InputSchema = t.Schema
		}
		tools = append(tools, tool)
	}

	modelReq := llmselection.ModelResolution{
		Models: r.cfg.LLM.Models,
	}
	if actor, ok := runtimeactors.ActorFromContext(ctx); ok {
		modelReq.Model = actor.Model
	}
	resolvedModel, err := llmselection.ResolveModel(profile, modelReq)
	if err != nil {
		return anthropicRequest{}, err
	}
	return anthropicRequest{
		Model:     resolvedModel.ConcreteModel,
		MaxTokens: 1024,
		System:    strings.TrimSpace(s.SystemPrompt),
		Messages:  msgs,
		Tools:     tools,
	}, nil
}

func toAnthropicMessage(m Message) (anthropicMessage, bool) {
	content := strings.TrimSpace(m.Content)
	if content == "" {
		return anthropicMessage{}, false
	}
	role := strings.ToLower(strings.TrimSpace(m.Role))
	switch role {
	case "assistant":
		return anthropicMessage{Role: "assistant", Content: content}, true
	case "tool":
		// Tool results are normalized as user observations for stateless API calls.
		return anthropicMessage{Role: "user", Content: "Tool result:\n" + content}, true
	default:
		return anthropicMessage{Role: "user", Content: content}, true
	}
}

func (r *AnthropicAPIRuntime) sendRequest(ctx context.Context, payload []byte) ([]byte, anthropicResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.apiURL, bytes.NewReader(payload))
	if err != nil {
		return nil, anthropicResponse{}, fmt.Errorf("build anthropic request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", r.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	attempt, err := runtimeeffects.Begin(ctx, "anthropic_api", payload, nil)
	if err != nil {
		return nil, anthropicResponse{}, err
	}
	if err := attempt.MarkLaunched(ctx); err != nil {
		return nil, anthropicResponse{}, err
	}

	httpResp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, anthropicResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "anthropic-adapter", "send_request", map[string]any{"stage": "transport"}, err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, anthropicResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "anthropic-adapter", "read_response", map[string]any{"stage": "read_response"}, err)
	}

	var parsed anthropicResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return body, anthropicResponse{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "anthropic-adapter", "decode_response", map[string]any{"stage": "decode_response"}, err)
	}

	if httpResp.StatusCode >= 300 {
		return body, parsed, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "provider_http_status_effect_outcome_unconfirmed", "anthropic-adapter", "send_request", map[string]any{"status": httpResp.StatusCode}, providerStatusFailure("anthropic", httpResp.StatusCode))
	}
	if parsed.Error.Message != "" {
		return body, parsed, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "provider_error_effect_outcome_unconfirmed", "anthropic-adapter", "decode_response", nil, nil)
	}
	if err := attempt.Succeed(ctx, map[string]any{"status": httpResp.StatusCode, "response_fingerprint": runtimeeffects.Fingerprint(body)}); err != nil {
		return body, parsed, err
	}
	return body, parsed, nil
}

func convertAnthropicResponse(parsed anthropicResponse) Response {
	resp := Response{
		Message: Message{Role: "assistant"},
	}
	var textParts []string
	for _, block := range parsed.Content {
		switch block.Type {
		case "text":
			if strings.TrimSpace(block.Text) != "" {
				textParts = append(textParts, block.Text)
			}
		case "tool_use":
			resp.ToolCalls = append(resp.ToolCalls, ToolCall{
				Name:      block.Name,
				Arguments: block.Input,
			})
		}
	}
	resp.Message.Content = strings.TrimSpace(strings.Join(textParts, "\n"))
	resp.Message.ToolCalls = append([]ToolCall(nil), resp.ToolCalls...)
	return resp
}

type anthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int                `json:"max_tokens"`
	System    string             `json:"system,omitempty"`
	Messages  []anthropicMessage `json:"messages"`
	Tools     []anthropicTool    `json:"tools,omitempty"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type anthropicTool struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	InputSchema any    `json:"input_schema"`
}

type anthropicResponse struct {
	Model string `json:"model"`
	Usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
	Content []struct {
		Type  string `json:"type"`
		Text  string `json:"text,omitempty"`
		Name  string `json:"name,omitempty"`
		Input any    `json:"input,omitempty"`
	} `json:"content"`
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func extractUsageTokensFromJSON(raw []byte) (UsageTokens, bool) {
	if len(raw) == 0 {
		return UsageTokens{}, false
	}
	var obj struct {
		Model string `json:"model"`
		Usage struct {
			InputTokens  *int `json:"input_tokens"`
			OutputTokens *int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return UsageTokens{}, false
	}
	if obj.Usage.InputTokens == nil || obj.Usage.OutputTokens == nil {
		return UsageTokens{}, false
	}
	return UsageTokens{
		InputTokens:  *obj.Usage.InputTokens,
		OutputTokens: *obj.Usage.OutputTokens,
		Model:        obj.Model,
	}, true
}
