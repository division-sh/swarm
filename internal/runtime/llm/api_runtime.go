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
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

// AnthropicAPIRuntime provides production API-backed LLM execution.
type AnthropicAPIRuntime struct {
	cfg                  *config.Config
	sessions             sessions.Registry
	conversations        ConversationPersistence
	lockOwner            string
	httpClient           *http.Client
	apiURL               string
	apiKey               string
	events               EventPublisher
	providerAdmission    *ProviderAdmissionRegistry
	credentials          ProviderCredentialResolver
	completionController *runtimeeffects.Controller
}

func NewAnthropicAPIRuntime(cfg *config.Config, sessions sessions.Registry, lockOwner string, conversations ConversationPersistence, publisher EventPublisher) *AnthropicAPIRuntime {
	return NewAnthropicAPIRuntimeWithProviderCredentials(cfg, sessions, lockOwner, conversations, publisher, NewProviderCredentialResolver(nil))
}

func NewAnthropicAPIRuntimeWithProviderCredentials(cfg *config.Config, sessions sessions.Registry, lockOwner string, conversations ConversationPersistence, publisher EventPublisher, credentials ProviderCredentialResolver) *AnthropicAPIRuntime {
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	_ = profile
	return &AnthropicAPIRuntime{
		cfg:           cfg,
		sessions:      sessions,
		conversations: conversations,
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
			SupportsMemoryPlans:       true,
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
			PersistsStatelessAudit:        true,
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
	record, persist, err := memoryConversationRecord(s)
	if err != nil || !persist {
		return err
	}
	return r.conversations.UpsertConversation(ctx, record)
}

func (r *AnthropicAPIRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	actor, _ := runtimeactors.ActorFromContext(ctx)
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
		SystemPrompt: augmentAgentSystemPrompt(systemPrompt, actor, tools),
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

func (r *AnthropicAPIRuntime) ContinueSession(ctx context.Context, s *Session, message Message) (*Response, error) {
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
			logPublisherRuntime(ctx, r.events, "warn", "session_lease_heartbeat_failed", "Refreshing the API session lease heartbeat failed", s.AgentID, s.ID, entityID, map[string]any{
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
	ctx, completionTargetID, err := prepareCompletionContext(ctx, r.completionController, r.cfg, s, entityID)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	rawResp, parsed, dispatch, lastErr := r.sendAdmittedRequest(ctx, profile, resolvedModel, reqJSON)
	latency := time.Since(start)

	if lastErr != nil {
		turn := enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			RetryCount:     0,
			Failure:        agentTurnFailure(lastErr, "anthropic_turn"),
		}, nil)
		if dispatch != nil {
			if settleErr := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, nil, profile, unavailableCompletionUsage(reqBody.Model), runtimeeffects.StateOutcomeUncertain, turn.Failure, map[string]any{"stage": "provider_call"}); settleErr != nil {
				return nil, errors.Join(lastErr, settleErr)
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
		return nil, lastErr
	}

	resp := convertAnthropicResponse(parsed)
	resp.Raw = rawResp
	usage, ok := extractUsageTokensFromJSON(rawResp)
	if !ok {
		usageErr := fmt.Errorf("anthropic response missing usage")
		turn := enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID: s.AgentID, SessionID: s.ID,
			RequestPayload: reqJSON, ResponseRaw: rawResp, ParseOK: false, Latency: latency,
			Failure: agentTurnFailure(usageErr, "anthropic_usage"),
		}, nil)
		if settleErr := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, nil, profile, unavailableCompletionUsage(reqBody.Model), runtimeeffects.StateOutcomeUncertain, turn.Failure, map[string]any{"stage": "usage_decode"}); settleErr != nil {
			return nil, errors.Join(usageErr, settleErr)
		}
		if projectionErr := requireCurrentProviderProjection(ctx, s.AgentID); projectionErr != nil {
			return nil, projectionErr
		}
		s.ParseFailures++
		return nil, usageErr
	}
	usage.Model = strings.TrimSpace(coalesce(usage.Model, reqBody.Model))

	turn := enrichTurnRecord(ctx, s, AgentTurnRecord{
		AgentID:        s.AgentID,
		SessionID:      s.ID,
		RequestPayload: reqJSON,
		ResponseRaw:    rawResp,
		ParseOK:        true,
		Latency:        latency,
		RetryCount:     0,
	}, &resp)
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

func (r *AnthropicAPIRuntime) sendAdmittedRequest(ctx context.Context, profile llmselection.Profile, model llmselection.ResolvedModel, payload []byte) ([]byte, anthropicResponse, *completionDispatch, error) {
	release, err := admitProviderRequest(ctx, r.providerAdmission, profile, model)
	if err != nil {
		return nil, anthropicResponse{}, nil, err
	}
	defer release()
	return r.sendRequest(ctx, payload)
}

func (r *AnthropicAPIRuntime) persistConversation(ctx context.Context, s *Session) {
	if r.conversations == nil || s == nil {
		return
	}
	record, persist, err := memoryConversationRecord(s)
	if err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_api_conversation_invalid_memory", "Persisting the API conversation was skipped because the memory identity was invalid", s.AgentID, s.ID, "", nil, err)
		return
	}
	if !persist {
		return
	}
	if err := r.conversations.UpsertConversation(ctx, record); err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_api_conversation_failed", "Persisting the API conversation failed", s.AgentID, s.ID, "", map[string]any{
			"run_id":        record.Identity.RunID,
			"flow_instance": record.Identity.FlowInstance,
		}, err)
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

func (r *AnthropicAPIRuntime) sendRequest(ctx context.Context, payload []byte) ([]byte, anthropicResponse, *completionDispatch, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.apiURL, bytes.NewReader(payload))
	if err != nil {
		return nil, anthropicResponse{}, nil, fmt.Errorf("build anthropic request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", r.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	attempt, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", payload, nil)
	if err != nil {
		return nil, anthropicResponse{}, nil, err
	}
	dispatch := &completionDispatch{handle: attempt, state: runtimeeffects.StateOutcomeUncertain}
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		dispatch.state = runtimeeffects.StateTerminalFailure
		return nil, anthropicResponse{}, dispatch, err
	}
	req = req.WithContext(heartbeatCtx)
	if err := attempt.MarkLaunched(heartbeatCtx); err != nil {
		dispatch.state = runtimeeffects.StateTerminalFailure
		return nil, anthropicResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}

	httpResp, err := r.httpClient.Do(req)
	if err != nil {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "anthropic-adapter", "send_request", map[string]any{"stage": "transport"}, err)
		return nil, anthropicResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "anthropic-adapter", "read_response", map[string]any{"stage": "read_response"}, err)
		return nil, anthropicResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	dispatch.evidence = map[string]any{"status": httpResp.StatusCode, "response_fingerprint": runtimeeffects.Fingerprint(body)}
	if err := attempt.MarkResponseObserved(heartbeatCtx, dispatch.evidence); err != nil {
		return body, anthropicResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}

	var parsed anthropicResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_turn_outcome_unconfirmed", "anthropic-adapter", "decode_response", map[string]any{"stage": "decode_response"}, err)
		return body, anthropicResponse{}, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}

	if httpResp.StatusCode >= 300 {
		err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_http_status_effect_outcome_unconfirmed", "anthropic-adapter", "send_request", map[string]any{"status": httpResp.StatusCode}, providerStatusFailure("anthropic", httpResp.StatusCode))
		return body, parsed, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	if parsed.Error.Message != "" {
		err = runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "provider_error_effect_outcome_unconfirmed", "anthropic-adapter", "decode_response", nil)
		return body, parsed, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	dispatch.state = runtimeeffects.StateSettled
	return body, parsed, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, nil)
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
