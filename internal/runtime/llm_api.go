package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"empireai/internal/config"
	llm "empireai/internal/runtime/llm"
	"empireai/internal/runtime/sessions"
)

// AnthropicAPIRuntime provides production API-backed LLM execution.
type AnthropicAPIRuntime struct {
	cfg           *config.Config
	sessions      sessions.Registry
	turns         TurnPersistence
	conversations ConversationPersistence
	budget        *BudgetTracker
	lockOwner     string
	httpClient    *http.Client
	apiURL        string
	apiKey        string
}

func NewAnthropicAPIRuntime(cfg *config.Config, sessions sessions.Registry, lockOwner string, turns TurnPersistence, conversations ConversationPersistence, budget *BudgetTracker) *AnthropicAPIRuntime {
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
		apiURL: "https://api.anthropic.com/v1/messages",
		apiKey: os.Getenv("ANTHROPIC_API_KEY"),
	}
}

func (r *AnthropicAPIRuntime) PersistConversationSnapshot(ctx context.Context, s *llm.Session) error {
	if r.conversations == nil || s == nil {
		return nil
	}
	mode := strings.TrimSpace(s.ConversationMode)
	if mode == "" {
		mode = "session"
	}
	return r.conversations.UpsertConversation(ctx, ConversationRecord{
		AgentID:   s.AgentID,
		ScopeKey:  strings.TrimSpace(s.ScopeKey),
		Mode:      mode,
		Messages:  s.Messages,
		Summary:   buildSessionSummary(s),
		TurnCount: s.TurnCount,
		Status:    "active",
	})
}

func (r *AnthropicAPIRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	scope := sessions.ScopeFromContext(ctx)
	mode := strings.TrimSpace(scope.ConversationMode)
	if mode == "" {
		mode = "session"
	}
	scopeKey := strings.TrimSpace(scope.ScopeKey)

	lease, err := r.sessions.Acquire(agentID, "api", r.lockOwner, scopeKey)
	if err != nil {
		return nil, err
	}
	if err := r.sessions.Release(lease); err != nil {
		return nil, err
	}

	s := &llm.Session{
		ID:               lease.SessionID,
		AgentID:          agentID,
		RuntimeMode:      "api",
		ConversationMode: mode,
		ScopeKey:         scopeKey,
		SystemPrompt:     systemPrompt,
		Tools:            tools,
		Messages:         nil,
	}
	if r.conversations != nil {
		if rec, ok, err := r.conversations.LoadActiveConversation(context.Background(), agentID, mode, scopeKey); err == nil && ok {
			s.Messages = rec.Messages
			s.TurnCount = rec.TurnCount
		}
	}
	return s, nil
}

func (r *AnthropicAPIRuntime) ContinueSession(ctx context.Context, s *llm.Session, message llm.Message) (*llm.Response, error) {
	if s == nil {
		return nil, errors.New("nil session")
	}
	actor, _ := ActorFromContext(ctx)
	verticalID := strings.TrimSpace(actor.VerticalID)
	scopeKey := budgetExecutionScopeKey(actor)

	// Spec v2.0 budget cap enforcement: at 100% (budget.emergency) we hard-stop
	// LLM execution for the affected scope(s). This is treated as transient so
	// deliveries can be retried after budget resumes.
	if r.budget != nil {
		unlockScope := r.budget.LockExecutionScope(scopeKey)
		defer unlockScope()
		if r.budget.IsEmergency(verticalID) {
			return nil, fmt.Errorf("budget emergency: refusing llm execution (vertical=%s)", verticalID)
		}
	}

	lease, err := r.sessions.Acquire(s.AgentID, "api", r.lockOwner, strings.TrimSpace(s.ScopeKey))
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.sessions.Release(lease) }()
	stopLeaseHeartbeat := sessions.StartLeaseHeartbeat(ctx, r.sessions, lease, "api")
	defer stopLeaseHeartbeat()

	if lease.SessionID != s.ID {
		logSessionAdopted(s.AgentID, "api", s.ID, lease.SessionID, strings.TrimSpace(s.ScopeKey))
		s.ID = lease.SessionID
	}

	if strings.TrimSpace(r.apiKey) == "" {
		return nil, errors.New("ANTHROPIC_API_KEY is required for api runtime mode")
	}

	reqBody, err := r.buildRequest(ctx, s, message)
	if err != nil {
		return nil, err
	}
	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal anthropic request: %w", err)
	}

	maxRetries := r.cfg.LLM.ClaudeAPI.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}
	backoff := r.cfg.LLM.ClaudeAPI.RetryBackoff
	if backoff <= 0 {
		backoff = 2 * time.Second
	}

	start := time.Now()
	var lastErr error
	var rawResp []byte
	var parsed anthropicResponse
	var retryCount int
	for attempt := 0; attempt < maxRetries; attempt++ {
		retryCount = attempt
		rawResp, parsed, lastErr = r.sendRequest(ctx, reqJSON)
		if lastErr == nil {
			break
		}
		if !shouldRetryAnthropicError(lastErr) {
			break
		}
		if attempt == maxRetries-1 {
			break
		}
		sleep := backoff * time.Duration(1<<attempt)
		select {
		case <-ctx.Done():
			lastErr = ctx.Err()
			attempt = maxRetries - 1
		case <-time.After(sleep):
		}
	}
	latency := time.Since(start)

	if lastErr != nil {
		s.ParseFailures++
		r.persistTurn(ctx, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    "api",
			SessionID:      s.ID,
			RequestPayload: reqJSON,
			ResponseRaw:    rawResp,
			ParseOK:        false,
			Latency:        latency,
			RetryCount:     retryCount,
			Error:          lastErr.Error(),
		})
		if rotated, rotateErr := maybeRotateAfterParseFailures(s, "api", r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateOnParseFailures); rotateErr == nil && rotated != nil {
			lease = rotated
		}
		return nil, lastErr
	}

	resp := convertAnthropicResponse(parsed)
	resp.Raw = rawResp

	s.Messages = append(s.Messages, message, resp.Message)
	s.TurnCount++
	s.ParseFailures = 0
	if err := r.sessions.IncrementTurn(s.AgentID, "api", s.ID, strings.TrimSpace(s.ScopeKey)); err != nil {
		return nil, err
	}

	r.persistTurn(ctx, AgentTurnRecord{
		AgentID:        s.AgentID,
		RuntimeMode:    "api",
		SessionID:      s.ID,
		RequestPayload: reqJSON,
		ResponseRaw:    rawResp,
		ParseOK:        true,
		Latency:        latency,
		RetryCount:     retryCount,
	})
	r.persistConversation(ctx, s)

	// Spend ledger: exact usage for API runtime when usage fields are present.
	if r.budget != nil {
		usage := extractUsageTokensFromJSON(rawResp)
		usage.Model = strings.TrimSpace(coalesce(usage.Model, reqBody.Model))
		if err := r.budget.RecordLLMUsage(ctx, verticalID, s.AgentID, "api", usage, true, map[string]any{
			"session_id": s.ID,
		}); err != nil {
			log.Printf("failed to record api llm usage: agent=%s session=%s err=%v", s.AgentID, s.ID, err)
		}
	}

	if rotated, rotateErr := maybeRotateAfterTurn(s, "api", r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns); rotateErr == nil && rotated != nil {
		lease = rotated
	}

	return &resp, nil
}

func (r *AnthropicAPIRuntime) persistConversation(ctx context.Context, s *llm.Session) {
	if r.conversations == nil || s == nil {
		return
	}
	mode := strings.TrimSpace(s.ConversationMode)
	if mode == "" {
		mode = "session"
	}
	if err := r.conversations.UpsertConversation(ctx, ConversationRecord{
		AgentID:   s.AgentID,
		ScopeKey:  strings.TrimSpace(s.ScopeKey),
		Mode:      mode,
		Messages:  s.Messages,
		Summary:   buildSessionSummary(s),
		TurnCount: s.TurnCount,
		Status:    "active",
	}); err != nil {
		log.Printf("failed to persist api conversation: agent=%s err=%v", s.AgentID, err)
	}
}

func (r *AnthropicAPIRuntime) persistTurn(ctx context.Context, turn AgentTurnRecord) {
	if r.turns == nil {
		return
	}
	if err := r.turns.AppendAgentTurn(ctx, turn); err != nil {
		// Turn telemetry should not break runtime path.
		log.Printf("failed to persist agent turn: agent=%s session=%s err=%v", turn.AgentID, turn.SessionID, err)
	}
}

func (r *AnthropicAPIRuntime) buildRequest(ctx context.Context, s *llm.Session, input llm.Message) (anthropicRequest, error) {
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
			Description: t.Description,
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		}
		if t.Schema != nil {
			tool.InputSchema = t.Schema
		}
		tools = append(tools, tool)
	}

	model := strings.TrimSpace(r.cfg.LLM.ClaudeAPI.DefaultModel)
	if actor, ok := ActorFromContext(ctx); ok {
		switch strings.ToLower(strings.TrimSpace(actor.Type)) {
		case "haiku":
			if strings.TrimSpace(r.cfg.LLM.ClaudeAPI.HaikuModel) != "" {
				model = strings.TrimSpace(r.cfg.LLM.ClaudeAPI.HaikuModel)
			}
		}
		if r.budget != nil && r.budget.IsThrottle(strings.TrimSpace(actor.VerticalID)) {
			// Degradation on throttle: force cheaper model tier when configured.
			if strings.TrimSpace(r.cfg.LLM.ClaudeAPI.HaikuModel) != "" {
				model = strings.TrimSpace(r.cfg.LLM.ClaudeAPI.HaikuModel)
			}
		}
	}
	if model == "" {
		return anthropicRequest{}, errors.New("llm.claude_api.default_model is required in api mode")
	}
	return anthropicRequest{
		Model:     model,
		MaxTokens: 1024,
		System:    strings.TrimSpace(s.SystemPrompt),
		Messages:  msgs,
		Tools:     tools,
	}, nil
}

func toAnthropicMessage(m llm.Message) (anthropicMessage, bool) {
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

	httpResp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, anthropicResponse{}, fmt.Errorf("anthropic request failed: %w", err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, anthropicResponse{}, fmt.Errorf("read anthropic response: %w", err)
	}

	var parsed anthropicResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return body, anthropicResponse{}, fmt.Errorf("decode anthropic response: %w", err)
	}

	if httpResp.StatusCode >= 300 {
		msg := strings.TrimSpace(parsed.Error.Message)
		if msg == "" {
			msg = strings.TrimSpace(string(body))
		}
		return body, parsed, anthropicHTTPError{
			StatusCode: httpResp.StatusCode,
			Message:    msg,
		}
	}
	if parsed.Error.Message != "" {
		return body, parsed, fmt.Errorf("anthropic error: %s", parsed.Error.Message)
	}
	return body, parsed, nil
}

type anthropicHTTPError struct {
	StatusCode int
	Message    string
}

func (e anthropicHTTPError) Error() string {
	msg := strings.TrimSpace(e.Message)
	if msg == "" {
		msg = "request failed"
	}
	return fmt.Sprintf("anthropic status %d: %s", e.StatusCode, msg)
}

func shouldRetryAnthropicError(err error) bool {
	if err == nil {
		return false
	}
	var httpErr anthropicHTTPError
	if errors.As(err, &httpErr) {
		if httpErr.StatusCode == http.StatusTooManyRequests || httpErr.StatusCode >= 500 {
			return true
		}
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "context canceled") || strings.Contains(msg, "deadline exceeded") {
		return false
	}
	return strings.Contains(msg, "request failed") || strings.Contains(msg, "timeout") || strings.Contains(msg, "temporary")
}

func convertAnthropicResponse(parsed anthropicResponse) llm.Response {
	resp := llm.Response{
		Message: llm.Message{Role: "assistant"},
	}
	var textParts []string
	for _, block := range parsed.Content {
		switch block.Type {
		case "text":
			if strings.TrimSpace(block.Text) != "" {
				textParts = append(textParts, block.Text)
			}
		case "tool_use":
			resp.ToolCalls = append(resp.ToolCalls, llm.ToolCall{
				Name:      block.Name,
				Arguments: block.Input,
			})
		}
	}
	resp.Message.Content = strings.TrimSpace(strings.Join(textParts, "\n"))
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

func extractUsageTokensFromJSON(raw []byte) UsageTokens {
	if len(raw) == 0 {
		return UsageTokens{}
	}
	var obj struct {
		Model string `json:"model"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return UsageTokens{}
	}
	return UsageTokens{
		InputTokens:  obj.Usage.InputTokens,
		OutputTokens: obj.Usage.OutputTokens,
		Model:        obj.Model,
	}
}
