package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
)

type HealthChecker func(ctx context.Context) (map[string]any, error)

type AgentReader interface {
	LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error)
}

type canonicalAgentReader interface {
	ListOperatorAgents(ctx context.Context, opts store.OperatorAgentListOptions) (store.OperatorAgentListResult, error)
	LoadOperatorAgent(ctx context.Context, agentID string) (store.OperatorAgentDetail, error)
}

type MailboxReader interface {
	ListMailboxItems(ctx context.Context, status string, limit int) ([]runtimetools.MailboxItem, error)
	GetMailboxItem(ctx context.Context, id string) (runtimetools.MailboxItem, error)
}

type EntityReader interface {
	ListOperatorEntities(ctx context.Context, opts store.OperatorEntityListOptions) (store.OperatorEntityListResult, error)
	LoadOperatorEntity(ctx context.Context, entityID, runID string) (store.OperatorEntityFull, error)
	AggregateOperatorEntities(ctx context.Context, opts store.OperatorEntityAggregateOptions) (store.OperatorEntityAggregateResult, error)
}

type ConversationSummary struct {
	SessionID   string                      `json:"session_id,omitempty"`
	AgentID     string                      `json:"agent_id"`
	Kind        string                      `json:"kind,omitempty"`
	ScopeKey    string                      `json:"scope_key,omitempty"`
	Scope       string                      `json:"scope,omitempty"`
	RuntimeMode string                      `json:"runtime_mode,omitempty"`
	Status      string                      `json:"status,omitempty"`
	TurnCount   int                         `json:"turn_count,omitempty"`
	Summary     string                      `json:"summary,omitempty"`
	UpdatedAt   string                      `json:"updated_at,omitempty"`
	Metadata    ConversationSummaryMetadata `json:"metadata,omitempty"`
}

type ConversationSummaryMetadata struct {
	ProviderSessionID    string                       `json:"provider_session_id,omitempty"`
	RetryReason          string                       `json:"retry_reason,omitempty"`
	RetriesFromSessionID string                       `json:"retries_from_session_id,omitempty"`
	Watchdog             *ConversationRuntimeWatchdog `json:"watchdog,omitempty"`
	LiveTurn             *OperatorLiveTurn            `json:"live_turn,omitempty"`
}

type ConversationDetail struct {
	AgentID      string                   `json:"agent_id"`
	SessionID    string                   `json:"session_id,omitempty"`
	Kind         string                   `json:"kind,omitempty"`
	ScopeKey     string                   `json:"scope_key,omitempty"`
	Scope        string                   `json:"scope,omitempty"`
	RuntimeMode  string                   `json:"runtime_mode,omitempty"`
	Status       string                   `json:"status,omitempty"`
	TurnCount    int                      `json:"turn_count,omitempty"`
	Summary      string                   `json:"summary,omitempty"`
	UpdatedAt    string                   `json:"updated_at,omitempty"`
	Messages     []ConversationMessage    `json:"messages"`
	Turns        []ConversationTurn       `json:"turns,omitempty"`
	RuntimeState ConversationRuntimeState `json:"runtime_state,omitempty"`
}

type ConversationTurn struct {
	TurnIndex              int                      `json:"turn_index,omitempty"`
	TurnID                 string                   `json:"turn_id"`
	AgentID                string                   `json:"agent_id,omitempty"`
	SessionID              string                   `json:"session_id,omitempty"`
	RuntimeMode            string                   `json:"runtime_mode,omitempty"`
	ScopeKey               string                   `json:"scope_key,omitempty"`
	EntityID               string                   `json:"entity_id,omitempty"`
	TriggerEventID         string                   `json:"trigger_event_id,omitempty"`
	TriggerEventType       string                   `json:"trigger_event_type,omitempty"`
	TaskID                 string                   `json:"task_id,omitempty"`
	AvailableTools         []string                 `json:"available_tools,omitempty"`
	ToolCalls              []ConversationToolCall   `json:"tool_calls,omitempty"`
	ToolResults            []ConversationToolResult `json:"tool_results,omitempty"`
	TurnBlocks             []ConversationTurnBlock  `json:"turn_blocks,omitempty"`
	EmittedEvents          []string                 `json:"emitted_events,omitempty"`
	MCPServers             map[string]string        `json:"mcp_servers,omitempty"`
	MCPToolsListed         []string                 `json:"mcp_tools_listed,omitempty"`
	MCPToolsVisible        []string                 `json:"mcp_tools_visible,omitempty"`
	RequestPayload         json.RawMessage          `json:"request_payload,omitempty"`
	ResponsePayload        json.RawMessage          `json:"response_payload,omitempty"`
	AssistantVisibleOutput string                   `json:"assistant_visible_output,omitempty"`
	ReasoningBlocks        []string                 `json:"reasoning_blocks,omitempty"`
	ProgressUpdates        []string                 `json:"progress_updates,omitempty"`
	Outcome                string                   `json:"outcome,omitempty"`
	ParseOK                bool                     `json:"parse_ok"`
	LatencyMS              int                      `json:"latency_ms,omitempty"`
	RetryCount             int                      `json:"retry_count,omitempty"`
	Error                  string                   `json:"error,omitempty"`
	CreatedAt              string                   `json:"created_at,omitempty"`
}

type ConversationRuntimeState struct {
	Summary              string                       `json:"summary,omitempty"`
	LastTurn             *ConversationRuntimeLastTurn `json:"last_turn,omitempty"`
	ProviderSessionID    string                       `json:"provider_session_id,omitempty"`
	RetryReason          string                       `json:"retry_reason,omitempty"`
	RetriesFromSessionID string                       `json:"retries_from_session_id,omitempty"`
	Watchdog             *ConversationRuntimeWatchdog `json:"watchdog,omitempty"`
}

type ConversationRuntimeLastTurn struct {
	TaskID  string `json:"task_id,omitempty"`
	ParseOK bool   `json:"parse_ok"`
}

type ConversationRuntimeWatchdog struct {
	State         string `json:"state,omitempty"`
	BlockingLayer string `json:"blocking_layer,omitempty"`
	Action        string `json:"action,omitempty"`
	Outcome       string `json:"outcome,omitempty"`
	LastOutputAt  string `json:"last_output_at,omitempty"`
	RecordedAt    string `json:"recorded_at,omitempty"`
}

type OperatorLiveTurn struct {
	TurnID                 string         `json:"turn_id,omitempty"`
	TaskID                 string         `json:"task_id,omitempty"`
	ParseOK                bool           `json:"parse_ok"`
	AssistantVisibleOutput string         `json:"assistant_visible_output,omitempty"`
	Outcome                string         `json:"outcome,omitempty"`
	ProgressUpdates        []string       `json:"progress_updates,omitempty"`
	LastTool               *AgentLastTool `json:"last_tool,omitempty"`
}

type ConversationMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ConversationToolCall struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

type ConversationToolResult struct {
	ToolName  string          `json:"tool_name,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Output    json.RawMessage `json:"output,omitempty"`
}

type ConversationTurnBlock struct {
	Kind     string          `json:"kind"`
	Title    string          `json:"title,omitempty"`
	Text     string          `json:"text,omitempty"`
	ToolName string          `json:"tool_name,omitempty"`
	Input    json.RawMessage `json:"input,omitempty"`
	Output   json.RawMessage `json:"output,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
}

type ConversationReader interface {
	List(ctx context.Context, limit int) ([]ConversationSummary, error)
	Get(ctx context.Context, sessionID string) (ConversationDetail, bool, error)
}

type canonicalConversationReader interface {
	ListOperatorConversations(ctx context.Context, opts store.OperatorConversationListOptions) (store.OperatorConversationListResult, error)
	LoadOperatorConversation(ctx context.Context, sessionID string) (store.OperatorConversationDetail, error)
}

type ObservabilityReader interface {
	ListEvents(ctx context.Context, filter EventFilter, limit int) ([]eventRecord, error)
	GetEvent(ctx context.Context, id string) (eventRecord, bool, error)
	ListRuntimeLogs(ctx context.Context, filter RuntimeLogFilter, limit int) ([]runtimeLogRecord, error)
	ListIncidents(ctx context.Context, filter IncidentFilter) ([]incidentRecord, error)
}

type RunTraceReader interface {
	LoadRunDebugTrace(ctx context.Context, runID string, opts store.RunDebugTraceQueryOptions) ([]store.RunDebugTraceRow, error)
}

type AgentController interface {
	runtimeagentcontrol.Controller
}

type RuntimeController interface {
	PauseIngress() error
	ResumeIngress() error
}

type Options struct {
	Health        HealthChecker
	Agents        AgentReader
	AgentControl  AgentController
	Mailbox       MailboxReader
	Entities      EntityReader
	Conversations ConversationReader
	Observability ObservabilityReader
	RunTrace      RunTraceReader
	Runtime       RuntimeController
	AuthToken     string
	Version       string
	Builder       http.Handler
}

type Handler struct {
	health        HealthChecker
	agents        AgentReader
	agentControl  AgentController
	mailbox       MailboxReader
	entities      EntityReader
	conversations ConversationReader
	observability ObservabilityReader
	runTrace      RunTraceReader
	runtime       RuntimeController
	authToken     string
	version       string
	builder       http.Handler
	mux           *http.ServeMux
}

func NewHandler(opts Options) http.Handler {
	h := &Handler{
		health:        opts.Health,
		agents:        opts.Agents,
		agentControl:  opts.AgentControl,
		mailbox:       opts.Mailbox,
		entities:      opts.Entities,
		conversations: opts.Conversations,
		observability: opts.Observability,
		runTrace:      opts.RunTrace,
		runtime:       opts.Runtime,
		authToken:     strings.TrimSpace(opts.AuthToken),
		version:       strings.TrimSpace(opts.Version),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.handleHealth)
	h.mux = mux
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.mux == nil {
		http.NotFound(w, r)
		return
	}
	if h.requiresAuthentication(r) {
		if err := h.authorize(r); err != nil {
			h.writeAuthError(w, err)
			return
		}
	}
	h.mux.ServeHTTP(w, r)
}

var (
	errDashboardAuthNotConfigured = errors.New("operator authentication is not configured")
	errDashboardAuthMissingBearer = errors.New("missing authorization bearer token")
	errDashboardAuthInvalidBearer = errors.New("invalid authorization header")
	errDashboardAuthInvalidToken  = errors.New("invalid token")
)

func (h *Handler) requiresAuthentication(r *http.Request) bool {
	return false
}

func (h *Handler) authorize(r *http.Request) error {
	if strings.TrimSpace(h.authToken) == "" {
		return errDashboardAuthNotConfigured
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if authz == "" {
		return errDashboardAuthMissingBearer
	}
	const prefix = "bearer "
	if !strings.HasPrefix(strings.ToLower(authz), prefix) {
		return errDashboardAuthInvalidBearer
	}
	if strings.TrimSpace(authz[len(prefix):]) != h.authToken {
		return errDashboardAuthInvalidToken
	}
	return nil
}

func (h *Handler) writeAuthError(w http.ResponseWriter, err error) {
	if errors.Is(err, errDashboardAuthNotConfigured) {
		writeJSONError(w, http.StatusServiceUnavailable, err)
		return
	}
	w.Header().Set("WWW-Authenticate", `Bearer realm="swarm-operator"`)
	writeJSONError(w, http.StatusUnauthorized, err)
}

type genericAgent struct {
	ID                  string            `json:"id"`
	Type                string            `json:"type,omitempty"`
	Role                string            `json:"role,omitempty"`
	Mode                string            `json:"mode,omitempty"`
	Status              string            `json:"status,omitempty"`
	State               string            `json:"state,omitempty"`
	BlockingLayer       string            `json:"blocking_layer,omitempty"`
	EntityID            string            `json:"entity_id,omitempty"`
	ParentAgentID       string            `json:"parent_agent_id,omitempty"`
	CoordinatorID       string            `json:"coordinator_id,omitempty"`
	HiredBy             string            `json:"hired_by,omitempty"`
	TemplateVersion     string            `json:"template_version,omitempty"`
	BudgetEnvelope      float64           `json:"budget_envelope,omitempty"`
	Subscriptions       []string          `json:"subscriptions,omitempty"`
	Permissions         []string          `json:"permissions,omitempty"`
	PendingEvents       int               `json:"pending_events,omitempty"`
	OldestPendingAgeSec int               `json:"oldest_pending_age_sec,omitempty"`
	LockOwner           string            `json:"lock_owner,omitempty"`
	LockExpiresAt       string            `json:"lock_expires_at,omitempty"`
	TurnCount           int               `json:"turn_count,omitempty"`
	TurnLimit           int               `json:"turn_limit,omitempty"`
	TotalTokens24h      int               `json:"total_tokens_24h,omitempty"`
	NearBreaker         bool              `json:"near_breaker,omitempty"`
	SessionID           string            `json:"session_id,omitempty"`
	ProviderSessionID   string            `json:"provider_session_id,omitempty"`
	CurrentTaskID       string            `json:"current_task_id,omitempty"`
	LastTool            *AgentLastTool    `json:"last_tool,omitempty"`
	LiveTurn            *OperatorLiveTurn `json:"live_turn,omitempty"`
	StartedAt           string            `json:"started_at,omitempty"`
}

type AgentLastTool struct {
	Name      string          `json:"name"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	OK        bool            `json:"ok"`
	Result    json.RawMessage `json:"result,omitempty"`
}

type instanceAggregateGroup struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}

type controlResult struct {
	OK      bool   `json:"ok,omitempty"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

type directiveRequest struct {
	Message string `json:"message"`
}

type runtimeActionRequest struct {
	Action string `json:"action"`
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"ok":        true,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	if h.health != nil {
		checks, err := h.health(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
		resp["checks"] = checks
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleAgents(w http.ResponseWriter, r *http.Request) {
	reader, ok := h.agents.(canonicalAgentReader)
	if h.agents == nil || !ok {
		writeJSONError(w, http.StatusNotImplemented, errors.New("agents reader is not configured"))
		return
	}
	result, err := reader.ListOperatorAgents(r.Context(), store.OperatorAgentListOptions{
		Flow: strings.TrimSpace(r.URL.Query().Get("flow")),
		Role: strings.TrimSpace(r.URL.Query().Get("role")),
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]genericAgent, 0, len(result.Agents))
	for _, row := range result.Agents {
		out = append(out, genericAgentFromOperatorSummary(row))
	}
	writeJSON(w, http.StatusOK, map[string]any{"agents": out})
}

func (h *Handler) handleAgentDetail(w http.ResponseWriter, r *http.Request) {
	reader, ok := h.agents.(canonicalAgentReader)
	if h.agents == nil || !ok {
		writeJSONError(w, http.StatusNotImplemented, errors.New("agents reader is not configured"))
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("agent id is required"))
		return
	}
	row, err := reader.LoadOperatorAgent(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrAgentNotFound) {
			writeJSONError(w, http.StatusNotFound, errors.New("agent not found"))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, genericAgentFromOperatorSummary(row.Agent))
}

func (h *Handler) handleAgentDirective(w http.ResponseWriter, r *http.Request) {
	if h.agentControl == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("agent control is not configured"))
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("agent id is required"))
		return
	}
	var req directiveRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	req.Message = strings.TrimSpace(req.Message)
	if req.Message == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("message is required"))
		return
	}
	resp, err := h.agentControl.SendDirective(r.Context(), runtimeagentcontrol.SendDirectiveRequest{
		AgentID:   id,
		Directive: req.Message,
		Source:    runtimeagentcontrol.DirectiveSourceDashboardLegacy,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, controlResult{OK: true, Message: strings.TrimSpace(resp.Response)})
}

func (h *Handler) handleAgentRestart(w http.ResponseWriter, r *http.Request) {
	if h.agentControl == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("agent control is not configured"))
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("agent id is required"))
		return
	}
	if _, err := h.agentControl.Restart(r.Context(), runtimeagentcontrol.RestartRequest{AgentID: id}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, controlResult{OK: true, Message: "agent restarted"})
}

func (h *Handler) handleAgentReplay(w http.ResponseWriter, r *http.Request) {
	if h.agentControl == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("agent control is not configured"))
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("agent id is required"))
		return
	}
	if _, err := h.agentControl.ReplayBacklog(r.Context(), runtimeagentcontrol.ReplayBacklogRequest{AgentID: id}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, controlResult{OK: true, Message: "agent backlog replayed"})
}

func (h *Handler) handleConversations(w http.ResponseWriter, r *http.Request) {
	reader, ok := h.conversations.(canonicalConversationReader)
	if h.conversations == nil || !ok {
		writeJSONError(w, http.StatusNotImplemented, errors.New("conversation reader is not configured"))
		return
	}
	result, err := reader.ListOperatorConversations(r.Context(), store.OperatorConversationListOptions{
		AgentID: strings.TrimSpace(r.URL.Query().Get("agent_id")),
		RunID:   strings.TrimSpace(r.URL.Query().Get("run_id")),
		Cursor:  strings.TrimSpace(r.URL.Query().Get("cursor")),
		Limit:   intQuery(r, "limit", 100),
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	rows := make([]ConversationSummary, 0, len(result.Conversations))
	for _, item := range result.Conversations {
		rows = append(rows, conversationSummaryFromOperator(item))
	}
	writeJSON(w, http.StatusOK, map[string]any{"conversations": rows})
}

func (h *Handler) handleConversationDetail(w http.ResponseWriter, r *http.Request) {
	reader, ok := h.conversations.(canonicalConversationReader)
	if h.conversations == nil || !ok {
		writeJSONError(w, http.StatusNotImplemented, errors.New("conversation reader is not configured"))
		return
	}
	sessionID := strings.TrimSpace(r.PathValue("sessionID"))
	if sessionID == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("session id is required"))
		return
	}
	row, err := reader.LoadOperatorConversation(r.Context(), sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeJSONError(w, http.StatusNotFound, errors.New("conversation not found"))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	if strings.TrimSpace(row.Conversation.SessionID) == "" {
		writeJSONError(w, http.StatusNotFound, errors.New("conversation not found"))
		return
	}
	writeJSON(w, http.StatusOK, conversationDetailFromOperator(row))
}

func (h *Handler) handleEvents(w http.ResponseWriter, r *http.Request) {
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("stream")), "true") {
		h.handleEventStream(w, r)
		return
	}
	rows := []eventRecord{}
	if h.observability != nil {
		filter := EventFilter{
			Type:       strings.TrimSpace(r.URL.Query().Get("type")),
			Source:     strings.TrimSpace(r.URL.Query().Get("source")),
			EntityID:   strings.TrimSpace(r.URL.Query().Get("entity_id")),
			Subscriber: strings.TrimSpace(r.URL.Query().Get("subscriber")),
		}
		var err error
		rows, err = h.observability.ListEvents(r.Context(), filter, intQuery(r, "limit", 200))
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": rows})
}

func (h *Handler) handleEventDetail(w http.ResponseWriter, r *http.Request) {
	if h.observability == nil {
		writeJSONError(w, http.StatusNotFound, errors.New("event not found"))
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("event id is required"))
		return
	}
	row, ok, err := h.observability.GetEvent(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeJSONError(w, http.StatusNotFound, errors.New("event not found"))
		return
	}
	writeJSON(w, http.StatusOK, row)
}

func (h *Handler) handleFlowEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, errors.New("streaming is not supported"))
		return
	}
	w.Header().Set("content-type", "text/event-stream")
	w.Header().Set("cache-control", "no-cache")
	w.Header().Set("connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	filter := EventFilter{
		EntityID: strings.TrimSpace(r.URL.Query().Get("entity_id")),
		After:    time.Now().UTC().Add(-2 * time.Second),
	}
	heartbeat := time.NewTicker(15 * time.Second)
	poll := time.NewTicker(2 * time.Second)
	defer heartbeat.Stop()
	defer poll.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case <-poll.C:
			if h.observability == nil {
				continue
			}
			rows, err := h.observability.ListEvents(r.Context(), filter, intQuery(r, "limit", 200))
			if err != nil || len(rows) == 0 {
				continue
			}
			latest := filter.After
			for i := len(rows) - 1; i >= 0; i-- {
				payload := map[string]any{
					"event_id":     rows[i].ID,
					"id":           rows[i].ID,
					"type":         rows[i].Type,
					"event_type":   rows[i].Type,
					"source_agent": rows[i].SourceAgent,
					"entity_id":    rows[i].EntityID,
					"scope":        rows[i].Scope,
					"created_at":   rows[i].CreatedAt,
					"payload":      rows[i].Payload,
				}
				encoded, _ := json.Marshal(payload)
				_, _ = fmt.Fprintf(w, "event: flow\ndata: %s\n\n", encoded)
				if ts, err := time.Parse(time.RFC3339, rows[i].CreatedAt); err == nil && ts.After(latest) {
					latest = ts
				}
			}
			filter.After = latest
			flusher.Flush()
		}
	}
}

func (h *Handler) handleRuntimeLogs(w http.ResponseWriter, r *http.Request) {
	rows := []runtimeLogRecord{}
	if h.observability != nil {
		filter := RuntimeLogFilter{
			Type:      strings.TrimSpace(r.URL.Query().Get("type")),
			Source:    strings.TrimSpace(r.URL.Query().Get("source")),
			EntityID:  strings.TrimSpace(r.URL.Query().Get("entity_id")),
			Component: strings.TrimSpace(r.URL.Query().Get("component")),
			Level:     strings.TrimSpace(r.URL.Query().Get("level")),
			ErrorCode: strings.TrimSpace(r.URL.Query().Get("error_code")),
			Order:     strings.TrimSpace(r.URL.Query().Get("order")),
		}
		var err error
		rows, err = h.observability.ListRuntimeLogs(r.Context(), filter, intQuery(r, "limit", 200))
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"runtime_logs": rows})
}

func (h *Handler) handleRuntimeIncidents(w http.ResponseWriter, r *http.Request) {
	rows := []incidentRecord{}
	if h.observability != nil {
		filter := IncidentFilter{
			SinceHours: intQuery(r, "since_hours", 24),
			MCPOnly:    strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("mcp_only")), "true"),
			Level:      strings.TrimSpace(r.URL.Query().Get("level")),
			Component:  strings.TrimSpace(r.URL.Query().Get("component")),
			Limit:      intQuery(r, "limit", 2000),
		}
		var err error
		rows, err = h.observability.ListIncidents(r.Context(), filter)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"incidents": rows})
}

func (h *Handler) handleRunTrace(w http.ResponseWriter, r *http.Request) {
	if h.runTrace == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("run trace reader is not configured"))
		return
	}
	runID := strings.TrimSpace(r.PathValue("runID"))
	if runID == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("run id is required"))
		return
	}
	rows, err := h.runTrace.LoadRunDebugTrace(r.Context(), runID, store.RunDebugTraceQueryOptions{
		Limit: intQuery(r, "limit", 200),
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"run_id": runID,
		"trace":  rows,
	})
}

func (h *Handler) handleEventStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, errors.New("streaming is not supported"))
		return
	}
	w.Header().Set("content-type", "text/event-stream")
	w.Header().Set("cache-control", "no-cache")
	w.Header().Set("connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	includeRuntime := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("include_runtime")), "true")
	eventFilter := EventFilter{
		Type:       strings.TrimSpace(r.URL.Query().Get("type")),
		Source:     strings.TrimSpace(r.URL.Query().Get("source")),
		EntityID:   strings.TrimSpace(r.URL.Query().Get("entity_id")),
		Subscriber: strings.TrimSpace(r.URL.Query().Get("subscriber")),
		After:      time.Now().UTC().Add(-2 * time.Second),
	}
	logFilter := RuntimeLogFilter{
		Type:      strings.TrimSpace(r.URL.Query().Get("type")),
		Source:    strings.TrimSpace(r.URL.Query().Get("source")),
		EntityID:  strings.TrimSpace(r.URL.Query().Get("entity_id")),
		Component: strings.TrimSpace(r.URL.Query().Get("component")),
		Level:     strings.TrimSpace(r.URL.Query().Get("level")),
		After:     time.Now().UTC().Add(-2 * time.Second),
	}

	heartbeat := time.NewTicker(15 * time.Second)
	poll := time.NewTicker(2 * time.Second)
	defer heartbeat.Stop()
	defer poll.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case <-poll.C:
			if h.observability != nil {
				if rows, err := h.observability.ListEvents(r.Context(), eventFilter, 50); err == nil && len(rows) > 0 {
					latest := eventFilter.After
					for i := len(rows) - 1; i >= 0; i-- {
						_, _ = fmt.Fprintf(w, "event: event\ndata: {\"id\":%q}\n\n", rows[i].ID)
						if ts, err := time.Parse(time.RFC3339, rows[i].CreatedAt); err == nil && ts.After(latest) {
							latest = ts
						}
					}
					eventFilter.After = latest
					flusher.Flush()
				}
				if includeRuntime {
					if rows, err := h.observability.ListRuntimeLogs(r.Context(), logFilter, 50); err == nil && len(rows) > 0 {
						latest := logFilter.After
						for i := len(rows) - 1; i >= 0; i-- {
							_, _ = fmt.Fprintf(w, "event: runtime_log\ndata: {\"id\":%q}\n\n", rows[i].ID)
							if ts, err := time.Parse(time.RFC3339, rows[i].TS); err == nil && ts.After(latest) {
								latest = ts
							}
						}
						logFilter.After = latest
						flusher.Flush()
					}
				}
			}
		}
	}
}

func (h *Handler) handleMailbox(w http.ResponseWriter, r *http.Request) {
	if h.mailbox == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("mailbox reader is not configured"))
		return
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	limit := intQuery(r, "limit", 150)
	items, err := h.mailbox.ListMailboxItems(r.Context(), status, limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) handleMailboxDetail(w http.ResponseWriter, r *http.Request) {
	if h.mailbox == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("mailbox reader is not configured"))
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("mailbox id is required"))
		return
	}
	item, err := h.mailbox.GetMailboxItem(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) handleInstances(w http.ResponseWriter, r *http.Request) {
	if h.entities == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("entity reader is not configured"))
		return
	}
	opts, err := dashboardEntityListOptions(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	result, err := h.entities.ListOperatorEntities(r.Context(), opts)
	if handleDashboardEntityReadError(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"instances":   result.Entities,
		"next_cursor": result.NextCursor,
	})
}

func (h *Handler) handleInstanceDetail(w http.ResponseWriter, r *http.Request) {
	if h.entities == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("entity reader is not configured"))
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("instance id is required"))
		return
	}
	row, err := h.entities.LoadOperatorEntity(r.Context(), runtimeflowidentity.EntityID(id), strings.TrimSpace(r.URL.Query().Get("run_id")))
	if handleDashboardEntityReadError(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, row)
}

func (h *Handler) handleInstanceAggregate(w http.ResponseWriter, r *http.Request) {
	if h.entities == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("entity reader is not configured"))
		return
	}
	groupBy := strings.TrimSpace(r.URL.Query().Get("group_by"))
	if groupBy == "" {
		groupBy = "current_state"
	}
	opts, err := dashboardEntityAggregateOptions(r, groupBy)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	result, err := h.entities.AggregateOperatorEntities(r.Context(), opts)
	if handleDashboardEntityReadError(w, err) {
		return
	}
	out := make([]instanceAggregateGroup, 0, len(result.Counts))
	for key, count := range result.Counts {
		out = append(out, instanceAggregateGroup{Key: key, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Key < out[j].Key
		}
		return out[i].Count > out[j].Count
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"group_by": groupBy,
		"groups":   out,
	})
}

func (h *Handler) handleRuntimeAction(w http.ResponseWriter, r *http.Request) {
	if h.runtime == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("runtime control is not configured"))
		return
	}
	var req runtimeActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	switch strings.TrimSpace(req.Action) {
	case "pause":
		if err := h.runtime.PauseIngress(); err != nil {
			writeJSONError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, controlResult{OK: true, Message: "runtime paused"})
	case "resume":
		if err := h.runtime.ResumeIngress(); err != nil {
			writeJSONError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, controlResult{OK: true, Message: "runtime resumed"})
	default:
		writeJSONError(w, http.StatusBadRequest, errors.New("unsupported runtime action"))
	}
}

func dashboardEntityListOptions(r *http.Request) (store.OperatorEntityListOptions, error) {
	opts := store.OperatorEntityListOptions{
		RunID:        strings.TrimSpace(r.URL.Query().Get("run_id")),
		Flow:         strings.TrimSpace(r.URL.Query().Get("flow")),
		Type:         strings.TrimSpace(r.URL.Query().Get("type")),
		CurrentState: strings.TrimSpace(r.URL.Query().Get("current_state")),
		Cursor:       strings.TrimSpace(r.URL.Query().Get("cursor")),
		Limit:        intQuery(r, "limit", 0),
	}
	if opts.Flow == "" {
		opts.Flow = strings.TrimSpace(r.URL.Query().Get("workflow_name"))
	}
	if entityID := strings.TrimSpace(r.URL.Query().Get("entity_id")); entityID != "" {
		opts.EntityID = runtimeflowidentity.EntityID(entityID)
	}
	return opts, nil
}

func dashboardEntityAggregateOptions(r *http.Request, groupBy string) (store.OperatorEntityAggregateOptions, error) {
	return store.OperatorEntityAggregateOptions{
		RunID:   strings.TrimSpace(r.URL.Query().Get("run_id")),
		GroupBy: dashboardEntityAggregateGroupBy(groupBy),
		Type:    strings.TrimSpace(r.URL.Query().Get("type")),
	}, nil
}

func dashboardEntityAggregateGroupBy(groupBy string) string {
	switch strings.TrimSpace(groupBy) {
	case "workflow_name":
		return "workflow_name"
	case "workflow_version":
		return "workflow_version"
	default:
		return strings.TrimSpace(groupBy)
	}
}

func handleDashboardEntityReadError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, store.ErrEntityNotFound):
		writeJSONError(w, http.StatusNotFound, errors.New("entity not found"))
	case errors.Is(err, store.ErrAmbiguousEntityRunID):
		writeJSONError(w, http.StatusBadRequest, errors.New("run_id is required when entity_id exists in multiple runs"))
	case errors.Is(err, store.ErrInvalidEntityCursor), errors.Is(err, store.ErrInvalidEntityReadParam):
		writeJSONError(w, http.StatusBadRequest, err)
	default:
		writeJSONError(w, http.StatusInternalServerError, err)
	}
	return true
}

func intQuery(r *http.Request, key string, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	if n < 0 {
		return fallback
	}
	return n
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}

func toGenericAgent(row runtimemanager.PersistedAgent) genericAgent {
	return genericAgent{
		ID:              strings.TrimSpace(row.Config.ID),
		Type:            strings.TrimSpace(row.Config.Type),
		Role:            strings.TrimSpace(row.Config.Role),
		Mode:            strings.TrimSpace(row.Config.Mode),
		Status:          strings.TrimSpace(row.Status),
		EntityID:        strings.TrimSpace(row.Config.EffectiveEntityID()),
		ParentAgentID:   strings.TrimSpace(row.ParentAgentID),
		CoordinatorID:   strings.TrimSpace(row.CoordinatorID),
		HiredBy:         strings.TrimSpace(row.HiredBy),
		TemplateVersion: strings.TrimSpace(row.TemplateVersion),
		BudgetEnvelope:  row.Config.BudgetEnvelope,
		Subscriptions:   append([]string(nil), row.Config.Subscriptions...),
		Permissions:     append([]string(nil), row.Config.Permissions...),
		StartedAt:       formatTime(row.StartedAt),
	}
}

func genericAgentFromOperatorSummary(row store.OperatorAgentSummary) genericAgent {
	return genericAgent{
		ID:                  strings.TrimSpace(row.AgentID),
		Type:                strings.TrimSpace(row.Type),
		Role:                strings.TrimSpace(row.Role),
		Mode:                strings.TrimSpace(row.Mode),
		Status:              firstString(strings.TrimSpace(row.DashboardStatus), strings.TrimSpace(row.Status)),
		State:               strings.TrimSpace(row.DashboardState),
		BlockingLayer:       strings.TrimSpace(row.BlockingLayer),
		EntityID:            strings.TrimSpace(row.EntityID),
		ParentAgentID:       strings.TrimSpace(row.ParentAgentID),
		CoordinatorID:       strings.TrimSpace(row.CoordinatorID),
		HiredBy:             strings.TrimSpace(row.HiredBy),
		TemplateVersion:     strings.TrimSpace(row.TemplateVersion),
		BudgetEnvelope:      row.BudgetEnvelope,
		Subscriptions:       append([]string(nil), row.Subscriptions...),
		Permissions:         append([]string(nil), row.Permissions...),
		PendingEvents:       row.PendingEvents,
		OldestPendingAgeSec: row.OldestPendingAgeSec,
		LockOwner:           strings.TrimSpace(row.LockOwner),
		LockExpiresAt:       formatTime(row.LockExpiresAt),
		TurnCount:           row.TurnCount,
		TurnLimit:           row.TurnLimit,
		NearBreaker:         row.NearBreaker,
		SessionID:           strings.TrimSpace(row.SessionID),
		ProviderSessionID:   strings.TrimSpace(row.ProviderSessionID),
		CurrentTaskID:       strings.TrimSpace(row.CurrentTaskID),
		LastTool:            dashboardAgentLastTool(row.LastTool),
		LiveTurn:            dashboardLiveTurn(row.LiveTurn),
		StartedAt:           formatTime(row.StartedAt),
	}
}

func dashboardAgentLastTool(item *store.OperatorAgentTool) *AgentLastTool {
	if item == nil {
		return nil
	}
	return &AgentLastTool{
		Name:      strings.TrimSpace(item.Name),
		ToolUseID: strings.TrimSpace(item.ToolUseID),
		OK:        item.OK,
		Result:    append(json.RawMessage(nil), item.Result...),
	}
}

func dashboardLiveTurn(item *store.OperatorLiveTurn) *OperatorLiveTurn {
	if item == nil {
		return nil
	}
	return &OperatorLiveTurn{
		TurnID:                 strings.TrimSpace(item.TurnID),
		TaskID:                 strings.TrimSpace(item.TaskID),
		ParseOK:                item.ParseOK,
		AssistantVisibleOutput: strings.TrimSpace(item.AssistantVisibleOutput),
		Outcome:                strings.TrimSpace(item.Outcome),
		ProgressUpdates:        append([]string(nil), item.ProgressUpdates...),
		LastTool:               dashboardAgentLastTool(item.LastTool),
	}
}

type genericAgentProvider interface {
	ListGenericAgents(ctx context.Context) ([]genericAgent, error)
	GetGenericAgent(ctx context.Context, id string) (genericAgent, bool, error)
}

func writeJSONError(w http.ResponseWriter, status int, err error) {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, map[string]any{"error": err.Error()})
}
