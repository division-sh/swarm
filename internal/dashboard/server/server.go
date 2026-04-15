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

	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimetools "swarm/internal/runtime/tools"
)

type HealthChecker func(ctx context.Context) (map[string]any, error)

type AgentReader interface {
	LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error)
}

type MailboxReader interface {
	ListMailboxItems(ctx context.Context, status string, limit int) ([]runtimetools.MailboxItem, error)
	GetMailboxItem(ctx context.Context, id string) (runtimetools.MailboxItem, error)
}

type InstanceReader interface {
	List(ctx context.Context) ([]runtimepipeline.WorkflowInstance, error)
	Load(ctx context.Context, instanceID string) (runtimepipeline.WorkflowInstance, bool, error)
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

type ObservabilityReader interface {
	ListEvents(ctx context.Context, filter EventFilter, limit int) ([]eventRecord, error)
	GetEvent(ctx context.Context, id string) (eventRecord, bool, error)
	ListRuntimeLogs(ctx context.Context, filter RuntimeLogFilter, limit int) ([]runtimeLogRecord, error)
	ListIncidents(ctx context.Context, filter IncidentFilter) ([]incidentRecord, error)
}

type AgentController interface {
	RestartAgent(agentID string) error
	ReplayAgentBacklog(ctx context.Context, agentID string) error
	ChatWithAgent(ctx context.Context, agentID, directive string, killPrevious bool) (string, error)
}

type RuntimeController interface {
	PauseIngress()
	ResumeIngress()
	ResetState() error
}

type Options struct {
	Health        HealthChecker
	Agents        AgentReader
	AgentControl  AgentController
	Mailbox       MailboxReader
	Instances     InstanceReader
	Conversations ConversationReader
	Observability ObservabilityReader
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
	instances     InstanceReader
	conversations ConversationReader
	observability ObservabilityReader
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
		instances:     opts.Instances,
		conversations: opts.Conversations,
		observability: opts.Observability,
		runtime:       opts.Runtime,
		authToken:     strings.TrimSpace(opts.AuthToken),
		version:       strings.TrimSpace(opts.Version),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", h.handleHealth)
	mux.HandleFunc("GET /healthz", h.handleHealth)
	mux.HandleFunc("GET /api/healthz", h.handleHealth)
	mux.HandleFunc("GET /api/agents", h.handleAgents)
	mux.HandleFunc("GET /api/agents/{id}", h.handleAgentDetail)
	mux.HandleFunc("POST /api/agents/{id}/actions/directive", h.handleAgentDirective)
	mux.HandleFunc("POST /api/agents/{id}/actions/restart", h.handleAgentRestart)
	mux.HandleFunc("POST /api/agents/{id}/actions/replay", h.handleAgentReplay)
	mux.HandleFunc("GET /api/conversations", h.handleConversations)
	mux.HandleFunc("GET /api/conversations/{sessionID}", h.handleConversationDetail)
	mux.HandleFunc("GET /api/events", h.handleEvents)
	mux.HandleFunc("GET /api/events/flow", h.handleFlowEvents)
	mux.HandleFunc("GET /api/events/{id}", h.handleEventDetail)
	mux.HandleFunc("GET /api/mailbox", h.handleMailbox)
	mux.HandleFunc("GET /api/mailbox/{id}", h.handleMailboxDetail)
	mux.HandleFunc("GET /api/instances/aggregate", h.handleInstanceAggregate)
	mux.HandleFunc("GET /api/instances/{id}", h.handleInstanceDetail)
	mux.HandleFunc("GET /api/instances", h.handleInstances)
	mux.HandleFunc("GET /api/subjects/{id}/status", h.handleSubjectStatus)
	mux.HandleFunc("GET /api/runtime/logs", h.handleRuntimeLogs)
	mux.HandleFunc("GET /api/runtime/incidents", h.handleRuntimeIncidents)
	mux.HandleFunc("POST /api/runtime/actions", h.handleRuntimeAction)
	builderHandler := opts.Builder
	if builderHandler != nil {
		h.builder = builderHandler
		mux.Handle("POST /rpc", builderHandler)
		mux.Handle("POST /api/rpc", builderHandler)
		mux.Handle("GET /ws", builderHandler)
		mux.Handle("GET /api/ws", builderHandler)
	}
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
	if r == nil || r.URL == nil {
		return false
	}
	path := strings.TrimSpace(r.URL.Path)
	switch path {
	case "/api/rpc", "/api/ws":
		return false
	}
	return path == "/api" || strings.HasPrefix(path, "/api/")
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
	InFlightTurn        bool              `json:"in_flight_turn,omitempty"`
	InFlightSeconds     int               `json:"in_flight_seconds,omitempty"`
	LockOwner           string            `json:"lock_owner,omitempty"`
	LockExpiresAt       string            `json:"lock_expires_at,omitempty"`
	Failures24h         int               `json:"failures_24h,omitempty"`
	DeadLetters24h      int               `json:"dead_letters_24h,omitempty"`
	TurnCount           int               `json:"turn_count,omitempty"`
	TurnLimit           int               `json:"turn_limit,omitempty"`
	Turns24h            int               `json:"turns_24h,omitempty"`
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

type subjectStatusEntity struct {
	EntityID     string `json:"entity_id"`
	SubjectID    string `json:"subject_id,omitempty"`
	WorkflowName string `json:"workflow_name,omitempty"`
	FlowInstance string `json:"flow_instance,omitempty"`
	CurrentState string `json:"current_state,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

type subjectStatusResponse struct {
	SubjectID      string                   `json:"subject_id"`
	LatestEntityID string                   `json:"latest_entity_id,omitempty"`
	LatestFlow     string                   `json:"latest_flow,omitempty"`
	LatestState    string                   `json:"latest_state,omitempty"`
	EntityCount    int                      `json:"entity_count"`
	StatesByFlow   []instanceAggregateGroup `json:"states_by_flow,omitempty"`
	Entities       []subjectStatusEntity    `json:"entities"`
}

type controlResult struct {
	OK      bool   `json:"ok,omitempty"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

type directiveRequest struct {
	Message      string `json:"message"`
	KillPrevious bool   `json:"kill_previous,omitempty"`
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
	if h.agents == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("agents reader is not configured"))
		return
	}
	if richer, ok := h.agents.(genericAgentProvider); ok {
		rows, err := richer.ListGenericAgents(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"agents": rows})
		return
	}
	rows, err := h.agents.LoadAgents(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]genericAgent, 0, len(rows))
	for _, row := range rows {
		out = append(out, toGenericAgent(row))
	}
	writeJSON(w, http.StatusOK, map[string]any{"agents": out})
}

func (h *Handler) handleAgentDetail(w http.ResponseWriter, r *http.Request) {
	if h.agents == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("agents reader is not configured"))
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("agent id is required"))
		return
	}
	if richer, ok := h.agents.(genericAgentProvider); ok {
		row, ok, err := richer.GetGenericAgent(r.Context(), id)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
		if !ok {
			writeJSONError(w, http.StatusNotFound, errors.New("agent not found"))
			return
		}
		writeJSON(w, http.StatusOK, row)
		return
	}
	rows, err := h.agents.LoadAgents(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	for _, row := range rows {
		if strings.TrimSpace(row.Config.ID) != id {
			continue
		}
		writeJSON(w, http.StatusOK, toGenericAgent(row))
		return
	}
	writeJSONError(w, http.StatusNotFound, errors.New("agent not found"))
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	req.Message = strings.TrimSpace(req.Message)
	if req.Message == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("message is required"))
		return
	}
	resp, err := h.agentControl.ChatWithAgent(r.Context(), id, req.Message, req.KillPrevious)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, controlResult{OK: true, Message: strings.TrimSpace(resp)})
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
	if err := h.agentControl.RestartAgent(id); err != nil {
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
	if err := h.agentControl.ReplayAgentBacklog(r.Context(), id); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, controlResult{OK: true, Message: "agent backlog replayed"})
}

func (h *Handler) handleConversations(w http.ResponseWriter, r *http.Request) {
	if h.conversations == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("conversation reader is not configured"))
		return
	}
	limit := intQuery(r, "limit", 100)
	rows, err := h.conversations.List(r.Context(), limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"conversations": rows})
}

func (h *Handler) handleConversationDetail(w http.ResponseWriter, r *http.Request) {
	if h.conversations == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("conversation reader is not configured"))
		return
	}
	sessionID := strings.TrimSpace(r.PathValue("sessionID"))
	if sessionID == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("session id is required"))
		return
	}
	row, ok, err := h.conversations.Get(r.Context(), sessionID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeJSONError(w, http.StatusNotFound, errors.New("conversation not found"))
		return
	}
	writeJSON(w, http.StatusOK, row)
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
	if h.instances == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("instance reader is not configured"))
		return
	}
	rows, err := h.instances.List(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	rows = filterInstances(rows, r)
	writeJSON(w, http.StatusOK, map[string]any{"instances": rows})
}

func (h *Handler) handleInstanceDetail(w http.ResponseWriter, r *http.Request) {
	if h.instances == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("instance reader is not configured"))
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("instance id is required"))
		return
	}
	row, ok, err := h.instances.Load(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeJSONError(w, http.StatusNotFound, errors.New("instance not found"))
		return
	}
	writeJSON(w, http.StatusOK, row)
}

func (h *Handler) handleInstanceAggregate(w http.ResponseWriter, r *http.Request) {
	if h.instances == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("instance reader is not configured"))
		return
	}
	groupBy := strings.TrimSpace(r.URL.Query().Get("group_by"))
	if groupBy == "" {
		groupBy = "current_state"
	}
	rows, err := h.instances.List(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	rows = filterInstances(rows, r)
	counts := map[string]int{}
	for _, row := range rows {
		key := aggregateKey(row, groupBy)
		counts[key]++
	}
	out := make([]instanceAggregateGroup, 0, len(counts))
	for key, count := range counts {
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
		h.runtime.PauseIngress()
		writeJSON(w, http.StatusOK, controlResult{OK: true, Message: "runtime paused"})
	case "resume":
		h.runtime.ResumeIngress()
		writeJSON(w, http.StatusOK, controlResult{OK: true, Message: "runtime resumed"})
	case "reset_state":
		if err := h.runtime.ResetState(); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, controlResult{OK: true, Message: "runtime state reset"})
	default:
		writeJSONError(w, http.StatusBadRequest, errors.New("unsupported runtime action"))
	}
}

func (h *Handler) handleSubjectStatus(w http.ResponseWriter, r *http.Request) {
	if h.instances == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("instance reader is not configured"))
		return
	}
	subjectID := strings.TrimSpace(r.PathValue("id"))
	if subjectID == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("subject id is required"))
		return
	}
	rows, err := h.instances.List(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	filtered := make([]runtimepipeline.WorkflowInstance, 0, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row.SubjectID) == subjectID {
			filtered = append(filtered, row)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].UpdatedAt.Equal(filtered[j].UpdatedAt) {
			if filtered[i].CreatedAt.Equal(filtered[j].CreatedAt) {
				return filtered[i].InstanceID < filtered[j].InstanceID
			}
			return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
		}
		return filtered[i].UpdatedAt.After(filtered[j].UpdatedAt)
	})
	if len(filtered) == 0 {
		writeJSONError(w, http.StatusNotFound, errors.New("subject not found"))
		return
	}
	resp := subjectStatusResponse{
		SubjectID:      subjectID,
		LatestEntityID: strings.TrimSpace(filtered[0].InstanceID),
		LatestFlow:     strings.TrimSpace(filtered[0].WorkflowName),
		LatestState:    strings.TrimSpace(filtered[0].CurrentState),
		EntityCount:    len(filtered),
		Entities:       make([]subjectStatusEntity, 0, len(filtered)),
	}
	byFlowState := make(map[string]int, len(filtered))
	for _, row := range filtered {
		resp.Entities = append(resp.Entities, subjectStatusEntity{
			EntityID:     strings.TrimSpace(row.InstanceID),
			SubjectID:    strings.TrimSpace(row.SubjectID),
			WorkflowName: strings.TrimSpace(row.WorkflowName),
			FlowInstance: strings.TrimSpace(row.StorageRef),
			CurrentState: strings.TrimSpace(row.CurrentState),
			UpdatedAt:    formatTime(row.UpdatedAt),
		})
		key := strings.TrimSpace(row.WorkflowName)
		if key == "" {
			key = "unknown"
		}
		state := strings.TrimSpace(row.CurrentState)
		if state == "" {
			state = "unknown"
		}
		byFlowState[key+":"+state]++
	}
	resp.StatesByFlow = make([]instanceAggregateGroup, 0, len(byFlowState))
	for key, count := range byFlowState {
		resp.StatesByFlow = append(resp.StatesByFlow, instanceAggregateGroup{Key: key, Count: count})
	}
	sort.Slice(resp.StatesByFlow, func(i, j int) bool {
		if resp.StatesByFlow[i].Count == resp.StatesByFlow[j].Count {
			return resp.StatesByFlow[i].Key < resp.StatesByFlow[j].Key
		}
		return resp.StatesByFlow[i].Count > resp.StatesByFlow[j].Count
	})
	writeJSON(w, http.StatusOK, resp)
}

func filterInstances(rows []runtimepipeline.WorkflowInstance, r *http.Request) []runtimepipeline.WorkflowInstance {
	workflowName := strings.TrimSpace(r.URL.Query().Get("workflow_name"))
	currentState := strings.TrimSpace(r.URL.Query().Get("current_state"))
	subjectID := strings.TrimSpace(r.URL.Query().Get("subject_id"))
	entityID := strings.TrimSpace(r.URL.Query().Get("entity_id"))
	limit := intQuery(r, "limit", 0)
	out := make([]runtimepipeline.WorkflowInstance, 0, len(rows))
	for _, row := range rows {
		if workflowName != "" && row.WorkflowName != workflowName {
			continue
		}
		if currentState != "" && row.CurrentState != currentState {
			continue
		}
		if subjectID != "" && strings.TrimSpace(row.SubjectID) != subjectID {
			continue
		}
		if entityID != "" && row.InstanceID != entityID {
			continue
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].InstanceID < out[j].InstanceID
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	if limit > 0 && len(out) > limit {
		return out[:limit]
	}
	return out
}

func aggregateKey(row runtimepipeline.WorkflowInstance, groupBy string) string {
	switch groupBy {
	case "workflow_name":
		if strings.TrimSpace(row.WorkflowName) == "" {
			return "unknown"
		}
		return row.WorkflowName
	case "workflow_version":
		if strings.TrimSpace(row.WorkflowVersion) == "" {
			return "unknown"
		}
		return row.WorkflowVersion
	case "subject_id":
		if strings.TrimSpace(row.SubjectID) == "" {
			return "unknown"
		}
		return row.SubjectID
	case "current_state":
		fallthrough
	default:
		if strings.TrimSpace(row.CurrentState) == "" {
			return "unknown"
		}
		return row.CurrentState
	}
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
