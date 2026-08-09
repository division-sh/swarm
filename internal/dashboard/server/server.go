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

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

type HealthChecker func(ctx context.Context) (map[string]any, error)

type MailboxReader interface {
	ListMailboxItems(ctx context.Context, status string, limit int) ([]runtimetools.MailboxItem, error)
	GetMailboxItem(ctx context.Context, id string) (runtimetools.MailboxItem, error)
}

type EntityReader interface {
	ListOperatorEntities(ctx context.Context, opts operatorread.OperatorEntityListOptions) (operatorread.OperatorEntityListResult, error)
	LoadOperatorEntity(ctx context.Context, entityID, runID string) (operatorread.OperatorEntityFull, error)
	AggregateOperatorEntities(ctx context.Context, opts operatorread.OperatorEntityAggregateOptions) (operatorread.OperatorEntityAggregateResult, error)
}

type ConversationSummary struct {
	SessionID    string                      `json:"session_id,omitempty"`
	AgentID      string                      `json:"agent_id"`
	Kind         string                      `json:"kind,omitempty"`
	FlowInstance string                      `json:"flow_instance,omitempty"`
	Memory       bool                        `json:"memory"`
	MemorySource string                      `json:"memory_source,omitempty"`
	Status       string                      `json:"status,omitempty"`
	TurnCount    int                         `json:"turn_count,omitempty"`
	Summary      string                      `json:"summary,omitempty"`
	UpdatedAt    string                      `json:"updated_at,omitempty"`
	Metadata     ConversationSummaryMetadata `json:"metadata,omitempty"`
}

type ConversationSummaryMetadata struct {
	ProviderSessionID    string                       `json:"provider_session_id,omitempty"`
	RetryReason          string                       `json:"retry_reason,omitempty"`
	RetriesFromSessionID string                       `json:"retries_from_session_id,omitempty"`
	Watchdog             *ConversationRuntimeWatchdog `json:"watchdog,omitempty"`
	LiveTurn             *OperatorLiveTurn            `json:"live_turn,omitempty"`
}

type ConversationDetail struct {
	Conversation ConversationSummary        `json:"conversation"`
	Turns        []ConversationTurnListItem `json:"turns"`
	NextCursor   string                     `json:"next_cursor,omitempty"`
}

type ConversationTurnListItem struct {
	TurnID           string                     `json:"turn_id"`
	Ordinal          int                        `json:"ordinal"`
	CompletedAt      string                     `json:"completed_at"`
	DurationMS       int                        `json:"duration_ms"`
	TriggerEventID   string                     `json:"trigger_event_id,omitempty"`
	TriggerEventType string                     `json:"trigger_event_type,omitempty"`
	ActivityCounts   ConversationActivityCounts `json:"activity_counts"`
	Tokens           *ConversationTokenUsage    `json:"tokens,omitempty"`
	Outcome          string                     `json:"outcome,omitempty"`
	ParseOK          bool                       `json:"parse_ok"`
	Failure          *runtimefailures.Envelope  `json:"failure,omitempty"`
}

type ConversationActivityCounts struct {
	Dispatch   int `json:"dispatch"`
	Tool       int `json:"tool"`
	ToolResult int `json:"tool_result"`
	Publish    int `json:"publish"`
	Output     int `json:"output"`
	Failure    int `json:"failure"`
}

type ConversationTokenUsage struct {
	Input     int64  `json:"input"`
	Output    int64  `json:"output"`
	Exactness string `json:"exactness"`
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
	LastTool               *AgentLastTool `json:"last_tool,omitempty"`
}

type ConversationReader interface {
	ListOperatorConversations(ctx context.Context, opts operatorread.OperatorConversationListOptions) (operatorread.OperatorConversationListResult, error)
	ListOperatorConversationTurns(ctx context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error)
	LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (operatorread.OperatorPublicConversationTurnDetail, error)
}

type ObservabilityReader interface {
	ListEvents(ctx context.Context, filter EventFilter, limit int) ([]eventRecord, error)
	GetEvent(ctx context.Context, id string) (eventRecord, bool, error)
	ListRuntimeLogs(ctx context.Context, filter RuntimeLogFilter, limit int) ([]runtimeLogRecord, error)
	ListIncidents(ctx context.Context, filter IncidentFilter) ([]incidentRecord, error)
}

type RunTraceReader interface {
	LoadRunDebugTrace(ctx context.Context, runID string, opts operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, error)
}

type RuntimeController interface {
	PauseIngress() error
	ResumeIngress() error
}

type Options struct {
	Health        HealthChecker
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

type AgentLastTool struct {
	Name      string `json:"name"`
	ToolUseID string `json:"tool_use_id,omitempty"`
	OK        bool   `json:"ok"`
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

type runtimeActionRequest struct {
	Action string `json:"action"`
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if h.health == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("health checker is not configured"))
		return
	}
	resp := map[string]any{
		"ok":        true,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	checks, err := h.health(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	resp["checks"] = checks
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleConversations(w http.ResponseWriter, r *http.Request) {
	if h.conversations == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("conversation reader is not configured"))
		return
	}
	result, err := h.conversations.ListOperatorConversations(r.Context(), operatorread.OperatorConversationListOptions{
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
	writeJSON(w, http.StatusOK, map[string]any{"conversations": rows, "next_cursor": strings.TrimSpace(result.NextCursor)})
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
	row, err := h.conversations.ListOperatorConversationTurns(r.Context(), operatorread.OperatorConversationTurnListOptions{
		SessionID: sessionID,
		Limit:     intQuery(r, "limit", 50),
		Cursor:    strings.TrimSpace(r.URL.Query().Get("cursor")),
	})
	if err != nil {
		if errors.Is(err, operatorread.ErrSessionNotFound) {
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
	if h.observability == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("observability reader is not configured"))
		return
	}
	filter, err := dashboardEventFilterFromRequest(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	rows, err := h.observability.ListEvents(r.Context(), filter, intQuery(r, "limit", 200))
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": rows})
}

func (h *Handler) handleEventDetail(w http.ResponseWriter, r *http.Request) {
	if h.observability == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("observability reader is not configured"))
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
	if h.observability == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("observability reader is not configured"))
		return
	}
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
	emit := func() bool {
		rows, err := h.observability.ListEvents(r.Context(), filter, intQuery(r, "limit", 200))
		if err != nil {
			writeDashboardStreamError(w, flusher, err)
			return false
		}
		if len(rows) == 0 {
			return true
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
		return true
	}
	if !emit() {
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case <-poll.C:
			if !emit() {
				return
			}
		}
	}
}

func (h *Handler) handleRuntimeLogs(w http.ResponseWriter, r *http.Request) {
	if h.observability == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("observability reader is not configured"))
		return
	}
	filter := RuntimeLogFilter{
		Type:      strings.TrimSpace(r.URL.Query().Get("type")),
		Source:    strings.TrimSpace(r.URL.Query().Get("source")),
		EntityID:  strings.TrimSpace(r.URL.Query().Get("entity_id")),
		Component: strings.TrimSpace(r.URL.Query().Get("component")),
		Level:     strings.TrimSpace(r.URL.Query().Get("level")),
		ErrorCode: strings.TrimSpace(r.URL.Query().Get("error_code")),
		Order:     strings.TrimSpace(r.URL.Query().Get("order")),
	}
	rows, err := h.observability.ListRuntimeLogs(r.Context(), filter, intQuery(r, "limit", 200))
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runtime_logs": rows})
}

func (h *Handler) handleRuntimeIncidents(w http.ResponseWriter, r *http.Request) {
	if h.observability == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("observability reader is not configured"))
		return
	}
	filter := IncidentFilter{
		SinceHours: intQuery(r, "since_hours", 24),
		MCPOnly:    strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("mcp_only")), "true"),
		Level:      strings.TrimSpace(r.URL.Query().Get("level")),
		Component:  strings.TrimSpace(r.URL.Query().Get("component")),
		Limit:      intQuery(r, "limit", 2000),
	}
	rows, err := h.observability.ListIncidents(r.Context(), filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
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
	rows, err := h.runTrace.LoadRunDebugTrace(r.Context(), runID, operatorread.RunDebugTraceQueryOptions{
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
	if h.observability == nil {
		writeJSONError(w, http.StatusNotImplemented, errors.New("observability reader is not configured"))
		return
	}
	includeRuntime := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("include_runtime")), "true")
	eventFilter, err := dashboardEventFilterFromRequest(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	eventFilter.After = time.Now().UTC().Add(-2 * time.Second)

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
	emit := func() bool {
		rows, err := h.observability.ListEvents(r.Context(), eventFilter, 50)
		if err != nil {
			writeDashboardStreamError(w, flusher, err)
			return false
		}
		if len(rows) > 0 {
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
		if !includeRuntime {
			return true
		}
		logs, err := h.observability.ListRuntimeLogs(r.Context(), logFilter, 50)
		if err != nil {
			writeDashboardStreamError(w, flusher, err)
			return false
		}
		if len(logs) > 0 {
			latest := logFilter.After
			for i := len(logs) - 1; i >= 0; i-- {
				_, _ = fmt.Fprintf(w, "event: runtime_log\ndata: {\"id\":%q}\n\n", logs[i].ID)
				if ts, err := time.Parse(time.RFC3339, logs[i].TS); err == nil && ts.After(latest) {
					latest = ts
				}
			}
			logFilter.After = latest
			flusher.Flush()
		}
		return true
	}
	if !emit() {
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case <-poll.C:
			if !emit() {
				return
			}
		}
	}
}

func writeDashboardStreamError(w http.ResponseWriter, flusher http.Flusher, err error) {
	payload, marshalErr := json.Marshal(map[string]string{"error": err.Error()})
	if marshalErr != nil {
		payload = []byte(`{"error":"dashboard stream read failed"}`)
	}
	_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", payload)
	flusher.Flush()
}

func dashboardEventFilterFromRequest(r *http.Request) (EventFilter, error) {
	query := r.URL.Query()
	subscriberID := strings.TrimSpace(query.Get("subscriber_id"))
	if strings.TrimSpace(query.Get("subscriber")) != "" {
		return EventFilter{}, errors.New("subscriber is unsupported; use subscriber_id")
	}
	subscriberType := strings.TrimSpace(query.Get("subscriber_type"))
	if subscriberType != "" {
		if _, ok := dashboardEventSubscriberTypes[subscriberType]; !ok {
			return EventFilter{}, fmt.Errorf("subscriber_type=%q is not a valid SubscriberType", subscriberType)
		}
	}
	return EventFilter{
		Type:           strings.TrimSpace(query.Get("type")),
		Source:         strings.TrimSpace(query.Get("source")),
		EntityID:       strings.TrimSpace(query.Get("entity_id")),
		SubscriberID:   subscriberID,
		SubscriberType: subscriberType,
	}, nil
}

var dashboardEventSubscriberTypes = map[string]struct{}{
	"agent": {},
	"node":  {},
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

func dashboardEntityListOptions(r *http.Request) (operatorread.OperatorEntityListOptions, error) {
	opts := operatorread.OperatorEntityListOptions{
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

func dashboardEntityAggregateOptions(r *http.Request, groupBy string) (operatorread.OperatorEntityAggregateOptions, error) {
	return operatorread.OperatorEntityAggregateOptions{
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
	case errors.Is(err, operatorread.ErrEntityNotFound):
		writeJSONError(w, http.StatusNotFound, errors.New("entity not found"))
	case errors.Is(err, operatorread.ErrAmbiguousEntityRunID):
		writeJSONError(w, http.StatusBadRequest, errors.New("run_id is required when entity_id exists in multiple runs"))
	case errors.Is(err, operatorread.ErrInvalidEntityCursor), errors.Is(err, operatorread.ErrInvalidEntityReadParam):
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

func dashboardAgentLastTool(item *operatorread.OperatorAgentTool) *AgentLastTool {
	if item == nil {
		return nil
	}
	return &AgentLastTool{
		Name:      strings.TrimSpace(item.Name),
		ToolUseID: strings.TrimSpace(item.ToolUseID),
		OK:        item.OK,
	}
}

func dashboardLiveTurn(item *operatorread.OperatorLiveTurn) *OperatorLiveTurn {
	if item == nil {
		return nil
	}
	return &OperatorLiveTurn{
		TurnID:                 strings.TrimSpace(item.TurnID),
		TaskID:                 strings.TrimSpace(item.TaskID),
		ParseOK:                item.ParseOK,
		AssistantVisibleOutput: strings.TrimSpace(item.AssistantVisibleOutput),
		Outcome:                strings.TrimSpace(item.Outcome),
		LastTool:               dashboardAgentLastTool(item.LastTool),
	}
}

func writeJSONError(w http.ResponseWriter, status int, err error) {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, map[string]any{"error": err.Error()})
}
