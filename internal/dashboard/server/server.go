package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimetools "empireai/internal/runtime/tools"
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
	AgentID     string         `json:"agent_id"`
	ScopeKey    string         `json:"scope_key,omitempty"`
	Scope       string         `json:"scope,omitempty"`
	RuntimeMode string         `json:"runtime_mode,omitempty"`
	Status      string         `json:"status,omitempty"`
	TurnCount   int            `json:"turn_count,omitempty"`
	Summary     string         `json:"summary,omitempty"`
	UpdatedAt   string         `json:"updated_at,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type ConversationDetail struct {
	AgentID      string         `json:"agent_id"`
	ScopeKey     string         `json:"scope_key,omitempty"`
	Scope        string         `json:"scope,omitempty"`
	RuntimeMode  string         `json:"runtime_mode,omitempty"`
	Status       string         `json:"status,omitempty"`
	TurnCount    int            `json:"turn_count,omitempty"`
	Summary      string         `json:"summary,omitempty"`
	UpdatedAt    string         `json:"updated_at,omitempty"`
	Messages     []any          `json:"messages"`
	RuntimeState map[string]any `json:"runtime_state,omitempty"`
}

type ConversationReader interface {
	List(ctx context.Context, limit int) ([]ConversationSummary, error)
	Get(ctx context.Context, agentID string) (ConversationDetail, bool, error)
}

type AgentController interface {
	RestartAgent(agentID string) error
	ReplayAgentBacklog(ctx context.Context, agentID string) error
	ChatWithAgent(ctx context.Context, agentID, directive string) (string, error)
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
	Runtime       RuntimeController
}

type Handler struct {
	health        HealthChecker
	agents        AgentReader
	agentControl  AgentController
	mailbox       MailboxReader
	instances     InstanceReader
	conversations ConversationReader
	runtime       RuntimeController
}

func NewHandler(opts Options) http.Handler {
	h := &Handler{
		health:        opts.Health,
		agents:        opts.Agents,
		agentControl:  opts.AgentControl,
		mailbox:       opts.Mailbox,
		instances:     opts.Instances,
		conversations: opts.Conversations,
		runtime:       opts.Runtime,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", h.handleHealth)
	mux.HandleFunc("GET /api/agents", h.handleAgents)
	mux.HandleFunc("GET /api/agents/{id}", h.handleAgentDetail)
	mux.HandleFunc("POST /api/agents/{id}/actions/directive", h.handleAgentDirective)
	mux.HandleFunc("POST /api/agents/{id}/actions/restart", h.handleAgentRestart)
	mux.HandleFunc("POST /api/agents/{id}/actions/replay", h.handleAgentReplay)
	mux.HandleFunc("GET /api/conversations", h.handleConversations)
	mux.HandleFunc("GET /api/conversations/{agentID}", h.handleConversationDetail)
	mux.HandleFunc("GET /api/mailbox", h.handleMailbox)
	mux.HandleFunc("GET /api/mailbox/{id}", h.handleMailboxDetail)
	mux.HandleFunc("GET /api/instances/aggregate", h.handleInstanceAggregate)
	mux.HandleFunc("GET /api/instances/{id}", h.handleInstanceDetail)
	mux.HandleFunc("GET /api/instances", h.handleInstances)
	mux.HandleFunc("POST /api/runtime/actions", h.handleRuntimeAction)
	return mux
}

type genericAgent struct {
	ID                  string         `json:"id"`
	Type                string         `json:"type,omitempty"`
	Role                string         `json:"role,omitempty"`
	Mode                string         `json:"mode,omitempty"`
	Status              string         `json:"status,omitempty"`
	State               string         `json:"state,omitempty"`
	EntityID            string         `json:"entity_id,omitempty"`
	ParentAgentID       string         `json:"parent_agent_id,omitempty"`
	CoordinatorID       string         `json:"coordinator_id,omitempty"`
	HiredBy             string         `json:"hired_by,omitempty"`
	TemplateVersion     string         `json:"template_version,omitempty"`
	BudgetEnvelope      float64        `json:"budget_envelope,omitempty"`
	Subscriptions       []string       `json:"subscriptions,omitempty"`
	Permissions         []string       `json:"permissions,omitempty"`
	PendingEvents       int            `json:"pending_events,omitempty"`
	OldestPendingAgeSec int            `json:"oldest_pending_age_sec,omitempty"`
	InFlightTurn        bool           `json:"in_flight_turn,omitempty"`
	InFlightSeconds     int            `json:"in_flight_seconds,omitempty"`
	LockOwner           string         `json:"lock_owner,omitempty"`
	LockExpiresAt       string         `json:"lock_expires_at,omitempty"`
	Failures24h         int            `json:"failures_24h,omitempty"`
	DeadLetters24h      int            `json:"dead_letters_24h,omitempty"`
	TurnCount           int            `json:"turn_count,omitempty"`
	TurnLimit           int            `json:"turn_limit,omitempty"`
	Turns24h            int            `json:"turns_24h,omitempty"`
	TotalTokens24h      int            `json:"total_tokens_24h,omitempty"`
	NearBreaker         bool           `json:"near_breaker,omitempty"`
	CurrentTaskID       string         `json:"current_task_id,omitempty"`
	LastTool            map[string]any `json:"last_tool,omitempty"`
	StartedAt           string         `json:"started_at,omitempty"`
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
	resp, err := h.agentControl.ChatWithAgent(r.Context(), id, req.Message)
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
	agentID := strings.TrimSpace(r.PathValue("agentID"))
	if agentID == "" {
		writeJSONError(w, http.StatusBadRequest, errors.New("agent id is required"))
		return
	}
	row, ok, err := h.conversations.Get(r.Context(), agentID)
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

func filterInstances(rows []runtimepipeline.WorkflowInstance, r *http.Request) []runtimepipeline.WorkflowInstance {
	workflowName := strings.TrimSpace(r.URL.Query().Get("workflow_name"))
	currentState := strings.TrimSpace(r.URL.Query().Get("current_state"))
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, err error) {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, map[string]any{"error": err.Error()})
}
