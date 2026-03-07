package dashboard

import (
	"bufio"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/digest"
	"empireai/internal/events"
	mailboxsvc "empireai/internal/mailbox"
	runtimebus "empireai/internal/runtime/bus"
	runtimemanager "empireai/internal/runtime/manager"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/specaudit"
	"empireai/internal/store"
	"empireai/internal/templateops"
	"github.com/google/uuid"
)

//go:embed assets/*
var dashboardAssets embed.FS

var (
	dashboardPage   []byte
	dashboardStatic fs.FS
)

func init() {
	var err error
	dashboardPage, err = dashboardAssets.ReadFile("assets/dashboard.html")
	if err != nil {
		panic(fmt.Sprintf("load embedded dashboard.html: %v", err))
	}
	dashboardStatic, err = fs.Sub(dashboardAssets, "assets")
	if err != nil {
		panic(fmt.Sprintf("prepare embedded dashboard static fs: %v", err))
	}
}

type Server struct {
	db           *sql.DB
	cfg          *config.Config
	now          func() time.Time
	eventStore   runtimebus.EventStore
	mailboxStore runtimetools.MailboxPersistence
	manager      *runtimemanager.AgentManager
}

func NewServer(
	db *sql.DB,
	cfg *config.Config,
	eventStore runtimebus.EventStore,
	mailboxStore runtimetools.MailboxPersistence,
	manager *runtimemanager.AgentManager,
) *Server {
	return &Server{
		db:           db,
		cfg:          cfg,
		now:          time.Now,
		eventStore:   eventStore,
		mailboxStore: mailboxStore,
		manager:      manager,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/dashboard", s.handlePage)
	mux.HandleFunc("/dashboard/", s.handlePage)
	mux.Handle("/dashboard/assets/", http.StripPrefix("/dashboard/assets/", http.FileServer(http.FS(dashboardStatic))))
	mux.HandleFunc("/dashboard/api/overview", s.handleOverview)
	mux.HandleFunc("/dashboard/api/agents", s.handleAgents)
	mux.HandleFunc("/dashboard/api/agents/", s.handleAPIAgentPrompt)
	mux.HandleFunc("/dashboard/api/events", s.handleEvents)
	mux.HandleFunc("/dashboard/api/events/stream", s.handleEventStream)
	mux.HandleFunc("/dashboard/api/events/flow", s.handleFlowEvents)
	mux.HandleFunc("/dashboard/api/events/", s.handleEventDetail)
	mux.HandleFunc("/dashboard/api/runtime/logs", s.handleRuntimeLogs)
	mux.HandleFunc("/dashboard/api/runtime/incidents", s.handleRuntimeIncidents)
	mux.HandleFunc("/dashboard/api/conversations", s.handleConversations)
	mux.HandleFunc("/dashboard/api/conversations/", s.handleConversationDetail)
	mux.HandleFunc("/dashboard/api/funnel", s.handleFunnel)
	mux.HandleFunc("/dashboard/api/pipeline/shards", s.handlePipelineShards)
	mux.HandleFunc("/dashboard/api/pipeline/shards/", s.handlePipelineShardDetail)
	mux.HandleFunc("/dashboard/api/mailbox", s.handleMailbox)
	mux.HandleFunc("/dashboard/api/tasks", s.handleTasks)
	mux.HandleFunc("/dashboard/api/tasks/stats", s.handleTaskStats)
	mux.HandleFunc("/dashboard/api/tasks/", s.handleTaskDetail)
	mux.HandleFunc("/dashboard/api/digest", s.handleDigest)
	mux.HandleFunc("/dashboard/api/health", s.handleHealth)
	mux.HandleFunc("/dashboard/api/health/pipeline", s.handlePipelineHealth)
	mux.HandleFunc("/dashboard/api/graph", s.handleGraph)
	mux.HandleFunc("/dashboard/api/pipeline/graph", s.handlePipelineGraph)
	mux.HandleFunc("/dashboard/api/control/targets", s.handleControlTargets)
	mux.HandleFunc("/dashboard/api/control/seed-org", s.handleControlSeedOrg)
	mux.HandleFunc("/dashboard/api/control/verticals/create", s.handleControlCreateVertical)
	mux.HandleFunc("/dashboard/api/control/agents/restart", s.handleControlAgentRestart)
	mux.HandleFunc("/dashboard/api/control/agents/replay", s.handleControlAgentReplay)
	mux.HandleFunc("/dashboard/api/control/events/requeue", s.handleControlEventRequeue)
	mux.HandleFunc("/dashboard/api/control/runtime", s.handleControlRuntime)
	mux.HandleFunc("/dashboard/api/control/directive", s.handleControlDirective)
	mux.HandleFunc("/dashboard/api/control/chat", s.handleControlChat)
	mux.HandleFunc("/dashboard/api/control/mailbox/decide", s.handleControlMailboxDecide)
	mux.HandleFunc("/dashboard/api/holding", s.handleHolding)
	mux.HandleFunc("/dashboard/api/holding/vertical", s.handleHoldingVerticalDetail)
	mux.HandleFunc("/dashboard/api/verticals/", s.handleVerticalTrace)
	mux.HandleFunc("/dashboard/api/templates/publish", s.handleAPITemplatePublish)
	mux.HandleFunc("/dashboard/api/templates/", s.handleAPITemplatePrompt)

	// Spec v2.0 API surface aliases (Phase 1). These are the "real" API routes.
	mux.HandleFunc("/api/tasks", s.handleTasks)
	mux.HandleFunc("/api/tasks/stats", s.handleTaskStats)
	mux.HandleFunc("/api/tasks/", s.handleTaskDetail)
	mux.HandleFunc("/api/mailbox", s.handleMailbox)
	mux.HandleFunc("/api/mailbox/", s.handleAPIMailboxDetail)
	mux.HandleFunc("/api/events", s.handleAPIEvents)
	mux.HandleFunc("/api/events/flow", s.handleFlowEvents)
	mux.HandleFunc("/api/events/", s.handleEventDetail)
	mux.HandleFunc("/api/runtime/logs", s.handleRuntimeLogs)
	mux.HandleFunc("/api/runtime/incidents", s.handleRuntimeIncidents)
	mux.HandleFunc("/api/verticals", s.handleAPIVerticals)
	mux.HandleFunc("/api/verticals/", s.handleAPIVerticalDetail)
	mux.HandleFunc("/api/chat/", s.handleAPIChat)
	mux.HandleFunc("/api/conversations", s.handleConversations)
	mux.HandleFunc("/api/conversations/", s.handleConversationDetail)
	mux.HandleFunc("/api/agents/", s.handleAPIAgentPrompt)
	mux.HandleFunc("/api/templates/publish", s.handleAPITemplatePublish)
	mux.HandleFunc("/api/templates/", s.handleAPITemplatePrompt)
	mux.HandleFunc("/api/directive", s.handleAPIDirective)
	mux.HandleFunc("/api/budget", s.handleAPIBudget)
	mux.HandleFunc("/api/holding", s.handleHolding)
	mux.HandleFunc("/api/holding/vertical", s.handleHoldingVerticalDetail)
	mux.HandleFunc("/api/health/pipeline", s.handlePipelineHealth)
	mux.HandleFunc("/api/pipeline/shards", s.handlePipelineShards)
	mux.HandleFunc("/api/pipeline/shards/", s.handlePipelineShardDetail)
	mux.HandleFunc("/api/pipeline/graph", s.handlePipelineGraph)
	mux.HandleFunc("/api/graph", s.handleGraph)
	mux.HandleFunc("/health/pipeline", s.handlePipelineHealth)
	return s.authMiddleware(mux)
}

func (s *Server) handleAPIAgentPrompt(w http.ResponseWriter, r *http.Request) {
	prefix := "/api/agents/"
	if strings.HasPrefix(r.URL.Path, "/dashboard/api/agents/") {
		prefix = "/dashboard/api/agents/"
	}
	path := strings.TrimPrefix(r.URL.Path, prefix)
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" || parts[1] != "prompt" {
		http.NotFound(w, r)
		return
	}
	if s.manager == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
		return
	}
	agentID := strings.TrimSpace(parts[0])

	// GET /api/agents/:id/prompt/diff
	if len(parts) == 3 && parts[2] == "diff" {
		if !allowMethod(w, r, http.MethodGet) {
			return
		}
		state, err := s.manager.GetAgentPromptState(r.Context(), agentID)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		overridePrompt := ""
		if state.Override != nil {
			overridePrompt = strings.TrimSpace(state.Override.Prompt)
		}
		diff := renderPromptDiff(state.TemplatePrompt, overridePrompt)
		writeJSON(w, http.StatusOK, map[string]any{
			"agent_id":        state.AgentID,
			"role":            state.Role,
			"mode":            state.Mode,
			"template_prompt": state.TemplatePrompt,
			"override_prompt": overridePrompt,
			"has_override":    state.Override != nil,
			"diff":            diff,
			"generated_at":    s.now().UTC(),
		})
		return
	}

	// /api/agents/:id/prompt
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		state, err := s.manager.GetAgentPromptState(r.Context(), agentID)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		override := map[string]any(nil)
		if state.Override != nil {
			override = map[string]any{
				"prompt":          state.Override.Prompt,
				"previous_prompt": state.Override.PreviousPrompt,
				"source":          state.Override.Source,
				"notes":           state.Override.Notes,
				"created_at":      state.Override.CreatedAt,
				"updated_at":      state.Override.UpdatedAt,
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"agent_id":         state.AgentID,
			"role":             state.Role,
			"mode":             state.Mode,
			"template_prompt":  state.TemplatePrompt,
			"effective_prompt": state.EffectivePrompt,
			"has_override":     state.Override != nil,
			"override":         override,
			"generated_at":     s.now().UTC(),
		})
	case http.MethodPut:
		var req struct {
			Prompt string `json:"prompt"`
			Source string `json:"source"`
			Notes  string `json:"notes"`
		}
		if err := decodeJSONBody(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if strings.TrimSpace(req.Prompt) == "" {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("prompt is required"))
			return
		}
		source := strings.TrimSpace(req.Source)
		if source == "" {
			source = "api"
		}
		if err := s.manager.SetAgentPromptOverride(r.Context(), agentID, req.Prompt, source, req.Notes); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"agent_id": agentID,
			"action":   "set_override",
		})
	case http.MethodDelete:
		if err := s.manager.RevertAgentPromptOverride(r.Context(), agentID); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"agent_id": agentID,
			"action":   "revert_override",
		})
	default:
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
	}
}

func (s *Server) handleAPITemplatePrompt(w http.ResponseWriter, r *http.Request) {
	prefix := "/api/templates/"
	if strings.HasPrefix(r.URL.Path, "/dashboard/api/templates/") {
		prefix = "/dashboard/api/templates/"
	}
	path := strings.TrimPrefix(r.URL.Path, prefix)
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || parts[1] != "prompt" {
		http.NotFound(w, r)
		return
	}
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("database unavailable"))
		return
	}
	role := strings.TrimSpace(parts[0])
	switch r.Method {
	case http.MethodGet:
		templatePrompt, version, err := s.loadTemplatePromptForRole(r.Context(), role)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		draft, ok, err := s.loadTemplatePromptDraft(r.Context(), role)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		effective := templatePrompt
		if ok {
			effective = draft.Prompt
		}
		resp := map[string]any{
			"role":             role,
			"template_version": version,
			"template_prompt":  templatePrompt,
			"effective_prompt": effective,
			"has_draft":        ok,
			"generated_at":     s.now().UTC(),
		}
		if ok {
			resp["draft"] = map[string]any{
				"prompt":     draft.Prompt,
				"source":     draft.Source,
				"notes":      draft.Notes,
				"created_at": draft.CreatedAt,
				"updated_at": draft.UpdatedAt,
			}
		}
		writeJSON(w, http.StatusOK, resp)
	case http.MethodPut:
		var req struct {
			Prompt string `json:"prompt"`
			Source string `json:"source"`
			Notes  string `json:"notes"`
		}
		if err := decodeJSONBody(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if strings.TrimSpace(req.Prompt) == "" {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("prompt is required"))
			return
		}
		if _, _, err := s.loadTemplatePromptForRole(r.Context(), role); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		source := strings.TrimSpace(req.Source)
		if source == "" {
			source = "api"
		}
		if err := s.upsertTemplatePromptDraft(r.Context(), role, req.Prompt, source, req.Notes); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"role":    role,
			"action":  "set_draft",
			"updated": s.now().UTC(),
		})
	default:
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
	}
}

func (s *Server) handleAPITemplatePublish(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("database unavailable"))
		return
	}
	var req struct {
		Version     string `json:"version"`
		CreatedBy   string `json:"created_by"`
		Description string `json:"description"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.Version = strings.TrimSpace(req.Version)
	if req.Version == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("version is required"))
		return
	}
	rec, err := s.loadLatestTemplateRecord(r.Context())
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	agents, appliedRoles, err := s.applyPromptDrafts(r.Context(), rec.Agents)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	env := mustJSON(map[string]any{
		"version":          req.Version,
		"agents":           json.RawMessage(agents),
		"bootstrap_routes": json.RawMessage(rec.BootstrapRoutes),
		"seeded_routes":    json.RawMessage(rec.SeededRoutes),
	})
	if audit := specaudit.Validate("template", env); !audit.Passed {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"ok":          false,
			"error":       "template publish failed spec audit",
			"issue_count": len(audit.Issues),
			"issues":      audit.Issues,
		})
		return
	}
	createdBy := strings.TrimSpace(req.CreatedBy)
	if createdBy == "" {
		createdBy = "factory-cto"
	}
	svc := templateops.NewService(s.db, s.mailboxStore)
	if err := svc.PublishTemplate(r.Context(), req.Version, agents, rec.BootstrapRoutes, rec.SeededRoutes, createdBy, req.Description); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if err := s.clearTemplatePromptDrafts(r.Context()); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":               true,
		"version":          req.Version,
		"previous_version": rec.Version,
		"published_roles":  appliedRoles,
		"published_at":     s.now().UTC(),
	})
}

type templatePromptDraft struct {
	Role      string
	Prompt    string
	Source    string
	Notes     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (s *Server) ensureTemplatePromptDraftsTable(ctx context.Context) error {
	if s.db == nil {
		return fmt.Errorf("database unavailable")
	}
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS template_prompt_drafts (
			role        TEXT PRIMARY KEY,
			prompt      TEXT NOT NULL,
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	return err
}

func (s *Server) loadTemplatePromptDraft(ctx context.Context, role string) (templatePromptDraft, bool, error) {
	if err := s.ensureTemplatePromptDraftsTable(ctx); err != nil {
		return templatePromptDraft{}, false, err
	}
	role = strings.TrimSpace(role)
	var d templatePromptDraft
	err := s.db.QueryRowContext(ctx, `
		SELECT role, prompt, updated_at
		FROM template_prompt_drafts
		WHERE role = $1
	`, role).Scan(&d.Role, &d.Prompt, &d.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return templatePromptDraft{}, false, nil
		}
		return templatePromptDraft{}, false, err
	}
	d.Source = "template_draft"
	d.CreatedAt = d.UpdatedAt
	return d, true, nil
}

func (s *Server) loadAllTemplatePromptDrafts(ctx context.Context) (map[string]templatePromptDraft, error) {
	if err := s.ensureTemplatePromptDraftsTable(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT role, prompt, updated_at
		FROM template_prompt_drafts
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]templatePromptDraft)
	for rows.Next() {
		var d templatePromptDraft
		if err := rows.Scan(&d.Role, &d.Prompt, &d.UpdatedAt); err != nil {
			return nil, err
		}
		d.Source = "template_draft"
		d.CreatedAt = d.UpdatedAt
		out[strings.TrimSpace(d.Role)] = d
	}
	return out, rows.Err()
}

func (s *Server) upsertTemplatePromptDraft(ctx context.Context, role, prompt, source, notes string) error {
	if err := s.ensureTemplatePromptDraftsTable(ctx); err != nil {
		return err
	}
	role = strings.TrimSpace(role)
	prompt = strings.TrimSpace(prompt)
	_ = strings.TrimSpace(source)
	_ = strings.TrimSpace(notes)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO template_prompt_drafts (role, prompt, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (role) DO UPDATE SET
			prompt = EXCLUDED.prompt,
			updated_at = now()
	`, role, prompt)
	return err
}

func (s *Server) clearTemplatePromptDrafts(ctx context.Context) error {
	if err := s.ensureTemplatePromptDraftsTable(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM template_prompt_drafts`)
	return err
}

func (s *Server) loadLatestTemplateRecord(ctx context.Context) (runtimemanager.OrgTemplateRecord, error) {
	var rec runtimemanager.OrgTemplateRecord
	if err := s.db.QueryRowContext(ctx, `
		SELECT
			version,
			COALESCE(agents, '[]'::jsonb),
			COALESCE(bootstrap_routes, '[]'::jsonb),
			COALESCE(seeded_routes, '[]'::jsonb),
			COALESCE(created_by, ''),
			COALESCE(description, ''),
			COALESCE(created_at, now())
		FROM org_templates
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(
		&rec.Version,
		&rec.Agents,
		&rec.BootstrapRoutes,
		&rec.SeededRoutes,
		&rec.CreatedBy,
		&rec.Description,
		&rec.CreatedAt,
	); err != nil {
		return runtimemanager.OrgTemplateRecord{}, err
	}
	return rec, nil
}

func (s *Server) loadTemplatePromptForRole(ctx context.Context, role string) (prompt string, version string, err error) {
	rec, err := s.loadLatestTemplateRecord(ctx)
	if err != nil {
		return "", "", err
	}
	role = strings.TrimSpace(role)
	agents := make([]map[string]any, 0)
	if err := json.Unmarshal(rec.Agents, &agents); err != nil {
		return "", "", fmt.Errorf("parse template agents: %w", err)
	}
	for _, a := range agents {
		if strings.TrimSpace(asString(a["role"])) != role {
			continue
		}
		return strings.TrimSpace(asString(a["system_prompt"])), rec.Version, nil
	}
	return "", "", fmt.Errorf("template role not found: %s", role)
}

func (s *Server) applyPromptDrafts(ctx context.Context, agentsJSON []byte) ([]byte, []string, error) {
	drafts, err := s.loadAllTemplatePromptDrafts(ctx)
	if err != nil {
		return nil, nil, err
	}
	if len(drafts) == 0 {
		return agentsJSON, nil, nil
	}
	agents := make([]map[string]any, 0)
	if err := json.Unmarshal(agentsJSON, &agents); err != nil {
		return nil, nil, fmt.Errorf("parse template agents: %w", err)
	}
	applied := make([]string, 0, len(drafts))
	seen := make(map[string]struct{})
	for i := range agents {
		role := strings.TrimSpace(asString(agents[i]["role"]))
		if role == "" {
			continue
		}
		d, ok := drafts[role]
		if !ok {
			continue
		}
		agents[i]["system_prompt"] = strings.TrimSpace(d.Prompt)
		if _, dup := seen[role]; !dup {
			applied = append(applied, role)
			seen[role] = struct{}{}
		}
	}
	for role := range drafts {
		if _, ok := seen[role]; !ok {
			return nil, nil, fmt.Errorf("template draft role not found in latest template: %s", role)
		}
	}
	out, err := json.Marshal(agents)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal updated template agents: %w", err)
	}
	return out, applied, nil
}

func renderPromptDiff(templatePrompt, overridePrompt string) []string {
	base := strings.Split(strings.TrimSpace(templatePrompt), "\n")
	over := strings.Split(strings.TrimSpace(overridePrompt), "\n")
	if len(base) == 1 && base[0] == "" {
		base = nil
	}
	if len(over) == 1 && over[0] == "" {
		over = nil
	}
	if strings.TrimSpace(templatePrompt) == strings.TrimSpace(overridePrompt) {
		return []string{}
	}
	max := len(base)
	if len(over) > max {
		max = len(over)
	}
	out := make([]string, 0, max*2)
	for i := 0; i < max; i++ {
		left := ""
		right := ""
		if i < len(base) {
			left = base[i]
		}
		if i < len(over) {
			right = over[i]
		}
		if left == right {
			continue
		}
		if left != "" {
			out = append(out, "- "+left)
		}
		if right != "" {
			out = append(out, "+ "+right)
		}
	}
	return out
}

func (s *Server) handlePage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/dashboard" && r.URL.Path != "/dashboard/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("content-type", "text/html; charset=utf-8")
	_, _ = w.Write(dashboardPage)
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	resp := map[string]any{"generated_at": s.now().UTC()}

	var agentsTotal, agentsActive, events24h, pendingMailbox, verticalsTotal int
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM agents`).Scan(&agentsTotal)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM agents WHERE status <> 'terminated'`).Scan(&agentsActive)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE created_at >= now() - interval '24 hours'`).Scan(&events24h)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM mailbox WHERE status = 'pending'`).Scan(&pendingMailbox)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM verticals`).Scan(&verticalsTotal)

	resp["agents_total"] = agentsTotal
	resp["agents_active"] = agentsActive
	resp["events_24h"] = events24h
	resp["mailbox_pending"] = pendingMailbox
	resp["verticals_total"] = verticalsTotal
	writeJSON(w, http.StatusOK, resp)
}

type agentView struct {
	ID                  string     `json:"id"`
	Role                string     `json:"role"`
	Mode                string     `json:"mode"`
	Status              string     `json:"status"`
	VerticalID          string     `json:"vertical_id"`
	VerticalSlug        string     `json:"vertical_slug"`
	CurrentTaskID       string     `json:"current_task_id"`
	StartedAt           time.Time  `json:"started_at"`
	LastActiveAt        time.Time  `json:"last_active_at"`
	RuntimeMode         string     `json:"runtime_mode"`
	SessionID           string     `json:"session_id"`
	TurnCount           int        `json:"turn_count"`
	Turns24h            int        `json:"turns_24h"`
	TurnLimit           int        `json:"turn_limit"`
	NearBreaker         bool       `json:"near_breaker"`
	LockOwner           string     `json:"lock_owner"`
	LockExpiresAt       *time.Time `json:"lock_expires_at,omitempty"`
	LastUsedAt          *time.Time `json:"last_used_at,omitempty"`
	PendingEvents       int        `json:"pending_events"`
	OldestPendingAt     *time.Time `json:"oldest_pending_at,omitempty"`
	OldestPendingAgeSec int        `json:"oldest_pending_age_sec,omitempty"`
	InFlightTurn        bool       `json:"in_flight_turn"`
	InFlightSeconds     int        `json:"in_flight_seconds,omitempty"`
	DeadLetters24h      int        `json:"dead_letters_24h"`
	Failures24h         int        `json:"failures_24h"`
	InputTokens24h      int64      `json:"input_tokens_24h"`
	OutputTokens24h     int64      `json:"output_tokens_24h"`
	TotalTokens24h      int64      `json:"total_tokens_24h"`
	State               string     `json:"state"`
	StuckReason         string     `json:"stuck_reason,omitempty"`
	SystemPrompt        string     `json:"system_prompt,omitempty"`
	CreationEvent       eventRef   `json:"creation_event"`
	LastTool            toolView   `json:"last_tool"`
}

type eventRef struct {
	ID        string     `json:"id,omitempty"`
	Type      string     `json:"type,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
}

type toolView struct {
	Name           string     `json:"name,omitempty"`
	OK             *bool      `json:"ok,omitempty"`
	Error          string     `json:"error,omitempty"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorComponent string     `json:"error_component,omitempty"`
	ErrorOperation string     `json:"error_operation,omitempty"`
	ErrorRetryable *bool      `json:"error_retryable,omitempty"`
	Result         string     `json:"result,omitempty"`
	CreatedAt      *time.Time `json:"created_at,omitempty"`
}

func (s *Server) handleAgents(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	turnLimit := 40
	if s.cfg != nil && s.cfg.LLM.Session.RotateAfterTurns > 0 {
		turnLimit = s.cfg.LLM.Session.RotateAfterTurns
	}

	const q = `
		WITH latest_session AS (
			SELECT DISTINCT ON (agent_id)
				agent_id,
				runtime_mode,
				session_id,
				turn_count,
				COALESCE(lock_owner, '') AS lock_owner,
				lock_expires_at,
				last_used_at,
				COALESCE(checkpoint_summary, '') AS checkpoint_summary
			FROM agent_sessions
			WHERE status = 'active'
			ORDER BY agent_id, last_used_at DESC
		),
		pending AS (
			SELECT d.agent_id, count(*) AS pending_count, min(e.created_at) AS oldest_pending_at
			FROM event_deliveries d
			INNER JOIN events e ON e.id = d.event_id
			LEFT JOIN event_receipts r
				ON r.event_id = d.event_id
				AND r.agent_id = d.agent_id
			WHERE r.event_id IS NULL
				OR (r.status = 'error' AND r.retry_count < 3)
			GROUP BY d.agent_id
		),
		dead AS (
			SELECT agent_id, count(*) AS dead_count
			FROM event_receipts
			WHERE status = 'dead_letter'
			  AND processed_at >= now() - interval '24 hours'
			GROUP BY agent_id
		),
		recent_fail AS (
			SELECT agent_id, count(*) AS fail_count
			FROM agent_turns
			WHERE created_at >= now() - interval '24 hours'
			  AND (parse_ok = false OR COALESCE(error, '') <> '')
			GROUP BY agent_id
		),
		turn_agg AS (
			SELECT agent_id, count(*)::int AS turns_24h
			FROM agent_turns
			WHERE created_at >= now() - interval '24 hours'
			GROUP BY agent_id
		),
		token_turn_agg AS (
			SELECT agent_id,
				sum(COALESCE(NULLIF(regexp_replace(COALESCE(response_payload->'usage'->>'input_tokens', response_payload->'usage'->>'inputTokens', response_payload->>'input_tokens', '0'), '[^0-9]', '', 'g'), '')::bigint, 0)) AS input_tokens,
				sum(COALESCE(NULLIF(regexp_replace(COALESCE(response_payload->'usage'->>'output_tokens', response_payload->'usage'->>'outputTokens', response_payload->>'output_tokens', '0'), '[^0-9]', '', 'g'), '')::bigint, 0)) AS output_tokens
			FROM agent_turns
			WHERE created_at >= now() - interval '24 hours'
			GROUP BY agent_id
		),
		token_spend_agg AS (
			SELECT
				agent_id,
				sum(COALESCE(NULLIF(regexp_replace(COALESCE(metadata->>'input_tokens', '0'), '[^0-9]', '', 'g'), '')::bigint, 0)) AS input_tokens,
				sum(COALESCE(NULLIF(regexp_replace(COALESCE(metadata->>'output_tokens', '0'), '[^0-9]', '', 'g'), '')::bigint, 0)) AS output_tokens
			FROM spend_ledger
			WHERE created_at >= now() - interval '24 hours'
			  AND category = 'llm'
			  AND COALESCE(agent_id, '') <> ''
			GROUP BY agent_id
		)
			SELECT
				a.id,
				a.role,
				a.mode,
				a.status,
				COALESCE(a.vertical_id::text, ''),
				COALESCE(v.slug, ''),
				COALESCE(a.current_task_id::text, ''),
				a.started_at,
				a.last_active_at,
				COALESCE(ls.runtime_mode, ''),
				COALESCE(ls.session_id, ''),
			COALESCE(ls.turn_count, 0),
			COALESCE(ta.turns_24h, 0),
			COALESCE(ls.lock_owner, ''),
			ls.lock_expires_at,
			ls.last_used_at,
			COALESCE(p.pending_count, 0),
			p.oldest_pending_at,
			COALESCE(d.dead_count, 0),
			COALESCE(f.fail_count, 0),
			COALESCE(ts.input_tokens, tt.input_tokens, 0),
			COALESCE(ts.output_tokens, tt.output_tokens, 0),
				COALESCE((SELECT payload->>'tool_name' FROM events e2 WHERE e2.type = 'agent.tool_execution' AND e2.source_agent = a.id ORDER BY e2.created_at DESC LIMIT 1), ''),
				COALESCE((SELECT payload->>'ok' FROM events e2 WHERE e2.type = 'agent.tool_execution' AND e2.source_agent = a.id ORDER BY e2.created_at DESC LIMIT 1), ''),
				COALESCE((SELECT payload->>'error' FROM events e2 WHERE e2.type = 'agent.tool_execution' AND e2.source_agent = a.id ORDER BY e2.created_at DESC LIMIT 1), ''),
				COALESCE((SELECT payload->>'result' FROM events e2 WHERE e2.type = 'agent.tool_execution' AND e2.source_agent = a.id ORDER BY e2.created_at DESC LIMIT 1), ''),
				COALESCE((SELECT e2.id::text FROM events e2 WHERE e2.source_agent = a.id ORDER BY e2.created_at ASC LIMIT 1), ''),
				COALESCE((SELECT e2.type FROM events e2 WHERE e2.source_agent = a.id ORDER BY e2.created_at ASC LIMIT 1), ''),
				(SELECT e2.created_at FROM events e2 WHERE e2.source_agent = a.id ORDER BY e2.created_at ASC LIMIT 1),
				(SELECT e2.created_at FROM events e2 WHERE e2.type = 'agent.tool_execution' AND e2.source_agent = a.id ORDER BY e2.created_at DESC LIMIT 1),
				COALESCE(a.config, '{}'::jsonb)
			FROM agents a
		LEFT JOIN verticals v ON v.id = a.vertical_id
		LEFT JOIN latest_session ls ON ls.agent_id = a.id
		LEFT JOIN pending p ON p.agent_id = a.id
		LEFT JOIN dead d ON d.agent_id = a.id
		LEFT JOIN recent_fail f ON f.agent_id = a.id
		LEFT JOIN turn_agg ta ON ta.agent_id = a.id
		LEFT JOIN token_turn_agg tt ON tt.agent_id = a.id
		LEFT JOIN token_spend_agg ts ON ts.agent_id = a.id
		ORDER BY a.last_active_at DESC, a.id ASC
	`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(strings.ToLower(err.Error()), "runtime manager unavailable") {
			status = http.StatusServiceUnavailable
		}
		writeErr(w, status, err)
		return
	}
	defer rows.Close()

	agents := make([]agentView, 0, 64)
	stateCounts := map[string]int{"running": 0, "idle": 0, "stuck": 0, "terminated": 0}
	now := s.now()

	for rows.Next() {
		var av agentView
		av.TurnLimit = turnLimit
		var lockExp sql.NullTime
		var lastUsed sql.NullTime
		var oldestPending sql.NullTime
		var toolOK string
		var creationAt sql.NullTime
		var toolAt sql.NullTime
		var configRaw []byte
		if err := rows.Scan(
			&av.ID,
			&av.Role,
			&av.Mode,
			&av.Status,
			&av.VerticalID,
			&av.VerticalSlug,
			&av.CurrentTaskID,
			&av.StartedAt,
			&av.LastActiveAt,
			&av.RuntimeMode,
			&av.SessionID,
			&av.TurnCount,
			&av.Turns24h,
			&av.LockOwner,
			&lockExp,
			&lastUsed,
			&av.PendingEvents,
			&oldestPending,
			&av.DeadLetters24h,
			&av.Failures24h,
			&av.InputTokens24h,
			&av.OutputTokens24h,
			&av.LastTool.Name,
			&toolOK,
			&av.LastTool.Error,
			&av.LastTool.Result,
			&av.CreationEvent.ID,
			&av.CreationEvent.Type,
			&creationAt,
			&toolAt,
			&configRaw,
		); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if lockExp.Valid {
			av.LockExpiresAt = &lockExp.Time
		}
		if lastUsed.Valid {
			av.LastUsedAt = &lastUsed.Time
		}
		if oldestPending.Valid {
			av.OldestPendingAt = &oldestPending.Time
			if age := int(now.Sub(oldestPending.Time).Seconds()); age > 0 {
				av.OldestPendingAgeSec = age
			}
		}
		if toolAt.Valid {
			av.LastTool.CreatedAt = &toolAt.Time
		}
		if creationAt.Valid {
			av.CreationEvent.CreatedAt = &creationAt.Time
		}
		if av.CreationEvent.ID == "" {
			av.CreationEvent.Type = "agent.started"
			started := av.StartedAt
			av.CreationEvent.CreatedAt = &started
		}
		if toolOK != "" {
			ok := strings.EqualFold(toolOK, "true")
			av.LastTool.OK = &ok
		}
		toolErrMeta := parseRuntimeErrorMetadata(av.LastTool.Error)
		av.LastTool.ErrorCode = toolErrMeta.Code
		av.LastTool.ErrorComponent = toolErrMeta.Component
		av.LastTool.ErrorOperation = toolErrMeta.Operation
		av.LastTool.ErrorRetryable = toolErrMeta.Retryable
		av.LastTool.Result = truncate(av.LastTool.Result, 300)
		av.SystemPrompt, _, _, _ = parseAgentRuntimeConfig(configRaw)
		av.TotalTokens24h = av.InputTokens24h + av.OutputTokens24h

		if turnLimit > 0 {
			av.NearBreaker = float64(av.TurnCount)/float64(turnLimit) >= 0.9
		}
		if av.LockExpiresAt != nil && strings.TrimSpace(av.LockOwner) != "" && av.LockExpiresAt.After(now) {
			av.InFlightTurn = true
			switch {
			case av.OldestPendingAgeSec > 0:
				av.InFlightSeconds = av.OldestPendingAgeSec
			case av.LastUsedAt != nil:
				if sec := int(now.Sub(*av.LastUsedAt).Seconds()); sec > 0 {
					av.InFlightSeconds = sec
				}
			}
		}
		av.State, av.StuckReason = classifyAgent(av, now)
		stateCounts[av.State]++
		agents = append(agents, av)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"turn_limit":   turnLimit,
		"states":       stateCounts,
		"agents":       agents,
	})
}

func classifyAgent(a agentView, now time.Time) (string, string) {
	lastSignalAt := latestAgentSignalAt(a)
	if strings.EqualFold(a.Status, "terminated") {
		return "terminated", ""
	}
	if a.DeadLetters24h > 0 {
		return "stuck", "dead-letter receipts in last 24h"
	}
	if a.Failures24h >= 3 {
		return "stuck", "3+ failed turns in last 24h"
	}
	if a.PendingEvents > 0 && now.Sub(lastSignalAt) > 10*time.Minute {
		return "stuck", "pending deliveries while inactive for >10m"
	}
	if a.NearBreaker && a.Failures24h > 0 {
		return "stuck", "near turn circuit breaker with failures"
	}
	if a.LockOwner != "" || a.PendingEvents > 0 || now.Sub(lastSignalAt) < 2*time.Minute {
		return "running", ""
	}
	return "idle", ""
}

func latestAgentSignalAt(a agentView) time.Time {
	latest := a.LastActiveAt
	if a.LastUsedAt != nil && a.LastUsedAt.After(latest) {
		latest = *a.LastUsedAt
	}
	if a.LastTool.CreatedAt != nil && a.LastTool.CreatedAt.After(latest) {
		latest = *a.LastTool.CreatedAt
	}
	return latest
}

type eventView struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"`
	SourceAgent    string    `json:"source_agent"`
	TaskID         string    `json:"task_id"`
	VerticalID     string    `json:"vertical_id"`
	VerticalSlug   string    `json:"vertical_slug"`
	CreatedAt      time.Time `json:"created_at"`
	DeliveryCount  int       `json:"delivery_count"`
	ProcessedCount int       `json:"processed_count"`
	ErrorCount     int       `json:"error_count"`
	DeadCount      int       `json:"dead_count"`
	PendingCount   int       `json:"pending_count"`
	AvgProcessMS   int64     `json:"avg_processing_ms"`
}

type runtimeLogView struct {
	ID             int64     `json:"id"`
	TS             time.Time `json:"ts"`
	Level          string    `json:"level"`
	Component      string    `json:"component"`
	Action         string    `json:"action"`
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	AgentID        string    `json:"agent_id"`
	VerticalID     string    `json:"vertical_id"`
	CampaignID     string    `json:"campaign_id"`
	ScanID         string    `json:"scan_id"`
	SessionID      string    `json:"session_id"`
	Detail         any       `json:"detail"`
	Error          string    `json:"error"`
	ErrorCode      string    `json:"error_code,omitempty"`
	ErrorComponent string    `json:"error_component,omitempty"`
	ErrorOperation string    `json:"error_operation,omitempty"`
	ErrorRetryable *bool     `json:"error_retryable,omitempty"`
	DurationUS     int       `json:"duration_us"`
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 200), 1, 1000)
	filter := eventFilter{
		EventType:  strings.TrimSpace(r.URL.Query().Get("type")),
		Source:     strings.TrimSpace(r.URL.Query().Get("source")),
		Vertical:   strings.TrimSpace(r.URL.Query().Get("vertical")),
		Subscriber: strings.TrimSpace(r.URL.Query().Get("subscriber")),
	}

	items, err := s.queryEvents(ctx, filter, time.Time{}, limit, false)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"events":       items,
	})
}

func (s *Server) handleEventDetail(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	prefix := "/dashboard/api/events/"
	if strings.HasPrefix(r.URL.Path, "/api/events/") {
		prefix = "/api/events/"
	}
	id := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, prefix))
	if id == "" || id == "stream" {
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()

	var evt eventView
	var payloadRaw []byte
	if err := s.db.QueryRowContext(ctx, `
		SELECT
			e.id::text,
			e.type,
			e.source_agent,
			COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			COALESCE(e.created_at, now()),
			COALESCE(e.payload, '{}'::jsonb)
		FROM events e
		LEFT JOIN verticals v ON v.id = e.vertical_id
		WHERE e.id::text = $1
	`, id).Scan(
		&evt.ID,
		&evt.Type,
		&evt.SourceAgent,
		&evt.TaskID,
		&evt.VerticalID,
		&evt.VerticalSlug,
		&evt.CreatedAt,
		&payloadRaw,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	type deliveryView struct {
		AgentID        string     `json:"agent_id"`
		AgentRole      string     `json:"agent_role"`
		CreatedAt      time.Time  `json:"delivery_created_at"`
		Status         string     `json:"status"`
		RetryCount     int        `json:"retry_count"`
		Error          string     `json:"error,omitempty"`
		ErrorCode      string     `json:"error_code,omitempty"`
		ErrorComponent string     `json:"error_component,omitempty"`
		ErrorOperation string     `json:"error_operation,omitempty"`
		ErrorRetryable *bool      `json:"error_retryable,omitempty"`
		ProcessedAt    *time.Time `json:"processed_at,omitempty"`
		ProcessingMS   int64      `json:"processing_ms"`
	}

	deliveries := make([]deliveryView, 0, 16)
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			d.agent_id,
			COALESCE(a.role, ''),
			COALESCE(d.created_at, now()),
			COALESCE(r.status, 'pending'),
			COALESCE(r.retry_count, 0),
			COALESCE(r.error, ''),
			r.processed_at,
			COALESCE((extract(epoch from (r.processed_at - e.created_at)) * 1000)::bigint, 0)
		FROM event_deliveries d
		LEFT JOIN agents a ON a.id = d.agent_id
		LEFT JOIN event_receipts r ON r.event_id = d.event_id AND r.agent_id = d.agent_id
		JOIN events e ON e.id = d.event_id
		WHERE d.event_id::text = $1
		ORDER BY d.created_at ASC, d.agent_id ASC
	`, id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var d deliveryView
		var processed sql.NullTime
		if err := rows.Scan(&d.AgentID, &d.AgentRole, &d.CreatedAt, &d.Status, &d.RetryCount, &d.Error, &processed, &d.ProcessingMS); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if processed.Valid {
			d.ProcessedAt = &processed.Time
		}
		errMeta := parseRuntimeErrorMetadata(d.Error)
		d.ErrorCode = errMeta.Code
		d.ErrorComponent = errMeta.Component
		d.ErrorOperation = errMeta.Operation
		d.ErrorRetryable = errMeta.Retryable
		deliveries = append(deliveries, d)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	var payload any
	_ = json.Unmarshal(payloadRaw, &payload)
	writeJSON(w, http.StatusOK, map[string]any{
		"event":      evt,
		"payload":    payload,
		"deliveries": deliveries,
	})
}

func (s *Server) handleEventStream(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
		return
	}
	w.Header().Set("content-type", "text/event-stream")
	w.Header().Set("cache-control", "no-cache")
	w.Header().Set("connection", "keep-alive")

	filter := eventFilter{
		EventType:  strings.TrimSpace(r.URL.Query().Get("type")),
		Source:     strings.TrimSpace(r.URL.Query().Get("source")),
		Vertical:   strings.TrimSpace(r.URL.Query().Get("vertical")),
		Subscriber: strings.TrimSpace(r.URL.Query().Get("subscriber")),
		Component:  strings.TrimSpace(r.URL.Query().Get("component")),
		Level:      strings.TrimSpace(r.URL.Query().Get("level")),
	}
	includeRuntime := parseBoolQuery(r.URL.Query().Get("include_runtime"), true)
	since := s.now().Add(-30 * time.Second)
	if raw := strings.TrimSpace(r.URL.Query().Get("since")); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			since = t
		}
	}

	ctx := r.Context()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		items, err := s.queryEvents(ctx, filter, since, 200, true)
		if err == nil {
			for _, item := range items {
				raw, _ := json.Marshal(item)
				_, _ = fmt.Fprintf(w, "event: event\ndata: %s\n\n", raw)
				if item.CreatedAt.After(since) {
					since = item.CreatedAt
				}
			}
		}
		if includeRuntime {
			logItems, logErr := s.queryRuntimeLogs(ctx, filter, since, 200, true)
			if logErr == nil {
				for _, item := range logItems {
					raw, _ := json.Marshal(item)
					_, _ = fmt.Fprintf(w, "event: runtime_log\ndata: %s\n\n", raw)
					if item.TS.After(since) {
						since = item.TS
					}
				}
			}
		}
		flusher.Flush()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) handleRuntimeLogs(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	filter := eventFilter{
		EventType: strings.TrimSpace(r.URL.Query().Get("type")),
		Source:    strings.TrimSpace(r.URL.Query().Get("source")),
		Vertical:  strings.TrimSpace(r.URL.Query().Get("vertical")),
		Component: strings.TrimSpace(r.URL.Query().Get("component")),
		Level:     strings.TrimSpace(r.URL.Query().Get("level")),
		ErrorCode: strings.TrimSpace(r.URL.Query().Get("error_code")),
	}
	since := time.Time{}
	if raw := strings.TrimSpace(r.URL.Query().Get("since")); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			since = t
		}
	}
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 100), 1, 500)
	asc := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("order")), "asc")
	items, err := s.queryRuntimeLogs(r.Context(), filter, since, limit, asc)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"count":        len(items),
		"runtime_logs": items,
	})
}

func (s *Server) handleRuntimeIncidents(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	hours := clamp(parseInt(r.URL.Query().Get("since_hours"), 24), 1, 24*14)
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 1200), 100, 5000)
	mcpOnly := parseBoolQuery(r.URL.Query().Get("mcp_only"), true)
	level := strings.TrimSpace(r.URL.Query().Get("level"))
	if level == "" {
		level = "warn"
	}
	filter := eventFilter{
		Component: strings.TrimSpace(r.URL.Query().Get("component")),
		Level:     level,
	}
	since := s.now().UTC().Add(-time.Duration(hours) * time.Hour)
	logs, err := s.queryRuntimeLogs(r.Context(), filter, since, limit, false)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	type incidentAgg struct {
		Code       string
		Count      int
		FirstSeen  time.Time
		LastSeen   time.Time
		Agents     map[string]struct{}
		Components map[string]struct{}
		Actions    map[string]struct{}
		LastError  string
	}
	agg := make(map[string]*incidentAgg)
	for _, item := range logs {
		code := strings.TrimSpace(item.ErrorCode)
		if code == "" {
			continue
		}
		if mcpOnly && !strings.HasPrefix(code, "mcp_") {
			continue
		}
		entry, ok := agg[code]
		if !ok {
			entry = &incidentAgg{
				Code:       code,
				Count:      0,
				FirstSeen:  item.TS,
				LastSeen:   item.TS,
				Agents:     map[string]struct{}{},
				Components: map[string]struct{}{},
				Actions:    map[string]struct{}{},
				LastError:  item.Error,
			}
			agg[code] = entry
		}
		entry.Count++
		if item.TS.Before(entry.FirstSeen) {
			entry.FirstSeen = item.TS
		}
		if item.TS.After(entry.LastSeen) {
			entry.LastSeen = item.TS
			entry.LastError = item.Error
		}
		if v := strings.TrimSpace(item.AgentID); v != "" {
			entry.Agents[v] = struct{}{}
		}
		if v := strings.TrimSpace(item.Component); v != "" {
			entry.Components[v] = struct{}{}
		}
		if v := strings.TrimSpace(item.Action); v != "" {
			entry.Actions[v] = struct{}{}
		}
	}

	incidents := make([]map[string]any, 0, len(agg))
	for _, v := range agg {
		incidents = append(incidents, map[string]any{
			"code":        v.Code,
			"count":       v.Count,
			"first_seen":  v.FirstSeen.UTC(),
			"last_seen":   v.LastSeen.UTC(),
			"agents":      mapKeys(v.Agents),
			"components":  mapKeys(v.Components),
			"actions":     mapKeys(v.Actions),
			"last_error":  truncate(v.LastError, 500),
			"root_cause":  classifyIncidentRootCause(v.Code),
			"is_mcp_code": strings.HasPrefix(v.Code, "mcp_"),
		})
	}
	sort.SliceStable(incidents, func(i, j int) bool {
		ci := asInt(incidents[i]["count"])
		cj := asInt(incidents[j]["count"])
		if ci == cj {
			ti, _ := incidents[i]["last_seen"].(time.Time)
			tj, _ := incidents[j]["last_seen"].(time.Time)
			return tj.Before(ti)
		}
		return ci > cj
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"since_hours":  hours,
		"mcp_only":     mcpOnly,
		"count":        len(incidents),
		"incidents":    incidents,
	})
}

type eventFilter struct {
	EventType  string
	Source     string
	Vertical   string
	Subscriber string
	Component  string
	Level      string
	ErrorCode  string
}

func (s *Server) queryEvents(ctx context.Context, filter eventFilter, since time.Time, limit int, asc bool) ([]eventView, error) {
	where := []string{"1=1"}
	args := make([]any, 0, 8)
	add := func(clause string, value any) {
		args = append(args, value)
		clause = strings.ReplaceAll(clause, "?", "$"+strconv.Itoa(len(args)))
		where = append(where, clause)
	}
	add2 := func(clause string, v1, v2 any) {
		args = append(args, v1, v2)
		first := "$" + strconv.Itoa(len(args)-1)
		second := "$" + strconv.Itoa(len(args))
		clause = strings.Replace(clause, "?", first, 1)
		clause = strings.Replace(clause, "?", second, 1)
		where = append(where, clause)
	}

	if !since.IsZero() {
		add("e.created_at > ?", since)
	}
	if filter.EventType != "" {
		if strings.HasSuffix(filter.EventType, "*") {
			add("e.type LIKE ?", strings.TrimSuffix(filter.EventType, "*")+"%")
		} else {
			add("e.type = ?", filter.EventType)
		}
	}
	if filter.Source != "" {
		add("e.source_agent = ?", filter.Source)
	}
	if filter.Vertical != "" {
		add2("(COALESCE(e.vertical_id::text, '') = ? OR COALESCE(v.slug, '') = ?)", filter.Vertical, filter.Vertical)
	}
	if filter.Subscriber != "" {
		add("EXISTS (SELECT 1 FROM event_deliveries d2 WHERE d2.event_id = e.id AND d2.agent_id = ?)", filter.Subscriber)
	}
	args = append(args, limit)
	order := "DESC"
	if asc {
		order = "ASC"
	}
	q := fmt.Sprintf(`
		SELECT
			e.id::text,
			e.type,
			e.source_agent,
			COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			COALESCE(e.created_at, now()),
			count(d.agent_id) AS delivery_count,
			count(r.agent_id) FILTER (WHERE r.status = 'processed') AS processed_count,
			count(r.agent_id) FILTER (WHERE r.status = 'error') AS error_count,
			count(r.agent_id) FILTER (WHERE r.status = 'dead_letter') AS dead_count,
			(count(d.agent_id) - count(r.agent_id)) AS pending_count,
			COALESCE((avg(extract(epoch from (r.processed_at - e.created_at)) * 1000) FILTER (WHERE r.processed_at IS NOT NULL))::bigint, 0) AS avg_ms
		FROM events e
		LEFT JOIN verticals v ON v.id = e.vertical_id
		LEFT JOIN event_deliveries d ON d.event_id = e.id
		LEFT JOIN event_receipts r ON r.event_id = d.event_id AND r.agent_id = d.agent_id
		WHERE %s
		GROUP BY e.id, v.slug
		ORDER BY e.created_at %s
		LIMIT $%d
	`, strings.Join(where, " AND "), order, len(args))

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]eventView, 0, limit)
	for rows.Next() {
		var ev eventView
		if err := rows.Scan(
			&ev.ID,
			&ev.Type,
			&ev.SourceAgent,
			&ev.TaskID,
			&ev.VerticalID,
			&ev.VerticalSlug,
			&ev.CreatedAt,
			&ev.DeliveryCount,
			&ev.ProcessedCount,
			&ev.ErrorCount,
			&ev.DeadCount,
			&ev.PendingCount,
			&ev.AvgProcessMS,
		); err != nil {
			return nil, err
		}
		items = append(items, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Server) queryFlowEvents(ctx context.Context, since, until time.Time, vertical string, limit int, asc bool) ([]flowEventView, error) {
	where := []string{"1=1"}
	args := make([]any, 0, 8)
	add := func(clause string, value any) {
		args = append(args, value)
		clause = strings.ReplaceAll(clause, "?", "$"+strconv.Itoa(len(args)))
		where = append(where, clause)
	}
	add2 := func(clause string, v1, v2 any) {
		args = append(args, v1, v2)
		first := "$" + strconv.Itoa(len(args)-1)
		second := "$" + strconv.Itoa(len(args))
		clause = strings.Replace(clause, "?", first, 1)
		clause = strings.Replace(clause, "?", second, 1)
		where = append(where, clause)
	}

	if !since.IsZero() {
		add("e.created_at > ?", since)
	}
	if !until.IsZero() {
		add("e.created_at <= ?", until)
	}
	if v := strings.TrimSpace(vertical); v != "" {
		add2("(COALESCE(e.vertical_id::text, '') = ? OR COALESCE(v.slug, '') = ?)", v, v)
	}

	args = append(args, limit)
	order := "DESC"
	if asc {
		order = "ASC"
	}
	q := fmt.Sprintf(`
		SELECT
			e.id::text,
			e.type,
			COALESCE(e.source_agent, ''),
			COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''),
			COALESCE(e.created_at, now()),
			COALESCE(e.payload, '{}'::jsonb),
			COALESCE((
				SELECT jsonb_agg(d.agent_id ORDER BY d.agent_id)
				FROM event_deliveries d
				WHERE d.event_id = e.id
			), '[]'::jsonb)
		FROM events e
		LEFT JOIN verticals v ON v.id = e.vertical_id
		WHERE %s
		ORDER BY e.created_at %s
		LIMIT $%d
	`, strings.Join(where, " AND "), order, len(args))

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]flowEventView, 0, limit)
	for rows.Next() {
		var (
			id, eventType, sourceAgent, taskID, verticalID string
			createdAt                                      time.Time
			payloadRaw                                     []byte
			targetsRaw                                     []byte
		)
		if err := rows.Scan(
			&id,
			&eventType,
			&sourceAgent,
			&taskID,
			&verticalID,
			&createdAt,
			&payloadRaw,
			&targetsRaw,
		); err != nil {
			return nil, err
		}
		targets := make([]string, 0, 8)
		if len(targetsRaw) > 0 && json.Valid(targetsRaw) {
			var rawTargets []any
			if err := json.Unmarshal(targetsRaw, &rawTargets); err == nil {
				for _, item := range rawTargets {
					v := strings.TrimSpace(asString(item))
					if v != "" {
						targets = append(targets, v)
					}
				}
			}
		}

		intercepted, passthrough := flowInterceptPolicy(eventType, payloadRaw)
		if intercepted && len(targets) == 0 {
			targets = append(targets, "pipeline-coordinator")
		}
		sourceNode := strings.TrimSpace(sourceAgent)
		if sourceNode == "" {
			sourceNode = "runtime"
		}
		items = append(items, flowEventView{
			EventID:     id,
			EventType:   eventType,
			SourceNode:  sourceNode,
			TargetNodes: targets,
			Intercepted: intercepted,
			Passthrough: passthrough,
			Timestamp:   createdAt.UTC(),
			VerticalID:  strings.TrimSpace(verticalID),
			TaskID:      strings.TrimSpace(taskID),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Server) queryRuntimeLogs(ctx context.Context, filter eventFilter, since time.Time, limit int, asc bool) ([]runtimeLogView, error) {
	where := []string{"1=1"}
	args := make([]any, 0, 8)
	add := func(clause string, value any) {
		args = append(args, value)
		clause = strings.ReplaceAll(clause, "?", "$"+strconv.Itoa(len(args)))
		where = append(where, clause)
	}
	add2 := func(clause string, v1, v2 any) {
		args = append(args, v1, v2)
		first := "$" + strconv.Itoa(len(args)-1)
		second := "$" + strconv.Itoa(len(args))
		clause = strings.Replace(clause, "?", first, 1)
		clause = strings.Replace(clause, "?", second, 1)
		where = append(where, clause)
	}

	if !since.IsZero() {
		add("rl.ts > ?", since)
	}
	if filter.EventType != "" {
		if strings.HasSuffix(filter.EventType, "*") {
			add("COALESCE(rl.event_type, '') LIKE ?", strings.TrimSuffix(filter.EventType, "*")+"%")
		} else {
			add("COALESCE(rl.event_type, '') = ?", filter.EventType)
		}
	}
	if filter.Source != "" {
		add("COALESCE(rl.agent_id, '') = ?", filter.Source)
	}
	if filter.Vertical != "" {
		add2("(COALESCE(rl.vertical_id::text, '') = ? OR COALESCE(v.slug, '') = ?)", filter.Vertical, filter.Vertical)
	}
	if filter.Component != "" {
		add("COALESCE(rl.component, '') = ?", filter.Component)
	}
	if filter.Level != "" {
		add("COALESCE(rl.level, '') = ?", strings.ToLower(filter.Level))
	}
	if filter.ErrorCode != "" {
		add("COALESCE(rl.error, '') LIKE ?", "%code="+strings.TrimSpace(filter.ErrorCode)+"%")
	}
	args = append(args, limit)
	order := "DESC"
	if asc {
		order = "ASC"
	}
	q := fmt.Sprintf(`
		SELECT
			rl.id,
			rl.ts,
			COALESCE(rl.level, ''),
			COALESCE(rl.component, ''),
			COALESCE(rl.action, ''),
			COALESCE(rl.event_id::text, ''),
			COALESCE(rl.event_type, ''),
			COALESCE(rl.agent_id, ''),
			COALESCE(rl.vertical_id::text, ''),
			COALESCE(rl.campaign_id::text, ''),
			COALESCE(rl.scan_id::text, ''),
			COALESCE(rl.session_id::text, ''),
			COALESCE(rl.detail, '{}'::jsonb),
			COALESCE(rl.error, ''),
			COALESCE(rl.duration_us, 0)
		FROM runtime_log rl
		LEFT JOIN verticals v ON v.id = rl.vertical_id
		WHERE %s
		ORDER BY rl.ts %s
		LIMIT $%d
	`, strings.Join(where, " AND "), order, len(args))

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		if isMissingRuntimeLogTable(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	items := make([]runtimeLogView, 0, limit)
	for rows.Next() {
		var item runtimeLogView
		var detailRaw []byte
		if err := rows.Scan(
			&item.ID,
			&item.TS,
			&item.Level,
			&item.Component,
			&item.Action,
			&item.EventID,
			&item.EventType,
			&item.AgentID,
			&item.VerticalID,
			&item.CampaignID,
			&item.ScanID,
			&item.SessionID,
			&detailRaw,
			&item.Error,
			&item.DurationUS,
		); err != nil {
			return nil, err
		}
		var detail any
		_ = json.Unmarshal(detailRaw, &detail)
		item.Detail = detail
		errMeta := parseRuntimeErrorMetadata(item.Error)
		item.ErrorCode = errMeta.Code
		item.ErrorComponent = errMeta.Component
		item.ErrorOperation = errMeta.Operation
		item.ErrorRetryable = errMeta.Retryable
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Server) handleConversations(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 50), 1, 200)

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			c.agent_id,
			COALESCE(a.role, ''),
			COALESCE(a.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			COALESCE(c.mode, 'task'),
			COALESCE(c.turn_count, 0),
			COALESCE(c.summary, ''),
			COALESCE(c.updated_at, c.created_at)
		FROM conversations c
		LEFT JOIN agents a ON a.id = c.agent_id
		LEFT JOIN verticals v ON v.id = a.vertical_id
		WHERE c.status = 'active'
		ORDER BY c.updated_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var agentID, role, verticalID, verticalSlug, mode, summary string
		var turnCount int
		var updatedAt time.Time
		if err := rows.Scan(&agentID, &role, &verticalID, &verticalSlug, &mode, &turnCount, &summary, &updatedAt); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		items = append(items, map[string]any{
			"agent_id":      agentID,
			"role":          role,
			"vertical_id":   verticalID,
			"vertical_slug": verticalSlug,
			"mode":          mode,
			"turn_count":    turnCount,
			"summary":       summary,
			"updated_at":    updatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"conversations": items, "generated_at": s.now().UTC()})
}

func (s *Server) handleConversationDetail(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	agentID, subresource, ok := parseConversationPath(r.URL.Path)
	if !ok || agentID == "" {
		http.NotFound(w, r)
		return
	}
	if subresource == "artifacts" {
		s.handleConversationArtifacts(w, r, agentID)
		return
	}
	if subresource != "" {
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()

	var mode, taskID, summary, status string
	var turnCount int
	var messagesRaw []byte
	var createdAt, updatedAt time.Time
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(mode, 'task'), COALESCE(task_id::text, ''), COALESCE(summary, ''), COALESCE(status, ''),
			COALESCE(turn_count, 0), COALESCE(messages, '[]'::jsonb), COALESCE(created_at, now()), COALESCE(updated_at, now())
		FROM conversations
		WHERE agent_id = $1
		ORDER BY updated_at DESC
		LIMIT 1
	`, agentID).Scan(&mode, &taskID, &summary, &status, &turnCount, &messagesRaw, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	var messages any
	_ = json.Unmarshal(messagesRaw, &messages)

	turnRows, err := s.db.QueryContext(ctx, `
		SELECT
			turn_index,
			created_at,
			COALESCE(latency_ms, 0),
			COALESCE(retry_count, 0),
			parse_ok,
			COALESCE(error, ''),
			COALESCE(request_payload, '{}'::jsonb),
			COALESCE(response_payload, '{}'::jsonb)
		FROM agent_turns
		WHERE agent_id = $1
		ORDER BY created_at DESC
		LIMIT 80
	`, agentID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer turnRows.Close()

	turns := make([]map[string]any, 0, 80)
	for turnRows.Next() {
		var idx, latency, retries int
		var created time.Time
		var parseOK bool
		var errText string
		var reqRaw, respRaw []byte
		if err := turnRows.Scan(&idx, &created, &latency, &retries, &parseOK, &errText, &reqRaw, &respRaw); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		assistantText, toolCalls := extractTurnArtifacts(respRaw)
		toolResult := extractToolResult(reqRaw)
		turns = append(turns, map[string]any{
			"turn_index":       idx,
			"created_at":       created,
			"latency_ms":       latency,
			"retry_count":      retries,
			"parse_ok":         parseOK,
			"error":            errText,
			"assistant_text":   assistantText,
			"tool_calls":       toolCalls,
			"tool_result":      truncate(toolResult, 400),
			"response_payload": json.RawMessage(respRaw),
		})
	}
	if err := turnRows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id":   agentID,
		"mode":       mode,
		"task_id":    taskID,
		"summary":    summary,
		"status":     status,
		"turn_count": turnCount,
		"created_at": createdAt,
		"updated_at": updatedAt,
		"messages":   messages,
		"turns":      turns,
	})
}

func parseConversationPath(path string) (agentID, subresource string, ok bool) {
	prefix := "/dashboard/api/conversations/"
	if strings.HasPrefix(path, "/api/conversations/") {
		prefix = "/api/conversations/"
	}
	trimmed := strings.Trim(strings.TrimPrefix(path, prefix), "/")
	if trimmed == "" {
		return "", "", false
	}
	parts := strings.Split(trimmed, "/")
	agentID = strings.TrimSpace(parts[0])
	if agentID == "" {
		return "", "", false
	}
	if len(parts) > 1 {
		subresource = strings.TrimSpace(parts[1])
	}
	return agentID, subresource, true
}

type sessionArtifactFile struct {
	Path  string `json:"path,omitempty"`
	Found bool   `json:"found"`
	Tail  string `json:"tail,omitempty"`
	Error string `json:"error,omitempty"`
}

func (s *Server) handleConversationArtifacts(w http.ResponseWriter, r *http.Request, agentID string) {
	ctx := r.Context()
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		http.NotFound(w, r)
		return
	}

	lines := clamp(parseInt(r.URL.Query().Get("lines"), 80), 10, 300)
	sessionID, runtimeMode, provider, sessionStatus, err := s.lookupLatestAgentSession(ctx, agentID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	role, verticalID, err := s.lookupAgentRoleAndVertical(ctx, agentID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	container, slug, err := s.resolveWorkspaceContainer(ctx, role, verticalID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if container == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("no workspace container mapping for agent role=%s", role))
		return
	}

	out := map[string]any{
		"agent_id":    agentID,
		"role":        role,
		"vertical_id": verticalID,
		"vertical_slug": func() string {
			return slug
		}(),
		"session": map[string]any{
			"session_id":   sessionID,
			"runtime_mode": runtimeMode,
			"provider":     provider,
			"status":       sessionStatus,
		},
		"workspace_container": container,
		"generated_at":        s.now().UTC(),
	}

	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(runtimeMode)), "cli") {
		out["artifacts"] = map[string]any{}
		out["note"] = "session artifacts are only available for cli runtime modes"
		writeJSON(w, http.StatusOK, out)
		return
	}

	projectPath, err := s.findSessionProjectFile(ctx, container, sessionID)
	projectArtifact := sessionArtifactFile{Path: projectPath}
	if err != nil {
		projectArtifact.Error = err.Error()
	} else if strings.TrimSpace(projectPath) != "" {
		projectArtifact.Found = true
		tail, tailErr := s.tailContainerFile(ctx, container, projectPath, lines)
		if tailErr != nil {
			projectArtifact.Error = tailErr.Error()
		} else {
			projectArtifact.Tail = tail
		}
	}

	debugPath := "/home/agent/.claude/debug/" + strings.TrimSpace(sessionID) + ".txt"
	debugArtifact := sessionArtifactFile{Path: debugPath}
	if tail, tailErr := s.tailContainerFile(ctx, container, debugPath, lines); tailErr != nil {
		debugArtifact.Error = tailErr.Error()
	} else {
		debugArtifact.Found = true
		debugArtifact.Tail = tail
	}

	out["artifacts"] = map[string]any{
		"project_jsonl": projectArtifact,
		"debug_log":     debugArtifact,
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) lookupLatestAgentSession(ctx context.Context, agentID string) (sessionID, runtimeMode, provider, status string, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(session_id, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(provider, ''),
			COALESCE(status, '')
		FROM agent_sessions
		WHERE agent_id = $1
		ORDER BY (status = 'active') DESC, last_used_at DESC NULLS LAST, created_at DESC
		LIMIT 1
	`, strings.TrimSpace(agentID)).Scan(&sessionID, &runtimeMode, &provider, &status)
	if err != nil {
		return "", "", "", "", err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "", "", "", "", sql.ErrNoRows
	}
	return sessionID, strings.TrimSpace(runtimeMode), strings.TrimSpace(provider), strings.TrimSpace(status), nil
}

func (s *Server) lookupAgentRoleAndVertical(ctx context.Context, agentID string) (role, verticalID string, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(role, ''), COALESCE(vertical_id::text, '')
		FROM agents
		WHERE id = $1
	`, strings.TrimSpace(agentID)).Scan(&role, &verticalID)
	if err != nil {
		return "", "", err
	}
	return strings.TrimSpace(role), strings.TrimSpace(verticalID), nil
}

func (s *Server) resolveWorkspaceContainer(ctx context.Context, role, verticalID string) (container, verticalSlug string, err error) {
	role = strings.ToLower(strings.TrimSpace(role))
	switch role {
	case "holding-devops":
		return envOr("EMPIREAI_INFRA_CONTAINER", "empireai-infra"), "", nil
	case "factory-cto",
		"empire-coordinator",
		"operations-analyst",
		"scanner-agent",
		"analysis-agent",
		"validation-coordinator",
		"pre-brand-agent",
		"business-research-agent",
		"lightweight-spec-agent",
		"spec-reviewer",
		"market-research-agent",
		"trend-research-agent",
		"spec-auditor",
		"discovery-coordinator":
		return envOr("EMPIREAI_FACTORY_CONTAINER", "empireai-factory"), "", nil
	}

	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return "", "", nil
	}
	slug := ""
	_ = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&slug)
	slug = sanitizeContainerSlug(slug)
	if slug == "" {
		slug = sanitizeContainerSlug(verticalID)
	}
	if slug == "" {
		return "", "", fmt.Errorf("vertical slug unavailable for %s", verticalID)
	}
	return envOr("EMPIREAI_VERTICAL_CONTAINER_PREFIX", "empireai-") + slug, slug, nil
}

func (s *Server) findSessionProjectFile(ctx context.Context, container, sessionID string) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "", fmt.Errorf("session_id is required")
	}
	if !isSafeSessionID(sessionID) {
		return "", fmt.Errorf("invalid session_id format")
	}
	out, err := runDocker(ctx, "exec", container, "find", "/home/agent/.claude/projects", "-type", "f", "-name", sessionID+".jsonl")
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for _, line := range lines {
		path := strings.TrimSpace(line)
		if path != "" {
			return path, nil
		}
	}
	return "", nil
}

func (s *Server) tailContainerFile(ctx context.Context, container, path string, lines int) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("path is required")
	}
	if lines <= 0 {
		lines = 80
	}
	out, err := runDocker(ctx, "exec", container, "tail", "-n", strconv.Itoa(lines), path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func runDocker(ctx context.Context, args ...string) (string, error) {
	dockerBin := strings.TrimSpace(os.Getenv("EMPIREAI_DOCKER_BIN"))
	if dockerBin == "" {
		dockerBin = "docker"
	}
	cmd := exec.CommandContext(ctx, dockerBin, args...)
	raw, err := cmd.CombinedOutput()
	out := strings.TrimSpace(string(raw))
	if err != nil {
		if out == "" {
			return "", err
		}
		return "", fmt.Errorf("%w: %s", err, out)
	}
	return out, nil
}

func isSafeSessionID(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return false
	}
	for _, r := range v {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		return false
	}
	return true
}

func sanitizeContainerSlug(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_' || r == ' ' || r == '/':
			if !lastDash {
				b.WriteRune('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func extractTurnArtifacts(respRaw []byte) (string, []map[string]any) {
	if len(respRaw) == 0 {
		return "", nil
	}
	var obj map[string]any
	if err := json.Unmarshal(respRaw, &obj); err != nil {
		return "", nil
	}
	textParts := make([]string, 0, 4)
	toolCalls := make([]map[string]any, 0, 4)

	if v, ok := obj["result"].(string); ok && strings.TrimSpace(v) != "" {
		textParts = append(textParts, v)
	}
	if v, ok := obj["content"].(string); ok && strings.TrimSpace(v) != "" {
		textParts = append(textParts, v)
	}
	if arr, ok := obj["content"].([]any); ok {
		for _, item := range arr {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			typeName := strings.TrimSpace(asString(m["type"]))
			if typeName == "text" {
				if txt := strings.TrimSpace(asString(m["text"])); txt != "" {
					textParts = append(textParts, txt)
				}
			}
			if typeName == "tool_use" {
				toolCalls = append(toolCalls, map[string]any{
					"name":      asString(m["name"]),
					"arguments": m["input"],
				})
			}
		}
	}
	if arr, ok := obj["tool_calls"].([]any); ok {
		for _, item := range arr {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			toolCalls = append(toolCalls, map[string]any{
				"name":      asString(m["name"]),
				"arguments": m["arguments"],
			})
		}
	}
	return strings.TrimSpace(strings.Join(textParts, "\n")), toolCalls
}

func extractToolResult(reqRaw []byte) string {
	if len(reqRaw) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(reqRaw, &obj); err != nil {
		return ""
	}
	message, ok := obj["message"].(map[string]any)
	if !ok {
		return ""
	}
	role := strings.TrimSpace(strings.ToLower(asString(message["role"])))
	if role != "tool" {
		return ""
	}
	return asString(message["content"])
}

func (s *Server) handleFunnel(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()

	stageCounts := map[string]int{}
	rows, err := s.db.QueryContext(ctx, `SELECT stage, count(*) FROM verticals GROUP BY stage`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for rows.Next() {
		var stage string
		var n int
		if err := rows.Scan(&stage, &n); err != nil {
			rows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		stageCounts[stage] = n
	}
	rows.Close()

	stuck := make([]map[string]any, 0, 32)
	rows, err = s.db.QueryContext(ctx, `
		SELECT id::text, name, COALESCE(slug, ''), stage, mode, COALESCE(created_at, now()), COALESCE(updated_at, now()),
			(extract(epoch from (now() - COALESCE(updated_at, now())) ) / 3600)::bigint AS idle_hours
		FROM verticals
		WHERE stage NOT IN ('operating', 'expanding', 'killed', 'winding_down')
		  AND updated_at < now() - interval '24 hours'
		ORDER BY updated_at ASC
		LIMIT 50
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for rows.Next() {
		var id, name, slug, stage, mode string
		var created, updated time.Time
		var idleHours int64
		if err := rows.Scan(&id, &name, &slug, &stage, &mode, &created, &updated, &idleHours); err != nil {
			rows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		stuck = append(stuck, map[string]any{
			"id":         id,
			"name":       name,
			"slug":       slug,
			"stage":      stage,
			"mode":       mode,
			"created_at": created,
			"updated_at": updated,
			"idle_hours": idleHours,
		})
	}
	rows.Close()

	byDay := make([]map[string]any, 0, 14)
	rows, err = s.db.QueryContext(ctx, `
		SELECT
			date(created_at) AS day,
			count(*) FILTER (WHERE mode = 'factory') AS discoveries,
			count(*) FILTER (WHERE stage NOT IN ('discovered', 'scoring')) AS progressed,
			count(*) FILTER (WHERE stage = 'killed') AS killed
		FROM verticals
		WHERE created_at >= now() - interval '14 days'
		GROUP BY 1
		ORDER BY 1 ASC
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	var totalDiscoveries, totalProgressed, totalKilled int
	for rows.Next() {
		var day time.Time
		var d, p, k int
		if err := rows.Scan(&day, &d, &p, &k); err != nil {
			rows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		totalDiscoveries += d
		totalProgressed += p
		totalKilled += k
		byDay = append(byDay, map[string]any{
			"day":         day.Format("2006-01-02"),
			"discoveries": d,
			"progressed":  p,
			"killed":      k,
		})
	}
	rows.Close()

	var approvedOrLive int
	_ = s.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM verticals
		WHERE stage IN ('approved', 'full_speccing', 'building', 'pre_launch', 'launched', 'operating', 'expanding')
	`).Scan(&approvedOrLive)
	var killedTotal int
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM verticals WHERE stage = 'killed'`).Scan(&killedTotal)

	scoringRate := 0.0
	if totalDiscoveries > 0 {
		scoringRate = float64(totalProgressed) / float64(totalDiscoveries)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"stage_counts": stageCounts,
		"stuck":        stuck,
		"throughput": map[string]any{
			"daily":                   byDay,
			"discoveries_14d":         totalDiscoveries,
			"progressed_14d":          totalProgressed,
			"killed_14d":              totalKilled,
			"scoring_completion_rate": scoringRate,
			"specs_approved_or_live":  approvedOrLive,
			"specs_killed_total":      killedTotal,
		},
	})
}

func (s *Server) handleDigest(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	if s.db == nil || s.mailboxStore == nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("digest requires persistent store mode"))
		return
	}
	ctx := r.Context()
	topN := clamp(parseInt(r.URL.Query().Get("top"), 10), 1, 100)

	pg := &store.PostgresStore{DB: s.db}
	snap, err := digest.BuildSnapshot(ctx, pg, s.mailboxStore, topN)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	currentText := digest.RenderText(snap)

	// Best-effort: load most recent compiled digest event.
	var last map[string]any
	var lastAt sql.NullTime
	var lastPayloadRaw []byte
	if err := s.db.QueryRowContext(ctx, `
		SELECT created_at, COALESCE(payload, '{}'::jsonb)
		FROM events
		WHERE type = 'portfolio.digest_compiled'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&lastAt, &lastPayloadRaw); err == nil && lastAt.Valid {
		_ = json.Unmarshal(lastPayloadRaw, &last)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"current": map[string]any{
			"top_n": topN,
			"text":  currentText,
			"snap":  snap,
		},
		"last_compiled": map[string]any{
			"at":      lastAt.Time.UTC(),
			"payload": last,
		},
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()

	postgres := map[string]any{}
	var activeConnections, maxConnections int
	var commits int64
	var blksHit, blksRead int64
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()`).Scan(&activeConnections)
	_ = s.db.QueryRowContext(ctx, `SELECT setting::int FROM pg_settings WHERE name = 'max_connections'`).Scan(&maxConnections)
	_ = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(xact_commit,0), COALESCE(blks_hit,0), COALESCE(blks_read,0)
		FROM pg_stat_database
		WHERE datname = current_database()
	`).Scan(&commits, &blksHit, &blksRead)
	postgres["active_connections"] = activeConnections
	postgres["max_connections"] = maxConnections
	postgres["xact_commit"] = commits
	postgres["blks_hit"] = blksHit
	postgres["blks_read"] = blksRead

	spend := map[string]any{}
	var api24h, api7d, infra24h, spend24h int64
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(api_cost_cents),0) FROM vertical_metrics WHERE period_end >= current_date - interval '1 day'`).Scan(&api24h)
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(api_cost_cents),0) FROM vertical_metrics WHERE period_end >= current_date - interval '7 days'`).Scan(&api7d)
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(infra_cost_cents),0) FROM vertical_metrics WHERE period_end >= current_date - interval '1 day'`).Scan(&infra24h)
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(amount_cents),0) FROM spend_ledger WHERE created_at >= now() - interval '24 hours'`).Scan(&spend24h)
	spend["api_cost_24h_cents"] = api24h
	spend["api_cost_daily_avg_7d_cents"] = api7d / 7
	spend["infra_cost_24h_cents"] = infra24h
	spend["spend_ledger_24h_cents"] = spend24h

	auth := map[string]any{}
	auth["oauth_token_configured"] = strings.TrimSpace(os.Getenv("CLAUDE_CODE_OAUTH_TOKEN")) != ""
	var authErr1h, authErr24h int
	_ = s.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM agent_turns
		WHERE created_at >= now() - interval '1 hour'
		  AND lower(COALESCE(error, '')) LIKE '%not logged in%'
	`).Scan(&authErr1h)
	_ = s.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM agent_turns
		WHERE created_at >= now() - interval '24 hours'
		  AND lower(COALESCE(error, '')) LIKE '%not logged in%'
	`).Scan(&authErr24h)
	auth["auth_errors_1h"] = authErr1h
	auth["auth_errors_24h"] = authErr24h

	runtimeHealth := map[string]any{}
	if s.manager != nil {
		runtimeHealth["running"] = s.manager.IsRunning()
		runtimeHealth["loaded_agents"] = s.manager.Count()
	} else {
		runtimeHealth["running"] = false
		runtimeHealth["loaded_agents"] = 0
	}

	verticalHealth := make([]map[string]any, 0, 100)
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			v.id::text,
			COALESCE(v.slug, ''),
			v.stage,
			COALESCE(d.environment, ''),
			COALESCE(d.status, ''),
			COALESCE(d.health_status, 'unknown'),
			d.last_health_at,
			COALESCE(d.url, '')
		FROM verticals v
		LEFT JOIN LATERAL (
			SELECT environment, status, health_status, last_health_at, url
			FROM deployments d
			WHERE d.vertical_id = v.id
			ORDER BY d.created_at DESC
			LIMIT 1
		) d ON true
		WHERE v.stage IN ('building', 'pre_launch', 'launched', 'operating', 'expanding')
		ORDER BY v.updated_at DESC
		LIMIT 100
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id, slug, stage, env, depStatus, healthStatus, url string
			var lastHealth sql.NullTime
			if err := rows.Scan(&id, &slug, &stage, &env, &depStatus, &healthStatus, &lastHealth, &url); err != nil {
				continue
			}
			row := map[string]any{
				"vertical_id":   id,
				"slug":          slug,
				"stage":         stage,
				"environment":   env,
				"deploy_status": depStatus,
				"health_status": healthStatus,
				"url":           url,
			}
			if lastHealth.Valid {
				row["last_health_at"] = lastHealth.Time
			}
			verticalHealth = append(verticalHealth, row)
		}
	}

	containers, dockerErr := dockerContainers(ctx)
	out := map[string]any{
		"generated_at":    s.now().UTC(),
		"postgres":        postgres,
		"spend":           spend,
		"auth":            auth,
		"runtime":         runtimeHealth,
		"containers":      containers,
		"vertical_health": verticalHealth,
	}
	if dockerErr != nil {
		out["container_error"] = dockerErr.Error()
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handlePipelineHealth(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("database unavailable"))
		return
	}
	ctx := r.Context()

	campaigns := map[string]any{
		"active":    0,
		"paused":    0,
		"completed": 0,
	}
	campaignRows, err := s.db.QueryContext(ctx, `
		SELECT status, COUNT(*)
		FROM scan_campaigns
		GROUP BY status
	`)
	if err == nil {
		defer campaignRows.Close()
		for campaignRows.Next() {
			var status string
			var count int
			if err := campaignRows.Scan(&status, &count); err != nil {
				continue
			}
			switch strings.TrimSpace(status) {
			case "active":
				campaigns["active"] = count
			case "paused":
				campaigns["paused"] = count
			case "completed":
				campaigns["completed"] = count
			}
		}
	}

	validations := map[string]any{
		"active":   0,
		"packaged": 0,
		"parked":   0,
		"rejected": 0,
		"approved": 0,
	}
	var validationActive, validationPackaged, validationParked, validationRejected, validationApproved int
	_ = s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE stage IN ('researching','mvp_speccing','spec_review','cto_spec_review','branding')) AS active,
			COUNT(*) FILTER (WHERE stage = 'ready_for_review') AS packaged,
			COUNT(*) FILTER (WHERE stage = 'marginal_review') AS parked,
			COUNT(*) FILTER (WHERE stage = 'killed') AS rejected,
			COUNT(*) FILTER (WHERE stage IN ('approved','building','pre_launch','launched','operating','expanding')) AS approved
		FROM verticals
	`).Scan(&validationActive, &validationPackaged, &validationParked, &validationRejected, &validationApproved)
	validations["active"] = validationActive
	validations["packaged"] = validationPackaged
	validations["parked"] = validationParked
	validations["rejected"] = validationRejected
	validations["approved"] = validationApproved

	scans := map[string]any{
		"active":             campaigns["active"],
		"timed_out_last_24h": 0,
	}
	var scanTimeouts int
	_ = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pipeline_transitions
		WHERE pipeline_type = 'scan'
		  AND created_at >= now() - interval '24 hours'
		  AND (
			(action = 'error' AND lower(COALESCE(error, '')) LIKE '%timeout%')
			OR
			(action = 'dropped' AND lower(COALESCE(drop_reason, '')) LIKE '%timeout%')
		  )
	`).Scan(&scanTimeouts)
	scans["timed_out_last_24h"] = scanTimeouts

	marginals := map[string]any{
		"parked":      validationParked,
		"oldest_days": 0,
	}
	var marginalOldestDays int
	_ = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(EXTRACT(EPOCH FROM (now() - COALESCE(parked_at, updated_at))) / 86400), 0)::int
		FROM verticals
		WHERE stage = 'marginal_review'
	`).Scan(&marginalOldestDays)
	marginals["oldest_days"] = marginalOldestDays

	alerts := make([]string, 0, 32)
	validationAlerts, err := s.db.QueryContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), id::text), stage, updated_at
		FROM verticals
		WHERE stage IN ('researching','mvp_speccing','spec_review','cto_spec_review','branding','ready_for_review')
		  AND updated_at <= now() - interval '2 hours'
		ORDER BY updated_at ASC
		LIMIT 25
	`)
	if err == nil {
		defer validationAlerts.Close()
		for validationAlerts.Next() {
			var verticalLabel, stage string
			var updatedAt time.Time
			if err := validationAlerts.Scan(&verticalLabel, &stage, &updatedAt); err != nil {
				continue
			}
			alerts = append(alerts, fmt.Sprintf(
				"validation %s: no transition in %s (stage=%s)",
				strings.TrimSpace(verticalLabel),
				compactAge(s.now().UTC().Sub(updatedAt.UTC())),
				strings.TrimSpace(stage),
			))
		}
	}
	campaignAlerts, err := s.db.QueryContext(ctx, `
		SELECT id::text, status, COALESCE(completed_at, started_at, created_at) AS activity_at
		FROM scan_campaigns
		WHERE (status = 'paused' AND COALESCE(started_at, created_at) <= now() - interval '6 hours')
		   OR (status = 'active' AND COALESCE(started_at, created_at) <= now() - interval '90 minutes')
		ORDER BY COALESCE(started_at, created_at) ASC
		LIMIT 25
	`)
	if err == nil {
		defer campaignAlerts.Close()
		for campaignAlerts.Next() {
			var id, status string
			var updatedAt time.Time
			if err := campaignAlerts.Scan(&id, &status, &updatedAt); err != nil {
				continue
			}
			reason := "running too long"
			if status == "paused" {
				reason = "paused"
			}
			alerts = append(alerts, fmt.Sprintf(
				"campaign %s: %s for %s",
				strings.TrimSpace(id),
				reason,
				compactAge(s.now().UTC().Sub(updatedAt.UTC())),
			))
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"campaigns":    campaigns,
		"validations":  validations,
		"scans":        scans,
		"marginals":    marginals,
		"alerts":       alerts,
		"generated_at": s.now().UTC(),
	})
}

func (s *Server) handleHolding(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()

	// Q1 — Campaigns
	campaigns := make([]map[string]any, 0, 50)
	campRows, err := s.db.QueryContext(ctx, `
		SELECT sc.id::text, sc.mode, g.name, g.country, sc.status, sc.priority,
		       COALESCE(sc.discoveries,0), COALESCE(array_to_string(sc.categories,','), ''),
		       sc.created_at, sc.started_at, sc.completed_at
		FROM scan_campaigns sc JOIN geographies g ON g.id = sc.geography_id
		ORDER BY CASE sc.status WHEN 'active' THEN 0 WHEN 'queued' THEN 1 WHEN 'paused' THEN 2 ELSE 3 END,
		         sc.created_at DESC
		LIMIT 50
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for campRows.Next() {
		var id, mode, geoName, country, status, priority, catStr string
		var discoveries int
		var createdAt time.Time
		var startedAt, completedAt sql.NullTime
		if err := campRows.Scan(&id, &mode, &geoName, &country, &status, &priority,
			&discoveries, &catStr, &createdAt, &startedAt, &completedAt); err != nil {
			campRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		var cats []string
		if catStr != "" {
			cats = strings.Split(catStr, ",")
		}
		c := map[string]any{
			"id":          strings.TrimSpace(id),
			"mode":        mode,
			"geography":   geoName,
			"country":     country,
			"status":      status,
			"priority":    priority,
			"discoveries": discoveries,
			"categories":  cats,
			"created_at":  createdAt,
		}
		if startedAt.Valid {
			c["started_at"] = startedAt.Time
		}
		if completedAt.Valid {
			c["completed_at"] = completedAt.Time
		}
		campaigns = append(campaigns, c)
	}
	campRows.Close()

	// Q2 — Verticals with scores + kill info
	verts := make([]map[string]any, 0, 500)
	vertRows, err := s.db.QueryContext(ctx, `
		SELECT v.id::text, COALESCE(v.slug,''), v.name, COALESCE(v.geography,''),
		       v.stage, COALESCE(v.mode,'factory'),
		       COALESCE((v.scores->>'composite_score')::text,''),
		       COALESCE(v.kill_reason,''), COALESCE(v.killed_at_stage,''),
		       COALESCE(v.created_at, now()), COALESCE(v.updated_at, now()), v.approved_at, v.parked_at, v.launched_at
		FROM verticals v ORDER BY v.updated_at DESC LIMIT 500
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for vertRows.Next() {
		var id, slug, name, geo, stage, mode, composite, killReason, killedAtStage string
		var createdAt, updatedAt time.Time
		var approvedAt, parkedAt, launchedAt sql.NullTime
		if err := vertRows.Scan(&id, &slug, &name, &geo, &stage, &mode,
			&composite, &killReason, &killedAtStage,
			&createdAt, &updatedAt, &approvedAt, &parkedAt, &launchedAt); err != nil {
			vertRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		v := map[string]any{
			"id":              strings.TrimSpace(id),
			"slug":            slug,
			"name":            name,
			"geography":       geo,
			"stage":           stage,
			"mode":            mode,
			"composite_score": composite,
			"kill_reason":     killReason,
			"killed_at_stage": killedAtStage,
			"created_at":      createdAt,
			"updated_at":      updatedAt,
		}
		if approvedAt.Valid {
			v["approved_at"] = approvedAt.Time
		}
		if parkedAt.Valid {
			v["parked_at"] = parkedAt.Time
		}
		if launchedAt.Valid {
			v["launched_at"] = launchedAt.Time
		}
		verts = append(verts, v)
	}
	vertRows.Close()

	// Q3 — Active agent counts per vertical
	agentCounts := map[string]map[string]int{}
	acRows, err := s.db.QueryContext(ctx, `
		SELECT vertical_id::text, COUNT(*),
		       COUNT(*) FILTER (WHERE status IN ('working','running','busy'))
		FROM agents WHERE vertical_id IS NOT NULL GROUP BY vertical_id
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for acRows.Next() {
		var vid string
		var total, active int
		if err := acRows.Scan(&vid, &total, &active); err != nil {
			acRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		agentCounts[strings.TrimSpace(vid)] = map[string]int{"total": total, "active": active}
	}
	acRows.Close()

	// Q4 — Summary counts
	var sTotal, sInPipeline, sKilled, sDiscovered int
	_ = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE stage NOT IN ('killed','winding_down')),
		       COUNT(*) FILTER (WHERE stage = 'killed'),
		       COUNT(*) FILTER (WHERE stage = 'discovered')
		FROM verticals
	`).Scan(&sTotal, &sInPipeline, &sKilled, &sDiscovered)

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"campaigns":    campaigns,
		"verticals":    verts,
		"agent_counts": agentCounts,
		"summary": map[string]int{
			"total":       sTotal,
			"in_pipeline": sInPipeline,
			"killed":      sKilled,
			"discovered":  sDiscovered,
		},
	})
}

func (s *Server) handleHoldingVerticalDetail(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	target := strings.TrimSpace(r.URL.Query().Get("id"))
	if target == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("id is required"))
		return
	}

	parseJSONDoc := func(raw []byte) any {
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" || trimmed == "null" || trimmed == "{}" || trimmed == "[]" {
			return nil
		}
		var out any
		if err := json.Unmarshal([]byte(trimmed), &out); err != nil {
			return trimmed
		}
		return out
	}

	var (
		verticalID      string
		slug            string
		name            string
		geography       string
		stage           string
		mode            string
		templateVersion string
		liveURL         string
		humanNotes      string
		killReason      string
		killedAtStage   string
		compositeScore  string
		rawSignalsRaw   []byte
		scoresRaw       []byte
		businessBrief   []byte
		mvpSpec         []byte
		specReview      []byte
		ctoFeasibility  []byte
		brandRaw        []byte
		validationKit   []byte
		fullSpec        []byte
		deployConfig    []byte
		launchTargets   []byte
		credsRaw        []byte
		createdAt       time.Time
		updatedAt       time.Time
		approvedAt      sql.NullTime
		parkedAt        sql.NullTime
		launchedAt      sql.NullTime
	)

	if err := s.db.QueryRowContext(ctx, `
		SELECT
			v.id::text,
			COALESCE(v.slug,''),
			COALESCE(v.name,''),
			COALESCE(v.geography,''),
			COALESCE(v.stage,''),
			COALESCE(v.mode,'factory'),
			COALESCE(v.template_version,''),
			COALESCE(v.live_url,''),
			COALESCE(v.human_notes,''),
			COALESCE(v.kill_reason,''),
			COALESCE(v.killed_at_stage,''),
			COALESCE((v.scores->>'composite_score')::text,''),
			COALESCE(v.raw_signals,'{}'::jsonb),
			COALESCE(v.scores,'{}'::jsonb),
			COALESCE(v.business_brief,'{}'::jsonb),
			COALESCE(v.mvp_spec,'{}'::jsonb),
			COALESCE(v.spec_review,'{}'::jsonb),
			COALESCE(v.cto_feasibility,'{}'::jsonb),
			COALESCE(v.brand,'{}'::jsonb),
			COALESCE(v.validation_kit,'{}'::jsonb),
			COALESCE(v.full_spec,'{}'::jsonb),
			COALESCE(v.deploy_config,'{}'::jsonb),
			COALESCE(v.launch_targets,'{}'::jsonb),
			COALESCE(v.credentials,'{}'::jsonb),
			COALESCE(v.created_at, now()),
			COALESCE(v.updated_at, now()),
			v.approved_at,
			v.parked_at,
			v.launched_at
		FROM verticals v
		WHERE v.id::text = $1 OR COALESCE(v.slug,'') = $1
		LIMIT 1
	`, target).Scan(
		&verticalID,
		&slug,
		&name,
		&geography,
		&stage,
		&mode,
		&templateVersion,
		&liveURL,
		&humanNotes,
		&killReason,
		&killedAtStage,
		&compositeScore,
		&rawSignalsRaw,
		&scoresRaw,
		&businessBrief,
		&mvpSpec,
		&specReview,
		&ctoFeasibility,
		&brandRaw,
		&validationKit,
		&fullSpec,
		&deployConfig,
		&launchTargets,
		&credsRaw,
		&createdAt,
		&updatedAt,
		&approvedAt,
		&parkedAt,
		&launchedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErr(w, http.StatusNotFound, fmt.Errorf("vertical not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	vertical := map[string]any{
		"id":               strings.TrimSpace(verticalID),
		"slug":             slug,
		"name":             name,
		"geography":        geography,
		"stage":            stage,
		"mode":             mode,
		"template_version": templateVersion,
		"live_url":         liveURL,
		"human_notes":      humanNotes,
		"kill_reason":      killReason,
		"killed_at_stage":  killedAtStage,
		"composite_score":  compositeScore,
		"raw_signals":      parseJSONDoc(rawSignalsRaw),
		"scores":           parseJSONDoc(scoresRaw),
		"business_brief":   parseJSONDoc(businessBrief),
		"mvp_spec":         parseJSONDoc(mvpSpec),
		"spec_review":      parseJSONDoc(specReview),
		"cto_feasibility":  parseJSONDoc(ctoFeasibility),
		"brand":            parseJSONDoc(brandRaw),
		"validation_kit":   parseJSONDoc(validationKit),
		"full_spec":        parseJSONDoc(fullSpec),
		"deploy_config":    parseJSONDoc(deployConfig),
		"launch_targets":   parseJSONDoc(launchTargets),
		"credentials":      parseJSONDoc(credsRaw),
		"created_at":       createdAt,
		"updated_at":       updatedAt,
	}
	if approvedAt.Valid {
		vertical["approved_at"] = approvedAt.Time
	}
	if parkedAt.Valid {
		vertical["parked_at"] = parkedAt.Time
	}
	if launchedAt.Valid {
		vertical["launched_at"] = launchedAt.Time
	}

	agents := make([]map[string]any, 0, 24)
	agentRows, err := s.db.QueryContext(ctx, `
		SELECT
			id,
			COALESCE(role,''),
			COALESCE(mode,''),
			COALESCE(status,''),
			COALESCE(current_task_id::text,''),
			last_active_at
		FROM agents
		WHERE COALESCE(vertical_id::text,'') = $1
		ORDER BY role ASC, id ASC
	`, verticalID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for agentRows.Next() {
		var id, role, agentMode, status, taskID string
		var lastActive sql.NullTime
		if err := agentRows.Scan(&id, &role, &agentMode, &status, &taskID, &lastActive); err != nil {
			agentRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		item := map[string]any{
			"id":              id,
			"role":            role,
			"mode":            agentMode,
			"status":          status,
			"current_task_id": taskID,
		}
		if lastActive.Valid {
			item["last_active_at"] = lastActive.Time
		}
		agents = append(agents, item)
	}
	agentRows.Close()

	recentEvents := make([]map[string]any, 0, 40)
	eventRows, err := s.db.QueryContext(ctx, `
		SELECT id::text, type, source_agent, payload, created_at
		FROM events
		WHERE COALESCE(vertical_id::text,'') = $1
		ORDER BY created_at DESC
		LIMIT 40
	`, verticalID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for eventRows.Next() {
		var eventID, typ, source string
		var payloadRaw []byte
		var created time.Time
		if err := eventRows.Scan(&eventID, &typ, &source, &payloadRaw, &created); err != nil {
			eventRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		recentEvents = append(recentEvents, map[string]any{
			"id":           eventID,
			"type":         typ,
			"source_agent": source,
			"payload":      parseJSONDoc(payloadRaw),
			"created_at":   created,
		})
	}
	eventRows.Close()
	enrichHoldingVerticalArtifacts(vertical, recentEvents)

	mailboxItems := make([]map[string]any, 0, 25)
	mailRows, err := s.db.QueryContext(ctx, `
		SELECT
			id::text,
			COALESCE(from_agent,''),
			COALESCE(type,''),
			COALESCE(priority,''),
			COALESCE(status,''),
			COALESCE(summary,''),
			COALESCE(decision,''),
			COALESCE(decision_notes,''),
			COALESCE(context,'{}'::jsonb),
			created_at,
			decided_at
		FROM mailbox
		WHERE COALESCE(vertical_id::text,'') = $1
		ORDER BY created_at DESC
		LIMIT 25
	`, verticalID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for mailRows.Next() {
		var id, from, typ, priority, status, summary, decision, decisionNotes string
		var ctxRaw []byte
		var created time.Time
		var decided sql.NullTime
		if err := mailRows.Scan(&id, &from, &typ, &priority, &status, &summary, &decision, &decisionNotes, &ctxRaw, &created, &decided); err != nil {
			mailRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		item := map[string]any{
			"id":             id,
			"from_agent":     from,
			"type":           typ,
			"priority":       priority,
			"status":         status,
			"summary":        summary,
			"decision":       decision,
			"decision_notes": decisionNotes,
			"context":        parseJSONDoc(ctxRaw),
			"created_at":     created,
		}
		if decided.Valid {
			item["decided_at"] = decided.Time
		}
		mailboxItems = append(mailboxItems, item)
	}
	mailRows.Close()

	var spendAll, spendLast30 int64
	_ = s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(amount_cents),0),
			COALESCE(SUM(amount_cents) FILTER (WHERE created_at >= now() - interval '30 days'),0)
		FROM spend_ledger
		WHERE COALESCE(vertical_id::text,'') = $1
	`, verticalID).Scan(&spendAll, &spendLast30)

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"vertical":     vertical,
		"agents":       agents,
		"events":       recentEvents,
		"mailbox":      mailboxItems,
		"spend": map[string]any{
			"all_time_cents": spendAll,
			"last_30d_cents": spendLast30,
		},
	})
}

func enrichHoldingVerticalArtifacts(vertical map[string]any, recentEvents []map[string]any) {
	if len(vertical) == 0 || len(recentEvents) == 0 {
		return
	}
	setFromPayload := func(key string, value any) {
		if holdingArtifactEmpty(value) {
			return
		}
		if holdingArtifactEmpty(vertical[key]) {
			vertical[key] = value
			return
		}
		dst, dstOK := vertical[key].(map[string]any)
		src, srcOK := value.(map[string]any)
		if dstOK && srcOK {
			vertical[key] = holdingMergeMapMissing(dst, src)
			return
		}
		if holdingArtifactEmpty(vertical[key]) {
			vertical[key] = value
		}
	}
	for _, evt := range recentEvents {
		typ := strings.TrimSpace(asString(evt["type"]))
		payload, _ := evt["payload"].(map[string]any)
		if len(payload) == 0 {
			continue
		}
		switch typ {
		case "vertical.discovered":
			setFromPayload("raw_signals", payload)
		case "vertical.scored":
			setFromPayload("scores", holdingPickMap(payload, "scores", "scoring", "result"))
			setFromPayload("scores", payload)
		case "research.completed":
			setFromPayload("business_brief", holdingPickNestedMap(payload, []string{"business_brief"}, []string{"brief"}, []string{"research", "business_brief"}, []string{"research"}))
		case "spec.draft_ready":
			setFromPayload("mvp_spec", holdingPickNestedMap(payload, []string{"mvp_spec"}, []string{"spec", "mvp_spec"}, []string{"spec"}, []string{"draft"}))
			setFromPayload("mvp_spec", payload)
		case "spec.approved":
			setFromPayload("full_spec", holdingPickNestedMap(payload, []string{"full_spec"}, []string{"spec"}))
			setFromPayload("mvp_spec", holdingPickNestedMap(payload, []string{"mvp_spec"}, []string{"spec", "mvp_spec"}, []string{"spec"}))
		case "spec_review.requested", "spec_review.passed", "spec_review.issues_found":
			setFromPayload("spec_review", payload)
		case "cto.spec_review_requested", "cto.spec_approved", "cto.spec_revision_needed":
			setFromPayload("cto_feasibility", holdingPickNestedMap(payload, []string{"cto_feasibility"}, []string{"cto_notes"}, []string{"feasibility"}))
			setFromPayload("cto_feasibility", payload)
		case "brand.requested", "brand.candidates_ready", "brand.revision_needed":
			setFromPayload("brand", holdingPickMap(payload, "brand"))
			setFromPayload("brand", payload)
		case "validation.package_ready", "vertical.ready_for_review":
			setFromPayload("validation_kit", payload)
			setFromPayload("business_brief", holdingPickNestedMap(payload, []string{"business_brief"}, []string{"research", "business_brief"}, []string{"research"}))
			setFromPayload("mvp_spec", holdingPickNestedMap(payload, []string{"mvp_spec"}, []string{"spec", "mvp_spec"}, []string{"spec"}))
			setFromPayload("full_spec", holdingPickNestedMap(payload, []string{"full_spec"}, []string{"spec"}))
			setFromPayload("cto_feasibility", holdingPickNestedMap(payload, []string{"cto_feasibility"}, []string{"cto_notes"}))
			setFromPayload("brand", holdingPickMap(payload, "brand"))
		}
	}
}

func holdingPickMap(payload map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		raw, ok := payload[key]
		if !ok || raw == nil {
			continue
		}
		if m, ok := raw.(map[string]any); ok && len(m) > 0 {
			return m
		}
	}
	return nil
}

func holdingPickNestedMap(payload map[string]any, paths ...[]string) map[string]any {
	for _, path := range paths {
		if len(path) == 0 {
			continue
		}
		var cursor any = payload
		ok := true
		for _, key := range path {
			m, isMap := cursor.(map[string]any)
			if !isMap {
				ok = false
				break
			}
			next, exists := m[key]
			if !exists || next == nil {
				ok = false
				break
			}
			cursor = next
		}
		if !ok {
			continue
		}
		if out, isMap := cursor.(map[string]any); isMap && len(out) > 0 {
			return out
		}
	}
	return nil
}

func holdingArtifactEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		s := strings.TrimSpace(t)
		return s == "" || s == "{}" || s == "[]"
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	default:
		return false
	}
}

func holdingMergeMapMissing(dst map[string]any, src map[string]any) map[string]any {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = map[string]any{}
	}
	for key, srcVal := range src {
		cur, exists := dst[key]
		if !exists || holdingArtifactEmpty(cur) {
			dst[key] = srcVal
			continue
		}
		curMap, curOK := cur.(map[string]any)
		srcMap, srcOK := srcVal.(map[string]any)
		if curOK && srcOK {
			dst[key] = holdingMergeMapMissing(curMap, srcMap)
		}
	}
	return dst
}

func compactAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func dockerContainers(parent context.Context) ([]map[string]string, error) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "ps", "--format", "{{.Names}}|{{.Image}}|{{.Status}}")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	containers := make([]map[string]string, 0, 16)
	s := bufio.NewScanner(strings.NewReader(string(out)))
	s.Buffer(make([]byte, 1024), 1024*1024)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 3)
		if len(parts) != 3 {
			continue
		}
		containers = append(containers, map[string]string{
			"name":   strings.TrimSpace(parts[0]),
			"image":  strings.TrimSpace(parts[1]),
			"status": strings.TrimSpace(parts[2]),
		})
	}
	if err := s.Err(); err != nil {
		return containers, err
	}
	return containers, nil
}

func (s *Server) handleVerticalTrace(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/dashboard/api/verticals/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[1] != "trace" {
		http.NotFound(w, r)
		return
	}
	vertical := strings.TrimSpace(parts[0])
	if vertical == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("vertical id or slug is required"))
		return
	}

	ctx := r.Context()
	rows, err := s.db.QueryContext(ctx, `
		SELECT e.id::text, e.type, e.source_agent, COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''), COALESCE(v.slug, ''), e.created_at,
			count(d.agent_id) AS delivery_count,
			count(r.agent_id) FILTER (WHERE r.status = 'processed') AS processed_count,
			count(r.agent_id) FILTER (WHERE r.status = 'error') AS error_count,
			count(r.agent_id) FILTER (WHERE r.status = 'dead_letter') AS dead_count,
			(count(d.agent_id) - count(r.agent_id)) AS pending_count,
			COALESCE((avg(extract(epoch from (r.processed_at - e.created_at)) * 1000) FILTER (WHERE r.processed_at IS NOT NULL))::bigint, 0) AS avg_ms
		FROM events e
		LEFT JOIN verticals v ON v.id = e.vertical_id
		LEFT JOIN event_deliveries d ON d.event_id = e.id
		LEFT JOIN event_receipts r ON r.event_id = d.event_id AND r.agent_id = d.agent_id
		WHERE (COALESCE(e.vertical_id::text, '') = $1 OR COALESCE(v.slug, '') = $1)
		GROUP BY e.id, v.slug
		ORDER BY e.created_at ASC
		LIMIT 1000
	`, vertical)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	trace := make([]eventView, 0, 200)
	for rows.Next() {
		var ev eventView
		if err := rows.Scan(
			&ev.ID,
			&ev.Type,
			&ev.SourceAgent,
			&ev.TaskID,
			&ev.VerticalID,
			&ev.VerticalSlug,
			&ev.CreatedAt,
			&ev.DeliveryCount,
			&ev.ProcessedCount,
			&ev.ErrorCount,
			&ev.DeadCount,
			&ev.PendingCount,
			&ev.AvgProcessMS,
		); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		trace = append(trace, ev)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	last := map[string]any{}
	if len(trace) > 0 {
		l := trace[len(trace)-1]
		last["id"] = l.ID
		last["type"] = l.Type
		last["source_agent"] = l.SourceAgent
		last["created_at"] = l.CreatedAt
		last["pending_count"] = l.PendingCount
		last["dead_count"] = l.DeadCount
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"vertical":    vertical,
		"event_count": len(trace),
		"last_event":  last,
		"trace":       trace,
	})
}

func (s *Server) handleAPIEvents(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("stream")), "true") {
		s.handleEventStream(w, r)
		return
	}
	s.handleEvents(w, r)
}

func (s *Server) handleAPIMailboxDetail(w http.ResponseWriter, r *http.Request) {
	// POST /api/mailbox/:id/decide
	path := strings.TrimPrefix(r.URL.Path, "/api/mailbox/")
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[1]) != "decide" {
		http.NotFound(w, r)
		return
	}
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.mailboxStore == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("mailbox store unavailable"))
		return
	}
	id := strings.TrimSpace(parts[0])
	if id == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("mailbox id is required"))
		return
	}
	var req struct {
		Action string `json:"action"`
		Notes  string `json:"notes"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.Action = strings.TrimSpace(req.Action)
	if req.Action == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("action is required"))
		return
	}

	item, err := s.mailboxStore.GetMailboxItem(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	outcome, err := mailboxsvc.Decide(r.Context(), s.mailboxStore, id, req.Action, req.Notes)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.emitMailboxDecisionSideEffects(r.Context(), item, outcome, req.Notes); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": id, "status": outcome.Status, "decision": outcome.Decision})
}

func (s *Server) handleAPIVerticals(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	type row struct {
		ID        string    `json:"id"`
		Slug      string    `json:"slug"`
		Name      string    `json:"name"`
		Geography string    `json:"geography"`
		Stage     string    `json:"stage"`
		Mode      string    `json:"mode"`
		CreatedAt time.Time `json:"created_at"`
		UpdatedAt time.Time `json:"updated_at"`
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id::text, COALESCE(slug,''), name, COALESCE(geography,''), COALESCE(stage,''), COALESCE(mode,''), COALESCE(created_at, now()), COALESCE(updated_at, now())
		FROM verticals
		ORDER BY updated_at DESC
		LIMIT 500
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()
	out := make([]row, 0, 64)
	for rows.Next() {
		var v row
		if err := rows.Scan(&v.ID, &v.Slug, &v.Name, &v.Geography, &v.Stage, &v.Mode, &v.CreatedAt, &v.UpdatedAt); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"generated_at": s.now().UTC(), "verticals": out})
}

func (s *Server) handleAPIVerticalDetail(w http.ResponseWriter, r *http.Request) {
	// GET /api/verticals/:id/agents
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/verticals/")
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[1]) != "agents" {
		http.NotFound(w, r)
		return
	}
	vertical := strings.TrimSpace(parts[0])
	if vertical == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("vertical id or slug is required"))
		return
	}
	ctx := r.Context()
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			a.id,
			COALESCE(a.role, ''),
			COALESCE(a.mode, ''),
			COALESCE(a.status, ''),
			COALESCE(a.vertical_id::text, ''),
			COALESCE(v.slug, '')
		FROM agents a
		LEFT JOIN verticals v ON v.id = a.vertical_id
		WHERE COALESCE(a.vertical_id::text,'') = $1
		   OR COALESCE(v.slug,'') = $1
		ORDER BY a.id ASC
		LIMIT 500
	`, vertical)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()
	out := make([]map[string]any, 0, 32)
	for rows.Next() {
		var id, role, mode, status, vid, slug string
		if err := rows.Scan(&id, &role, &mode, &status, &vid, &slug); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		out = append(out, map[string]any{
			"agent_id":      id,
			"role":          role,
			"mode":          mode,
			"status":        status,
			"vertical_id":   vid,
			"vertical_slug": slug,
		})
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"generated_at": s.now().UTC(), "vertical": vertical, "agents": out})
}

func (s *Server) handleAPIChat(w http.ResponseWriter, r *http.Request) {
	// POST /api/chat/:agent
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	agentID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/chat/"))
	agentID = strings.Trim(agentID, "/")
	if agentID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("agent is required"))
		return
	}
	target, err := s.lookupControlTarget(r.Context(), agentID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var req struct {
		Message string `json:"message"`
		Mode    string `json:"mode"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.Message = strings.TrimSpace(req.Message)
	req.Mode = strings.TrimSpace(strings.ToLower(req.Mode))
	if req.Message == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("message is required"))
		return
	}
	if req.Mode == "" {
		req.Mode = "live"
	}
	eventID, err := s.queueBoardMessage(r.Context(), target, events.EventType("board.chat"), req.Message)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	resp := map[string]any{"ok": true, "event_id": eventID, "target": target, "mode": req.Mode}
	if req.Mode != "async" && s.manager != nil {
		reply, chatErr := s.manager.ChatWithAgent(r.Context(), agentID, req.Message)
		if chatErr != nil {
			resp["chat_error"] = chatErr.Error()
		} else {
			resp["response"] = strings.TrimSpace(reply)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAPIDirective(w http.ResponseWriter, r *http.Request) {
	// POST /api/directive (queues system.directive to Empire Coordinator)
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.eventStore == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("event store unavailable"))
		return
	}
	var req struct {
		DirectiveText string `json:"directive_text"`
		Message       string `json:"message"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	text := strings.TrimSpace(coalesce(req.DirectiveText, req.Message))
	if text == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("directive_text is required"))
		return
	}
	started, err := s.hasSystemStarted(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if !started {
		writeErr(w, http.StatusConflict, fmt.Errorf("system is not initialized yet (missing system.started); run `empire init` first"))
		return
	}
	eventID, err := s.queueSystemDirective(r.Context(), text, "api")
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(strings.ToLower(err.Error()), "runtime manager unavailable") {
			status = http.StatusServiceUnavailable
		}
		writeErr(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "event_id": eventID})
}

func (s *Server) handleAPIBudget(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	monthStart := s.now().UTC()
	monthStart = time.Date(monthStart.Year(), monthStart.Month(), 1, 0, 0, 0, 0, time.UTC)
	var spent int64
	_ = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(sum(amount_cents),0)
		FROM spend_ledger
		WHERE created_at >= $1
	`, monthStart).Scan(&spent)

	caps := map[string]any{}
	if s.cfg != nil {
		caps["factory_monthly_cap_cents"] = s.cfg.Budget.FactoryMonthlyCap
		caps["per_vertical_monthly_cap_cents"] = s.cfg.Budget.PerVerticalMonthlyCap
		caps["portfolio_monthly_cap_cents"] = s.cfg.Budget.PortfolioMonthlyCap
	}
	out := map[string]any{
		"generated_at": s.now().UTC(),
		"month_start":  monthStart.Format(time.RFC3339),
		"spent_cents":  spent,
		"caps":         caps,
	}
	writeJSON(w, http.StatusOK, out)
}

type geographyExpansionRequest struct {
	Geography  string
	Country    string
	Region     string
	Mode       string
	Categories []string
	Priority   string
}

func mailboxReviewType(raw json.RawMessage) string {
	var obj map[string]any
	if len(raw) == 0 || !json.Valid(raw) {
		return ""
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	for _, key := range []string{"review_type", "kind", "mailbox_type", "subtype"} {
		val := strings.ToLower(strings.TrimSpace(asString(obj[key])))
		if val != "" {
			return val
		}
	}
	return ""
}

func isGeographyExpansionMailbox(item runtimetools.MailboxItem) bool {
	t := strings.ToLower(strings.TrimSpace(item.Type))
	if t == "" {
		return false
	}
	switch t {
	case "domain_approval", "geography_expansion", "vertical_geography_expansion", "expansion_recommendation":
		return true
	}
	if strings.Contains(t, "geography") && strings.Contains(t, "expansion") {
		return true
	}
	if t == "review" {
		rt := mailboxReviewType(item.Context)
		switch rt {
		case "domain_approval", "geography_expansion", "vertical_geography_expansion", "expansion_recommendation":
			return true
		}
	}
	return false
}

func isFounderInputMailbox(item runtimetools.MailboxItem) bool {
	t := strings.ToLower(strings.TrimSpace(item.Type))
	if t == "founder_input" {
		return true
	}
	return t == "review" && mailboxReviewType(item.Context) == "founder_input"
}

func parseGeographyExpansionRequest(raw json.RawMessage) geographyExpansionRequest {
	out := geographyExpansionRequest{
		Mode:     "local_services",
		Priority: "normal",
	}
	var obj map[string]any
	if len(raw) > 0 && json.Valid(raw) {
		_ = json.Unmarshal(raw, &obj)
	}
	lookup := func(keys ...string) string {
		for _, k := range keys {
			if obj == nil {
				continue
			}
			if v := strings.TrimSpace(asString(obj[k])); v != "" && v != "null" {
				return v
			}
		}
		return ""
	}
	out.Geography = lookup("geography", "target_geography", "geography_name")
	out.Country = lookup("country", "country_code")
	out.Region = lookup("region")
	if mode := strings.ToLower(lookup("mode")); mode != "" {
		out.Mode = mode
	}
	if priority := strings.ToLower(lookup("priority")); priority != "" {
		out.Priority = priority
	}
	if cats := parseStringList(anyFrom(obj, "categories", "taxonomy_categories")); len(cats) > 0 {
		out.Categories = cats
	}
	if out.Country == "" && strings.Contains(out.Geography, ",") {
		parts := strings.Split(out.Geography, ",")
		out.Country = strings.TrimSpace(parts[len(parts)-1])
	}
	if out.Country == "" {
		out.Country = "unspecified"
	}
	return out
}

func ensureGeographyRecord(ctx context.Context, db *sql.DB, req geographyExpansionRequest) (string, error) {
	if db == nil {
		return "", fmt.Errorf("postgres db is required")
	}
	name := strings.TrimSpace(req.Geography)
	country := strings.TrimSpace(req.Country)
	region := strings.TrimSpace(req.Region)

	var id string
	err := db.QueryRowContext(ctx, `
		SELECT id::text
		FROM geographies
		WHERE lower(name) = lower($1)
		  AND ($2 = '' OR lower(country) = lower($2))
		ORDER BY created_at DESC
		LIMIT 1
	`, name, country).Scan(&id)
	if err == nil && strings.TrimSpace(id) != "" {
		return id, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("lookup geography %q: %w", name, err)
	}

	id = uuid.NewString()
	scanCfg := mustJSON(map[string]any{
		"source":      "mailbox.geography_expansion",
		"mode":        req.Mode,
		"categories":  req.Categories,
		"priority":    req.Priority,
		"geography":   name,
		"country":     country,
		"region":      region,
		"recorded_at": time.Now().UTC().Format(time.RFC3339),
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, scan_config, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,''), $5::jsonb, now())
	`, id, name, country, region, string(scanCfg)); err != nil {
		return "", fmt.Errorf("insert geography %q: %w", name, err)
	}
	return id, nil
}

func anyFrom(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if m == nil {
			continue
		}
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
}

func parseStringList(v any) []string {
	normalize := func(in []string) []string {
		seen := make(map[string]struct{}, len(in))
		out := make([]string, 0, len(in))
		for _, raw := range in {
			s := strings.TrimSpace(raw)
			if s == "" {
				continue
			}
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
		return out
	}
	switch t := v.(type) {
	case []string:
		return normalize(t)
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			s := strings.TrimSpace(asString(x))
			if s != "" && s != "null" {
				out = append(out, s)
			}
		}
		return normalize(out)
	case string:
		parts := strings.Split(t, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			s := strings.TrimSpace(p)
			if s != "" {
				out = append(out, s)
			}
		}
		return normalize(out)
	default:
		return nil
	}
}

func allowMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		w.Header().Set("allow", method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func decodeJSONBody(r *http.Request, out any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("invalid json body: %w", err)
	}
	return nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func parseInt(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func parseBoolQuery(raw string, fallback bool) bool {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func isMissingRuntimeLogTable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") && strings.Contains(msg, "runtime_log")
}

type runtimeErrorMetadata struct {
	Code      string
	Component string
	Operation string
	Retryable *bool
}

func parseRuntimeErrorMetadata(raw string) runtimeErrorMetadata {
	text := strings.TrimSpace(raw)
	if text == "" || !strings.HasPrefix(text, "runtime_error") {
		return runtimeErrorMetadata{}
	}
	metadata := text
	if idx := strings.Index(metadata, ":"); idx >= 0 {
		metadata = strings.TrimSpace(metadata[:idx])
	}
	parts := strings.Fields(metadata)
	if len(parts) == 0 || parts[0] != "runtime_error" {
		return runtimeErrorMetadata{}
	}
	out := runtimeErrorMetadata{}
	for _, token := range parts[1:] {
		kv := strings.SplitN(token, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		if key == "" || val == "" {
			continue
		}
		switch key {
		case "code":
			out.Code = val
		case "component":
			out.Component = val
		case "operation":
			out.Operation = val
		case "retryable":
			if parsed, err := strconv.ParseBool(strings.ToLower(val)); err == nil {
				parsedBool := parsed
				out.Retryable = &parsedBool
			}
		}
	}
	return out
}

func classifyIncidentRootCause(code string) string {
	code = strings.TrimSpace(strings.ToLower(code))
	switch code {
	case "mcp_context_token_missing", "mcp_context_token_not_found", "mcp_context_token_stale_epoch":
		return "mcp_context_lifecycle"
	case "mcp_auth_missing_bearer", "mcp_auth_invalid_bearer":
		return "mcp_gateway_auth"
	case "mcp_tool_not_allowed":
		return "mcp_tool_allowlist"
	case "mcp_tool_execution_failed":
		return "tool_execution_failure"
	case "mcp_stall_detected":
		return "agent_stall_detected"
	default:
		if strings.HasPrefix(code, "mcp_") {
			return "mcp_unknown"
		}
		return "runtime_unknown"
	}
}

func mapKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		if strings.TrimSpace(k) == "" {
			continue
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	default:
		return 0
	}
}

func truncate(v string, max int) string {
	if max <= 0 || len(v) <= max {
		return v
	}
	return v[:max] + "..."
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	case nil:
		return ""
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func asFloatAny(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case int32:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f
	default:
		return 0
	}
}

func boolFromAny(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(strings.ToLower(t)))
		return err == nil && parsed
	case int:
		return t != 0
	case int32:
		return t != 0
	case int64:
		return t != 0
	case float32:
		return t != 0
	case float64:
		return t != 0
	default:
		return false
	}
}

func coalesce(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
