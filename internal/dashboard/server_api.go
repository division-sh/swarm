package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"empireai/internal/events"
	mailboxsvc "empireai/internal/mailbox"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/google/uuid"
)

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
