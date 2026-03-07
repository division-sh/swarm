package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/specaudit"
	"empireai/internal/templateops"
)

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
