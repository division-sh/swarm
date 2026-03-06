package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"empireai/internal/events"
	mailboxsvc "empireai/internal/mailbox"
	"empireai/internal/models"
	"empireai/internal/runtime"
	"empireai/internal/specaudit"
	"empireai/internal/store"
	"empireai/internal/templateops"
	"github.com/google/uuid"
)

type controlTarget struct {
	AgentID      string `json:"agent_id"`
	Role         string `json:"role"`
	VerticalID   string `json:"vertical_id"`
	VerticalSlug string `json:"vertical_slug"`
	Status       string `json:"status"`
}

func (s *Server) handleControlTargets(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			a.id,
			COALESCE(a.role, ''),
			COALESCE(a.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			COALESCE(a.status, '')
		FROM agents a
		LEFT JOIN verticals v ON v.id = a.vertical_id
		WHERE COALESCE(a.status, '') <> 'terminated'
		ORDER BY a.mode ASC, a.role ASC, a.id ASC
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()
	out := make([]controlTarget, 0, 32)
	for rows.Next() {
		var t controlTarget
		if err := rows.Scan(&t.AgentID, &t.Role, &t.VerticalID, &t.VerticalSlug, &t.Status); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"targets": out, "generated_at": s.now().UTC()})
}

func (s *Server) handleControlSeedOrg(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.manager == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
		return
	}

	templateErr := s.ensureInitialTemplate(r.Context())

	agentsDir := strings.TrimSpace(os.Getenv("EMPIREAI_GLOBAL_AGENTS_DIR"))
	if agentsDir == "" {
		agentsDir = "configs/agents"
	}
	specs, loadErr := templateops.LoadGlobalAgentsFromYAML(agentsDir)
	if loadErr != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("load global agents from YAML: %w", loadErr))
		return
	}
	created := make([]string, 0, len(specs))
	skipped := make([]string, 0, len(specs))
	errs := make([]string, 0)
	for _, cfg := range specs {
		if _, ok := s.manager.GetAgentConfig(cfg.ID); ok {
			skipped = append(skipped, cfg.ID)
			continue
		}
		if err := s.manager.SpawnAgent(cfg); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "already exists") {
				skipped = append(skipped, cfg.ID)
				continue
			}
			errs = append(errs, fmt.Sprintf("%s: %v", cfg.ID, err))
			continue
		}
		created = append(created, cfg.ID)
	}

	// Emit canonical system.started if not already present, so directives and agents work.
	if s.eventStore != nil && s.db != nil {
		var exists bool
		if err := s.db.QueryRowContext(r.Context(), `SELECT EXISTS(SELECT 1 FROM events WHERE type = 'system.started')`).Scan(&exists); err == nil && !exists {
			payload := s.buildSystemStartedPayload(r.Context())
			startEvt := events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("system.started"),
				SourceAgent: "runtime",
				Payload:     mustJSON(payload),
				CreatedAt:   time.Now(),
			}
			_ = s.eventStore.AppendEvent(r.Context(), startEvt)
		}
	}

	status := http.StatusOK
	ok := true
	if len(errs) > 0 {
		ok = false
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, map[string]any{
		"ok":            ok,
		"created":       created,
		"skipped":       skipped,
		"errors":        errs,
		"total":         len(specs),
		"agents_source": "yaml",
		"template": func() any {
			if templateErr == nil {
				return map[string]any{"ok": true}
			}
			return map[string]any{"ok": false, "error": templateErr.Error()}
		}(),
	})
}

func (s *Server) ensureInitialTemplate(ctx context.Context) error {
	if s.db == nil {
		return nil
	}
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM org_templates)`).Scan(&exists); err != nil {
		return fmt.Errorf("check org_templates: %w", err)
	}
	if exists {
		return nil
	}

	version := strings.TrimSpace(os.Getenv("EMPIREAI_INITIAL_TEMPLATE_VERSION"))
	if version == "" {
		version = "2.0.14"
	}
	agentsDir := strings.TrimSpace(os.Getenv("EMPIREAI_TEMPLATE_AGENTS_DIR"))
	if agentsDir == "" {
		agentsDir = "configs/agents/templates"
	}
	routesYAML := strings.TrimSpace(os.Getenv("EMPIREAI_TEMPLATE_ROUTES_YAML"))
	if routesYAML == "" {
		routesYAML = "configs/agents/templates/routes.yaml"
	}

	agents, bootstrap, seeded, err := templateops.CompileTemplateFromYAML(agentsDir, routesYAML)
	if err != nil {
		return err
	}
	env := mustJSON(map[string]any{
		"version":          version,
		"agents":           json.RawMessage(agents),
		"bootstrap_routes": json.RawMessage(bootstrap),
		"seeded_routes":    json.RawMessage(seeded),
	})
	if res := specaudit.Validate("template", env); !res.Passed {
		return fmt.Errorf("initial template failed spec audit issues=%d", len(res.Issues))
	}

	svc := templateops.NewService(s.db, s.mailboxStore)
	if err := svc.PublishTemplate(ctx, version, agents, bootstrap, seeded, "seed-org", "initial template (auto-published)"); err != nil {
		return err
	}
	return nil
}

func (s *Server) handleControlCreateVertical(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.manager == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
		return
	}
	var req struct {
		Name      string `json:"name"`
		Geography string `json:"geography"`
		Slug      string `json:"slug"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Geography = strings.TrimSpace(req.Geography)
	req.Slug = strings.TrimSpace(req.Slug)
	if req.Name == "" || req.Geography == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("name and geography are required"))
		return
	}

	verticalID := uuid.NewString()
	slug, err := s.resolveUniqueVerticalSlug(r.Context(), req.Slug, req.Name, verticalID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if _, err := s.db.ExecContext(r.Context(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, $2, $3, $4, 'operating', 'operating', now(), now())
	`, verticalID, req.Name, slug, req.Geography); err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("insert vertical: %w", err))
		return
	}

	if err := s.manager.SpawnOpCo(verticalID, models.MandateDocument{VerticalID: verticalID}); err != nil {
		_, _ = s.db.ExecContext(r.Context(), `DELETE FROM verticals WHERE id = $1::uuid`, verticalID)
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("spawn opco: %w", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"vertical_id": verticalID,
		"slug":        slug,
		"name":        req.Name,
		"geography":   req.Geography,
	})
}

func (s *Server) handleControlAgentRestart(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.manager == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
		return
	}
	var req struct {
		AgentID string `json:"agent_id"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	if req.AgentID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("agent_id is required"))
		return
	}
	if err := s.manager.RestartAgent(req.AgentID); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "agent_id": req.AgentID, "action": "restart"})
}

func (s *Server) handleControlAgentReplay(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.manager == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
		return
	}
	var req struct {
		AgentID string `json:"agent_id"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	if req.AgentID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("agent_id is required"))
		return
	}
	if err := s.manager.ReplayAgentBacklog(r.Context(), req.AgentID); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "agent_id": req.AgentID, "action": "replay"})
}

func (s *Server) handleControlEventRequeue(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		EventID string `json:"event_id"`
		AgentID string `json:"agent_id"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.EventID = strings.TrimSpace(req.EventID)
	req.AgentID = strings.TrimSpace(req.AgentID)
	if req.EventID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("event_id is required"))
		return
	}
	if req.AgentID == "" {
		rows, err := s.db.QueryContext(r.Context(), `SELECT agent_id FROM event_deliveries WHERE event_id = $1::uuid`, req.EventID)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		defer rows.Close()
		recipients := make([]string, 0, 16)
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				recipients = append(recipients, id)
			}
		}
		if len(recipients) == 0 {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("no deliveries found for event_id %s", req.EventID))
			return
		}
		if _, err := s.db.ExecContext(r.Context(), `DELETE FROM event_receipts WHERE event_id = $1::uuid`, req.EventID); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if s.manager != nil {
			for _, id := range recipients {
				_ = s.manager.ReplayAgentBacklog(r.Context(), id)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":        true,
			"event_id":  req.EventID,
			"agent_ids": recipients,
			"requeued":  len(recipients),
			"action":    "requeue_event_all",
		})
		return
	}

	if _, err := s.db.ExecContext(r.Context(), `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, agent_id) DO NOTHING
	`, req.EventID, req.AgentID); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if _, err := s.db.ExecContext(r.Context(), `
		DELETE FROM event_receipts WHERE event_id = $1::uuid AND agent_id = $2
	`, req.EventID, req.AgentID); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if s.manager != nil {
		_ = s.manager.ReplayAgentBacklog(r.Context(), req.AgentID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"event_id": req.EventID,
		"agent_id": req.AgentID,
		"action":   "requeue_event_single",
	})
}

func (s *Server) handleControlRuntime(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		Action   string `json:"action"`
		Confirm  string `json:"confirm"`
		SeedOrg  *bool  `json:"seed_org"`
		Template string `json:"template_version"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.Action = strings.TrimSpace(strings.ToLower(req.Action))
	switch req.Action {
	case "pause":
		if s.manager == nil {
			writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
			return
		}
		if err := s.manager.Shutdown(); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		runtime.PauseRuntimeIngress()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":             true,
			"action":         "pause",
			"running":        s.manager.IsRunning(),
			"ingress_paused": runtime.RuntimeIngressPaused(),
		})
	case "resume":
		if s.manager == nil {
			writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
			return
		}
		runtime.ResumeRuntimeIngress()
		s.manager.Run(context.Background())
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":             true,
			"action":         "resume",
			"running":        s.manager.IsRunning(),
			"ingress_paused": runtime.RuntimeIngressPaused(),
		})
	case "reset_state":
		if strings.TrimSpace(req.Confirm) != "RESET" {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("confirmation required: set confirm=RESET"))
			return
		}
		resetEpoch := runtime.EnterRuntimeResetMode()
		defer runtime.ExitRuntimeResetMode()
		// Match reset_db ordering to avoid writes racing with truncate.
		if s.manager != nil {
			if err := s.manager.ResetRuntimeState(); err != nil {
				writeErr(w, http.StatusInternalServerError, fmt.Errorf("reset runtime state: %w", err))
				return
			}
		}
		if err := s.resetState(r.Context()); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if s.manager != nil {
			if err := s.publishRuntimeReset(r.Context(), "dashboard_reset_state"); err != nil {
				writeErr(w, http.StatusInternalServerError, fmt.Errorf("publish runtime reset event: %w", err))
				return
			}
			s.manager.Run(context.Background())
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "action": "reset_state", "runtime_epoch": resetEpoch})
	case "reset_db":
		if strings.TrimSpace(req.Confirm) != "RESET" {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("confirmation required: set confirm=RESET"))
			return
		}
		if s.db == nil {
			writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("db unavailable"))
			return
		}
		if s.manager == nil {
			writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
			return
		}
		resetEpoch := runtime.EnterRuntimeResetMode()
		defer runtime.ExitRuntimeResetMode()

		// Stop agent loops and clear in-memory state before we truncate the DB.
		if err := s.manager.ResetRuntimeState(); err != nil {
			writeErr(w, http.StatusInternalServerError, fmt.Errorf("reset runtime state: %w", err))
			return
		}

		if err := s.resetState(r.Context()); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if err := s.publishRuntimeReset(r.Context(), "dashboard_reset_db"); err != nil {
			writeErr(w, http.StatusInternalServerError, fmt.Errorf("publish runtime reset event: %w", err))
			return
		}

		// Optional: allow callers to force a specific initial template version.
		if v := strings.TrimSpace(req.Template); v != "" {
			_ = os.Setenv("EMPIREAI_INITIAL_TEMPLATE_VERSION", v)
		}
		templateErr := s.ensureInitialTemplate(r.Context())

		seedOrg := true
		if req.SeedOrg != nil {
			seedOrg = *req.SeedOrg
		}

		created := []string{}
		skipped := []string{}
		errs := []string{}
		if seedOrg {
			agentsDir := strings.TrimSpace(os.Getenv("EMPIREAI_GLOBAL_AGENTS_DIR"))
			if agentsDir == "" {
				agentsDir = "configs/agents"
			}
			specs, loadErr := templateops.LoadGlobalAgentsFromYAML(agentsDir)
			if loadErr != nil {
				writeErr(w, http.StatusInternalServerError, fmt.Errorf("load global agents from YAML: %w", loadErr))
				return
			}
			for _, cfg := range specs {
				if cfg.ID == "" {
					continue
				}
				if err := s.manager.SpawnAgent(cfg); err != nil {
					if strings.Contains(strings.ToLower(err.Error()), "already exists") {
						skipped = append(skipped, cfg.ID)
						continue
					}
					errs = append(errs, fmt.Sprintf("%s: %v", cfg.ID, err))
					continue
				}
				created = append(created, cfg.ID)
			}
		}

		s.manager.Run(context.Background())

		// Emit canonical system.started so agents (and directive guard) know the system is live.
		if s.eventStore != nil {
			payload := s.buildSystemStartedPayload(r.Context())
			startEvt := events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("system.started"),
				SourceAgent: "runtime",
				Payload:     mustJSON(payload),
				CreatedAt:   time.Now(),
			}
			_ = s.eventStore.AppendEvent(r.Context(), startEvt)
		}

		status := http.StatusOK
		ok := true
		if len(errs) > 0 {
			ok = false
			status = http.StatusMultiStatus
		}
		writeJSON(w, status, map[string]any{
			"ok":            ok,
			"action":        "reset_db",
			"runtime_epoch": resetEpoch,
			"seeded":        seedOrg,
			"created":       created,
			"skipped":       skipped,
			"errors":        errs,
			"agents_source": "yaml",
			"template": func() any {
				if templateErr == nil {
					return map[string]any{"ok": true}
				}
				return map[string]any{"ok": false, "error": templateErr.Error()}
			}(),
		})
	default:
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid action: %s (expected pause|resume|reset_state|reset_db)", req.Action))
	}
}

func (s *Server) resetState(ctx context.Context) error {
	if s.db == nil {
		return fmt.Errorf("db unavailable")
	}

	// Full reset should automatically include new runtime tables without requiring
	// manual updates to a hardcoded list.
	rows, err := s.db.QueryContext(ctx, `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public'
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Keep migration bookkeeping intact.
	excluded := map[string]struct{}{
		"schema_version": {},
	}

	tables := make([]string, 0, 64)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, skip := excluded[name]; skip {
			continue
		}
		tables = append(tables, "public."+quoteIdent(name))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	sort.Strings(tables)
	q := "TRUNCATE TABLE " + strings.Join(tables, ", ") + " RESTART IDENTITY CASCADE"
	_, err = s.db.ExecContext(ctx, q)
	return err
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (s *Server) publishRuntimeReset(ctx context.Context, source string) error {
	if s.manager == nil {
		return nil
	}
	payload := map[string]any{
		"source":    strings.TrimSpace(source),
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	return s.manager.PublishEvent(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("runtime.reset"),
		SourceAgent: "runtime",
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	})
}

func (s *Server) buildSystemStartedPayload(ctx context.Context) map[string]any {
	out := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	if s.db == nil {
		return out
	}

	agentCount := 0
	verticalCount := 0
	geoCount := 0
	previousStarts := 0
	templateVersion := ""

	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents WHERE COALESCE(status,'') <> 'terminated'`).Scan(&agentCount)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM verticals`).Scan(&verticalCount)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM geographies`).Scan(&geoCount)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type = 'system.started'`).Scan(&previousStarts)
	_ = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(version,'')
		FROM org_templates
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&templateVersion)

	out["agent_count"] = agentCount
	out["template_version"] = strings.TrimSpace(templateVersion)
	out["is_cold_start"] = previousStarts == 0 && verticalCount == 0 && geoCount == 0
	out["startup_count"] = previousStarts + 1
	return out
}

func (s *Server) resolveUniqueVerticalSlug(ctx context.Context, requested, name, verticalID string) (string, error) {
	base := sanitizeSlug(requested)
	if base == "" {
		base = sanitizeSlug(name)
	}
	if base == "" {
		base = "vertical"
	}
	candidates := []string{
		base,
		fmt.Sprintf("%s-%s", base, verticalID[:8]),
		fmt.Sprintf("%s-%s", base, verticalID[:4]),
	}
	for _, slug := range candidates {
		if slug == "" {
			continue
		}
		var exists int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM verticals WHERE slug = $1`, slug).Scan(&exists); err != nil {
			return "", err
		}
		if exists == 0 {
			return slug, nil
		}
	}
	return fmt.Sprintf("%s-%d", base, time.Now().Unix()), nil
}

func sanitizeSlug(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 54 {
		out = strings.Trim(out[:54], "-")
	}
	return out
}

func (s *Server) handleControlDirective(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		AgentID string `json:"agent_id"`
		Message string `json:"message"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	req.Message = strings.TrimSpace(req.Message)
	if req.AgentID == "" || req.Message == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("agent_id and message are required"))
		return
	}
	target, err := s.lookupControlTarget(r.Context(), req.AgentID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var eventID string
	if strings.EqualFold(strings.TrimSpace(target.AgentID), "empire-coordinator") {
		started, serr := s.hasSystemStarted(r.Context())
		if serr != nil {
			writeErr(w, http.StatusInternalServerError, serr)
			return
		}
		if !started {
			writeErr(w, http.StatusConflict, fmt.Errorf("system is not initialized yet (missing system.started); run `empire init` first"))
			return
		}
		eventID, err = s.queueSystemDirective(r.Context(), req.Message, "dashboard")
	} else {
		eventID, err = s.queueBoardMessage(r.Context(), target, events.EventType("board.directive"), req.Message)
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"event_id": eventID,
		"target":   target,
	})
}

func (s *Server) handleControlChat(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		AgentID string `json:"agent_id"`
		Message string `json:"message"`
		Mode    string `json:"mode"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	req.Message = strings.TrimSpace(req.Message)
	req.Mode = strings.TrimSpace(strings.ToLower(req.Mode))
	if req.AgentID == "" || req.Message == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("agent_id and message are required"))
		return
	}
	if req.Mode == "" {
		req.Mode = "live"
	}
	target, err := s.lookupControlTarget(r.Context(), req.AgentID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	eventID, err := s.queueBoardMessage(r.Context(), target, events.EventType("board.chat"), req.Message)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	resp := map[string]any{
		"ok":       true,
		"mode":     req.Mode,
		"event_id": eventID,
		"target":   target,
	}
	if req.Mode != "async" {
		if s.manager == nil {
			resp["warning"] = "manager unavailable; message queued async"
		} else {
			reply, chatErr := s.manager.ChatWithAgent(r.Context(), req.AgentID, req.Message)
			if chatErr != nil {
				resp["chat_error"] = chatErr.Error()
			} else {
				resp["response"] = strings.TrimSpace(reply)

				// Live chat bypasses the event loop via ChatWithAgent, but we still emit
				// the board.chat event for traceability. Mark the delivery processed so
				// it doesn't linger as a pending delivery and get replayed later.
				_ = s.upsertEventReceipt(r.Context(), eventID, req.AgentID, "processed", "")
			}
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) upsertEventReceipt(ctx context.Context, eventID, agentID, status, errText string) error {
	if s.db == nil {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	status = strings.TrimSpace(status)
	if eventID == "" || agentID == "" || status == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count, error)
		VALUES ($1::uuid, $2, now(), $3, 0, NULLIF($4,''))
		ON CONFLICT (event_id, agent_id) DO UPDATE
			SET processed_at = now(),
				status = EXCLUDED.status,
				error = EXCLUDED.error
	`, eventID, agentID, status, strings.TrimSpace(errText))
	return err
}

func (s *Server) handleControlMailboxDecide(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.mailboxStore == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("mailbox store unavailable"))
		return
	}
	var req struct {
		MailboxID string `json:"mailbox_id"`
		Action    string `json:"action"`
		Notes     string `json:"notes"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.MailboxID = strings.TrimSpace(req.MailboxID)
	req.Action = strings.TrimSpace(req.Action)
	if req.MailboxID == "" || req.Action == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("mailbox_id and action are required"))
		return
	}

	item, err := s.mailboxStore.GetMailboxItem(r.Context(), req.MailboxID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	outcome, err := mailboxsvc.Decide(r.Context(), s.mailboxStore, req.MailboxID, req.Action, req.Notes)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.emitMailboxDecisionSideEffects(r.Context(), item, outcome, req.Notes); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"id":       req.MailboxID,
		"status":   outcome.Status,
		"decision": outcome.Decision,
	})
}

func (s *Server) lookupControlTarget(ctx context.Context, agentID string) (controlTarget, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return controlTarget{}, fmt.Errorf("agent_id is required")
	}
	var t controlTarget
	err := s.db.QueryRowContext(ctx, `
		SELECT
			a.id,
			COALESCE(a.role, ''),
			COALESCE(a.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			COALESCE(a.status, '')
		FROM agents a
		LEFT JOIN verticals v ON v.id = a.vertical_id
		WHERE a.id = $1
	`, agentID).Scan(&t.AgentID, &t.Role, &t.VerticalID, &t.VerticalSlug, &t.Status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return controlTarget{}, fmt.Errorf("agent not found: %s", agentID)
		}
		return controlTarget{}, err
	}
	if strings.EqualFold(t.Status, "terminated") {
		return controlTarget{}, fmt.Errorf("agent is terminated: %s", agentID)
	}
	return t, nil
}

func (s *Server) queueBoardMessage(ctx context.Context, target controlTarget, eventType events.EventType, message string) (string, error) {
	if s.eventStore == nil {
		return "", fmt.Errorf("event store unavailable")
	}
	payload := map[string]any{
		"target_agent_id": target.AgentID,
		"role":            target.Role,
		"vertical_id":     target.VerticalID,
		"vertical_key":    target.VerticalSlug,
		"message":         strings.TrimSpace(message),
		"sent_by":         "dashboard",
		"sent_at":         s.now().UTC().Format(time.RFC3339),
	}
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        eventType,
		SourceAgent: "dashboard",
		VerticalID:  target.VerticalID,
		Payload:     mustJSON(payload),
		CreatedAt:   s.now(),
	}
	if err := s.eventStore.AppendEvent(ctx, evt); err != nil {
		return "", err
	}
	if err := s.eventStore.InsertEventDeliveries(ctx, evt.ID, []string{target.AgentID}); err != nil {
		return "", err
	}
	return evt.ID, nil
}

func (s *Server) queueSystemDirective(ctx context.Context, message, sentBy string) (string, error) {
	if s.eventStore == nil {
		return "", fmt.Errorf("event store unavailable")
	}
	if s.manager == nil {
		return "", fmt.Errorf("runtime manager unavailable")
	}
	msg := strings.TrimSpace(message)
	if msg == "" {
		return "", fmt.Errorf("directive_text is required")
	}
	payload := mustJSON(map[string]any{
		"directive_text": msg,
		"timestamp":      s.now().UTC().Format(time.RFC3339),
		"sent_by":        strings.TrimSpace(coalesce(sentBy, "human")),
	})
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     payload,
		CreatedAt:   s.now(),
	}
	if err := s.manager.PublishEvent(ctx, evt); err != nil {
		return "", err
	}
	return evt.ID, nil
}

func (s *Server) hasSystemStarted(ctx context.Context) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("database unavailable")
	}
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE type = 'system.started')`).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *Server) emitMailboxDecisionSideEffects(
	ctx context.Context,
	item runtime.MailboxItem,
	outcome mailboxsvc.DecisionOutcome,
	notes string,
) error {
	if s.eventStore == nil {
		return nil
	}
	basePayload := map[string]any{
		"mailbox_id": item.ID,
		"type":       item.Type,
		"status":     outcome.Status,
		"decision":   outcome.Decision,
		"notes":      notes,
		"context":    json.RawMessage(item.Context),
	}
	if err := s.appendTargetedEvent(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("mailbox.decision"),
		SourceAgent: "mailbox",
		VerticalID:  item.VerticalID,
		Payload:     mustJSON(basePayload),
		CreatedAt:   s.now(),
	}, nil); err != nil {
		return err
	}
	if err := s.appendTargetedEvent(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("mailbox.item_decided"),
		SourceAgent: "mailbox",
		VerticalID:  item.VerticalID,
		Payload:     mustJSON(basePayload),
		CreatedAt:   s.now(),
	}, nil); err != nil {
		return err
	}
	if outcome.Status == "more_data" && item.VerticalID != "" {
		if err := s.appendTargetedEvent(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.needs_more_data"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload:     mustJSON(basePayload),
			CreatedAt:   s.now(),
		}, []string{"empire-coordinator"}); err != nil {
			return err
		}
	}
	if item.Type == "vertical_approval" && item.VerticalID != "" {
		var evtType events.EventType
		switch outcome.Status {
		case "approved":
			evtType = events.EventType("vertical.approved")
		case "rejected":
			evtType = events.EventType("vertical.killed")
		}
		if evtType != "" {
			if err := s.appendTargetedEvent(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        evtType,
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload:     mustJSON(basePayload),
				CreatedAt:   s.now(),
			}, []string{"empire-coordinator"}); err != nil {
				return err
			}
		}
	}
	if item.Type == "spend_request" || item.Type == "budget_increase" || item.Type == "devops.capacity_warning" {
		var evtType events.EventType
		if outcome.Status == "approved" {
			evtType = events.EventType("spend.approved")
		}
		if outcome.Status == "rejected" {
			evtType = events.EventType("spend.rejected")
		}
		if evtType != "" {
			recipients := []string{}
			if item.VerticalID != "" {
				recipients = []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}
			} else if strings.TrimSpace(item.FromAgent) != "" {
				recipients = []string{strings.TrimSpace(item.FromAgent)}
			}
			if err := s.appendTargetedEvent(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        evtType,
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload:     mustJSON(basePayload),
				CreatedAt:   s.now(),
			}, recipients); err != nil {
				return err
			}
		}
	}
	if isFounderInputMailbox(item) && item.VerticalID != "" {
		if err := s.appendTargetedEvent(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("founder_input.response"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload:     mustJSON(basePayload),
			CreatedAt:   s.now(),
		}, []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}); err != nil {
			return err
		}
	}

	// Spec v2.0 GAP 1: escalation responses are open-ended directives back to the OpCo CEO.
	if item.VerticalID != "" && strings.Contains(strings.ToLower(item.Type), "escalation") && outcome.Status == "approved" {
		directive := strings.TrimSpace(notes)
		if directive != "" {
			if err := s.appendTargetedEvent(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("opco.escalation_response"),
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload: mustJSON(map[string]any{
					"mailbox_id":     item.ID,
					"directive_text": directive,
					"action_items":   []any{},
					"context":        json.RawMessage(item.Context),
				}),
				CreatedAt: s.now(),
			}, []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}); err != nil {
				return err
			}
		}
	}

	// Spec v2.0 §7.6: approved geography expansion recommendations must queue
	// a validation scan campaign.
	if outcome.Status == "approved" && isGeographyExpansionMailbox(item) {
		geoID, req, campaignID, err := s.queueGeographyExpansionValidation(ctx, item)
		if err != nil {
			return err
		}
		if err := s.appendTargetedEvent(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("geography.expansion_queued"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload: mustJSON(map[string]any{
				"mailbox_id":   item.ID,
				"vertical_id":  item.VerticalID,
				"geography_id": geoID,
				"geography":    req.Geography,
				"country":      req.Country,
				"region":       req.Region,
				"mode":         req.Mode,
				"categories":   req.Categories,
				"priority":     req.Priority,
				"campaign_id":  campaignID,
				"context":      json.RawMessage(item.Context),
			}),
			CreatedAt: s.now(),
		}, []string{"empire-coordinator"}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) queueGeographyExpansionValidation(
	ctx context.Context,
	item runtime.MailboxItem,
) (string, geographyExpansionRequest, string, error) {
	req := parseGeographyExpansionRequest(item.Context)
	if strings.TrimSpace(req.Geography) == "" {
		return "", req, "", fmt.Errorf("geography expansion requires context.geography")
	}
	if s.db == nil {
		return "", req, "", fmt.Errorf("geography expansion requires postgres db")
	}
	geoID, err := ensureGeographyRecord(ctx, s.db, req)
	if err != nil {
		return "", req, "", err
	}
	pg := &store.PostgresStore{DB: s.db}
	campaign, err := pg.CreateScanCampaign(ctx, runtime.CreateScanCampaignInput{
		GeographyID: geoID,
		Mode:        req.Mode,
		Categories:  req.Categories,
		Priority:    req.Priority,
		Status:      "queued",
	})
	if err != nil {
		return "", req, "", fmt.Errorf("queue geography expansion scan campaign: %w", err)
	}
	return geoID, req, campaign.ID, nil
}

func (s *Server) appendTargetedEvent(ctx context.Context, evt events.Event, recipients []string) error {
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = s.now()
	}
	if len(evt.Payload) == 0 {
		evt.Payload = []byte("{}")
	}
	if err := s.eventStore.AppendEvent(ctx, evt); err != nil {
		return err
	}
	if len(recipients) == 0 {
		return nil
	}
	recipients = s.filterExistingRecipients(ctx, recipients)
	if len(recipients) == 0 {
		return nil
	}
	return s.eventStore.InsertEventDeliveries(ctx, evt.ID, recipients)
}

func (s *Server) filterExistingRecipients(ctx context.Context, recipients []string) []string {
	if s.db == nil || len(recipients) == 0 {
		return recipients
	}
	exists := map[string]struct{}{}
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM agents`)
	if err != nil {
		return recipients
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			exists[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(recipients))
	for _, id := range recipients {
		if _, ok := exists[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	key := strings.TrimSpace(os.Getenv("EMPIREAI_API_KEY"))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Dashboard page + assets are always accessible locally; APIs require a key.
		if strings.HasPrefix(r.URL.Path, "/dashboard/api/") || strings.HasPrefix(r.URL.Path, "/api/") {
			if key == "" {
				writeErr(w, http.StatusInternalServerError, fmt.Errorf("EMPIREAI_API_KEY is not set"))
				return
			}
			supplied := strings.TrimSpace(r.Header.Get("X-Empire-Key"))
			if supplied == "" {
				// SSE EventSource can't set headers; allow query param fallback.
				supplied = strings.TrimSpace(r.URL.Query().Get("key"))
			}
			if supplied == "" || supplied != key {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// API surface (§14.7). These handlers are thin aliases over the dashboard store layer.
