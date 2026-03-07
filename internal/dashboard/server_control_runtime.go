package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	"empireai/internal/templateops"
	"github.com/google/uuid"
)

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
		runtimebus.PauseRuntimeIngress()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":             true,
			"action":         "pause",
			"running":        s.manager.IsRunning(),
			"ingress_paused": runtimebus.RuntimeIngressPaused(),
		})
	case "resume":
		if s.manager == nil {
			writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
			return
		}
		runtimebus.ResumeRuntimeIngress()
		s.manager.Run(context.Background())
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":             true,
			"action":         "resume",
			"running":        s.manager.IsRunning(),
			"ingress_paused": runtimebus.RuntimeIngressPaused(),
		})
	case "reset_state":
		if strings.TrimSpace(req.Confirm) != "RESET" {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("confirmation required: set confirm=RESET"))
			return
		}
		resetEpoch := runtimebus.EnterRuntimeResetMode()
		defer runtimebus.ExitRuntimeResetMode()
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
		resetEpoch := runtimebus.EnterRuntimeResetMode()
		defer runtimebus.ExitRuntimeResetMode()

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
