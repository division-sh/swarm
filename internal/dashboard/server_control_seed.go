package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"empireai/internal/events"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/specaudit"
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

	if s.manager != nil {
		if source := runtimepipeline.DefaultWorkflowSemanticSourceOrNil(); source != nil {
			if _, ok := source.FlowSchemaByID("operating"); ok {
				if err := s.manager.ActivateFlowInstance(r.Context(), runtimepipeline.FlowInstanceActivationRequest{
					ContractBundle: source,
					TemplateID:     "operating",
					InstanceID:     verticalID,
					VerticalID:     verticalID,
					FlowPath:       "operating/" + verticalID,
					InitialState:   "approved",
					Config: map[string]any{
						"vertical_name": req.Name,
						"geography":     req.Geography,
					},
				}); err != nil {
					_, _ = s.db.ExecContext(r.Context(), `DELETE FROM verticals WHERE id = $1::uuid`, verticalID)
					writeErr(w, http.StatusInternalServerError, fmt.Errorf("activate operating flow: %w", err))
					return
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"vertical_id": verticalID,
		"slug":        slug,
		"name":        req.Name,
		"geography":   req.Geography,
	})
}
