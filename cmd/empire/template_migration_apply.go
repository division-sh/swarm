package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"empireai/internal/events"
	"empireai/internal/runtime"
	models "empireai/internal/runtime/actors"
	runtimemanager "empireai/internal/runtime/manager"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/google/uuid"
)

// applyTemplateMigrationWithPrimitives executes a template migration plan using the runtime primitives
// (spawn/teardown/reconfigure/configure routing) and only bumps template_version at the end.
// This matches the v2.0 migration execution contract.
func applyTemplateMigrationWithPrimitives(ctx context.Context, cfgRuntimeMode string, stores storeBundle, migrationID, executedBy string) error {
	if stores.SQLDB == nil || stores.ManagerStore == nil || stores.EventStore == nil {
		return fmt.Errorf("template migration apply requires postgres stores")
	}
	migrationID = strings.TrimSpace(migrationID)
	if migrationID == "" {
		return fmt.Errorf("migration id is required")
	}
	executedBy = strings.TrimSpace(executedBy)
	if executedBy == "" {
		executedBy = defaultControlPlaneAgentID()
	}

	var verticalID, fromVersion, toVersion, status string
	var mailboxID sql.NullString
	var planRaw []byte
	if err := stores.SQLDB.QueryRowContext(ctx, `
		SELECT vertical_id::text, from_version, to_version, status, COALESCE(mailbox_id::text, ''), COALESCE(plan, '{}'::jsonb)
		FROM template_migrations
		WHERE id = $1::uuid
	`, migrationID).Scan(&verticalID, &fromVersion, &toVersion, &status, &mailboxID, &planRaw); err != nil {
		return fmt.Errorf("load migration: %w", err)
	}
	if status != "pending" {
		return fmt.Errorf("migration %s is not pending (status=%s)", migrationID, status)
	}
	if mailboxID.Valid && strings.TrimSpace(mailboxID.String) != "" && stores.MailboxStore != nil {
		item, err := stores.MailboxStore.GetMailboxItem(ctx, mailboxID.String)
		if err != nil {
			return fmt.Errorf("load migration mailbox item: %w", err)
		}
		if strings.TrimSpace(item.Status) != "approved" {
			return fmt.Errorf("migration %s not approved in mailbox (status=%s)", migrationID, item.Status)
		}
	}

	plan, err := decodeTemplateMigrationPlan(planRaw)
	if err != nil || len(plan.Operations) == 0 {
		// Plan may be empty/invalid; rebuild by diffing templates using templateops.Service.
		// We keep this dependency-free by failing fast; the operator can re-run `empire template plan`.
		if err == nil {
			err = fmt.Errorf("empty migration plan")
		}
		return failTemplateMigration(ctx, stores, migrationID, verticalID, executedBy, err)
	}

	if _, err := stores.SQLDB.ExecContext(ctx, `
		UPDATE template_migrations
		SET status = 'executing'
		WHERE id = $1::uuid
	`, migrationID); err != nil {
		return failTemplateMigration(ctx, stores, migrationID, verticalID, executedBy, fmt.Errorf("set migration executing: %w", err))
	}

	// Hydrate a manager so we can execute primitives without calling the LLM.
	bus := runtime.NewEventBus(stores.EventStore)
	manager := runtimemanager.NewAgentManager(bus, nil, stores.ManagerStore)
	manager.SetSessionRegistry(stores.SessionRegistry, cfgRuntimeMode)
	if err := manager.Recover(ctx); err != nil {
		return failTemplateMigration(ctx, stores, migrationID, verticalID, executedBy, fmt.Errorf("recover manager: %w", err))
	}

	for _, op := range plan.Operations {
		if err := executeMigrationOp(ctx, stores.SQLDB, manager, verticalID, toVersion, executedBy, op); err != nil {
			return failTemplateMigration(ctx, stores, migrationID, verticalID, executedBy, err)
		}
	}

	// Bump version last write (spec v2.0).
	if _, err := stores.SQLDB.ExecContext(ctx, `
		UPDATE verticals
		SET template_version = $2,
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, toVersion); err != nil {
		return failTemplateMigration(ctx, stores, migrationID, verticalID, executedBy, fmt.Errorf("apply template version to vertical: %w", err))
	}

	completePayload := mustJSON(map[string]any{
		"migration_id": migrationID,
		"vertical_id":  verticalID,
		"from_version": fromVersion,
		"to_version":   toVersion,
		"executed_by":  executedBy,
		"operations":   plan.Operations,
		"warnings":     plan.Warnings,
		"executed_at":  time.Now().UTC().Format(time.RFC3339),
	})
	if err := stores.EventStore.AppendEvent(ctx, (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("template.migration_complete"),
		SourceAgent: executedBy,
		Payload:     completePayload,
		CreatedAt:   time.Now(),
	}).WithEntityID(verticalID)); err != nil {
		log.Printf("template.migration_complete append failed migration=%s err=%v", migrationID, err)
	}
	if err := stores.EventStore.AppendEvent(ctx, (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("template.migration_completed"),
		SourceAgent: executedBy,
		Payload:     completePayload,
		CreatedAt:   time.Now(),
	}).WithEntityID(verticalID)); err != nil {
		log.Printf("template.migration_completed append failed migration=%s err=%v", migrationID, err)
	}

	if _, err := stores.SQLDB.ExecContext(ctx, `
		UPDATE template_migrations
		SET status = 'completed',
		    executed_at = now(),
		    error = NULL
		WHERE id = $1::uuid
	`, migrationID); err != nil {
		return failTemplateMigration(ctx, stores, migrationID, verticalID, executedBy, fmt.Errorf("set migration completed: %w", err))
	}
	return nil
}

type templateMigrationPlan struct {
	VerticalID  string                `json:"vertical_id"`
	FromVersion string                `json:"from_version"`
	ToVersion   string                `json:"to_version"`
	GeneratedBy string                `json:"generated_by"`
	GeneratedAt string                `json:"generated_at"`
	Operations  []templateMigrationOp `json:"operations"`
	Warnings    []string              `json:"warnings,omitempty"`
}

type templateMigrationOp struct {
	Type          string             `json:"type"`
	AgentID       string             `json:"agent_id,omitempty"`
	Config        models.AgentConfig `json:"config,omitempty"`
	EventPattern  string             `json:"event_pattern,omitempty"`
	SubscriberID  string             `json:"subscriber_id,omitempty"`
	Reason        string             `json:"reason,omitempty"`
	Source        string             `json:"source,omitempty"`
	InstalledBy   string             `json:"installed_by,omitempty"`
	AllowedRemove bool               `json:"allowed_remove,omitempty"`
}

func decodeTemplateMigrationPlan(planRaw []byte) (templateMigrationPlan, error) {
	var plan templateMigrationPlan
	if len(planRaw) == 0 || !json.Valid(planRaw) {
		return plan, fmt.Errorf("invalid plan json")
	}
	if err := json.Unmarshal(planRaw, &plan); err != nil {
		return plan, err
	}
	return plan, nil
}

func executeMigrationOp(
	ctx context.Context,
	db *sql.DB,
	manager *runtimemanager.AgentManager,
	verticalID, toVersion, executedBy string,
	op templateMigrationOp,
) error {
	switch strings.TrimSpace(op.Type) {
	case "ADD_AGENT":
		cfg := op.Config
		if cfg.ID == "" {
			cfg.ID = strings.TrimSpace(op.AgentID)
		}
		if cfg.VerticalID == "" {
			cfg.VerticalID = verticalID
		}
		cfg.Mode = coalesce(cfg.Mode, "operating")
		cfg.Role = coalesce(cfg.Role, extractRoleFromAgentID(cfg.ID))
		cfg.Config = expandSystemPromptPlaceholders(ctx, db, verticalID, toVersion, cfg.Config)
		if err := manager.SpawnAgent(cfg); err != nil {
			return fmt.Errorf("ADD_AGENT %s failed: %w", cfg.ID, err)
		}
		_, _ = db.ExecContext(ctx, `UPDATE agents SET template_version = $2 WHERE id = $1`, cfg.ID, toVersion)
		return nil
	case "REMOVE_AGENT":
		id := strings.TrimSpace(op.AgentID)
		if id == "" {
			return fmt.Errorf("REMOVE_AGENT requires agent_id")
		}
		if err := manager.TeardownAgent(id); err != nil {
			return fmt.Errorf("REMOVE_AGENT %s failed: %w", id, err)
		}
		return nil
	case "RECONFIGURE_AGENT":
		cfg := op.Config
		id := strings.TrimSpace(op.AgentID)
		if id == "" {
			id = strings.TrimSpace(cfg.ID)
		}
		if id == "" {
			return fmt.Errorf("RECONFIGURE_AGENT requires agent_id")
		}
		if cfg.ID == "" {
			cfg.ID = id
		}
		if cfg.VerticalID == "" {
			cfg.VerticalID = verticalID
		}
		cfg.Mode = coalesce(cfg.Mode, "operating")
		cfg.Role = coalesce(cfg.Role, extractRoleFromAgentID(cfg.ID))
		cfg.Config = expandSystemPromptPlaceholders(ctx, db, verticalID, toVersion, cfg.Config)
		if err := manager.ReconfigureAgent(id, cfg); err != nil {
			return fmt.Errorf("RECONFIGURE_AGENT %s failed: %w", id, err)
		}
		_, _ = db.ExecContext(ctx, `UPDATE agents SET template_version = $2 WHERE id = $1`, id, toVersion)
		return nil
	case "ADD_ROUTE":
		// Routing derives from contracts and flow-instance expansion in the MAS runtime.
		// Legacy template route mutations are intentionally obsolete.
		return nil
	case "REMOVE_ROUTE":
		// Routing derives from contracts and flow-instance expansion in the MAS runtime.
		// Legacy template route mutations are intentionally obsolete.
		return nil
	default:
		return fmt.Errorf("unsupported migration operation: %s", op.Type)
	}
}

func resolveBootstrapVersionForTemplate(ctx context.Context, db *sql.DB, templateVersion string) int {
	if db == nil {
		return 1
	}
	templateVersion = strings.TrimSpace(templateVersion)
	if templateVersion != "" {
		var v int
		err := db.QueryRowContext(ctx, `
			SELECT bv.version
			FROM org_templates ot
			INNER JOIN bootstrap_versions bv
				ON bv.routes = ot.bootstrap_routes
			WHERE ot.version = $1
			ORDER BY bv.created_at DESC, bv.version DESC
			LIMIT 1
		`, templateVersion).Scan(&v)
		if err == nil && v > 0 {
			return v
		}
	}
	var latest int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 1) FROM bootstrap_versions`).Scan(&latest); err != nil {
		return 1
	}
	if latest <= 0 {
		return 1
	}
	return latest
}

func failTemplateMigration(ctx context.Context, stores storeBundle, migrationID, verticalID, executedBy string, cause error) error {
	msg := strings.TrimSpace(cause.Error())
	if msg == "" {
		msg = "unknown migration failure"
	}
	_, _ = stores.SQLDB.ExecContext(ctx, `
		UPDATE template_migrations
		SET status = 'failed',
		    error = $2,
		    executed_at = now()
		WHERE id = $1::uuid
	`, migrationID, msg)

	payload := mustJSON(map[string]any{
		"migration_id": migrationID,
		"vertical_id":  verticalID,
		"executed_by":  executedBy,
		"error":        msg,
		"failed_at":    time.Now().UTC().Format(time.RFC3339),
	})
	if err := stores.EventStore.AppendEvent(ctx, (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("template.migration_failed"),
		SourceAgent: executedBy,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}).WithEntityID(verticalID)); err != nil {
		log.Printf("template.migration_failed append failed migration=%s err=%v", migrationID, err)
	}
	if stores.MailboxStore != nil {
		if _, err := stores.MailboxStore.InsertMailboxItem(ctx, runtimetools.MailboxItem{
			VerticalID: verticalID,
			FromAgent:  executedBy,
			Type:       "digest",
			Priority:   "normal",
			Status:     "pending",
			Context:    payload,
			Summary:    fmt.Sprintf("Template migration failed: %s", migrationID),
		}); err != nil {
			log.Printf("template migration failure mailbox insert failed migration=%s err=%v", migrationID, err)
		}
	}
	return fmt.Errorf("template migration failed: %s", msg)
}

func expandSystemPromptPlaceholders(ctx context.Context, db *sql.DB, verticalID, templateVersion string, cfgRaw []byte) []byte {
	if db == nil || verticalID == "" || len(cfgRaw) == 0 || !json.Valid(cfgRaw) {
		return cfgRaw
	}
	var obj map[string]any
	if json.Unmarshal(cfgRaw, &obj) != nil {
		return cfgRaw
	}
	sp, _ := obj["system_prompt"].(string)
	sp = strings.TrimSpace(sp)
	if sp == "" {
		return cfgRaw
	}

	name, slug, geo, humanNotes := "", "", "", ""
	var businessBrief, mvpSpec, brand, deployCfg []byte
	_ = db.QueryRowContext(ctx, `
		SELECT
			COALESCE(name,''),
			COALESCE(slug,''),
			COALESCE(geography,''),
			COALESCE(human_notes,''),
			COALESCE(business_brief, '{}'::jsonb),
			COALESCE(mvp_spec, '{}'::jsonb),
			COALESCE(brand, '{}'::jsonb),
			COALESCE(deploy_config, '{}'::jsonb)
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&name, &slug, &geo, &humanNotes, &businessBrief, &mvpSpec, &brand, &deployCfg)

	orgRoster := renderOrgRosterFromTemplate(ctx, db, strings.TrimSpace(templateVersion), verticalID)
	mandateText := renderMandateFromVertical(verticalID, strings.TrimSpace(humanNotes), businessBrief, mvpSpec, brand, deployCfg)

	replacements := map[string]string{
		"{vertical_id}":        strings.TrimSpace(verticalID),
		"{vertical_name}":      strings.TrimSpace(name),
		"{vertical_slug}":      strings.TrimSpace(slug),
		"{geography}":          strings.TrimSpace(geo),
		"{org_roster}":         orgRoster,
		"{mandate_document}":   mandateText,
		"{founder_directives}": strings.TrimSpace(humanNotes),
	}
	for k, v := range replacements {
		sp = strings.ReplaceAll(sp, k, v)
	}
	obj["system_prompt"] = sp
	out, err := json.Marshal(obj)
	if err != nil {
		return cfgRaw
	}
	return out
}

type orgTemplateAgentRole struct {
	Role string `json:"role"`
}

func renderOrgRosterFromTemplate(ctx context.Context, db *sql.DB, templateVersion, verticalID string) string {
	if db == nil || strings.TrimSpace(templateVersion) == "" || strings.TrimSpace(verticalID) == "" {
		return ""
	}
	var agentsRaw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(agents, '[]'::jsonb)
		FROM org_templates
		WHERE version = $1
	`, templateVersion).Scan(&agentsRaw); err != nil || len(agentsRaw) == 0 || !json.Valid(agentsRaw) {
		return ""
	}
	var agents []orgTemplateAgentRole
	if err := json.Unmarshal(agentsRaw, &agents); err != nil || len(agents) == 0 {
		return ""
	}
	parts := make([]string, 0, len(agents))
	for _, a := range agents {
		role := strings.TrimSpace(a.Role)
		if role == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("- %s (%s)", role, role+"-"+strings.TrimSpace(verticalID)))
	}
	return strings.Join(parts, "\n")
}

func renderMandateFromVertical(verticalID, founderNotes string, businessBrief, mvpSpec, brand, deployCfg []byte) string {
	obj := map[string]any{
		"vertical_id":   strings.TrimSpace(verticalID),
		"founder_notes": strings.TrimSpace(founderNotes),
	}
	if len(businessBrief) > 0 && json.Valid(businessBrief) && string(businessBrief) != "null" {
		obj["business_brief"] = json.RawMessage(businessBrief)
	}
	if len(mvpSpec) > 0 && json.Valid(mvpSpec) && string(mvpSpec) != "null" {
		obj["mvp_spec"] = json.RawMessage(mvpSpec)
	}
	if len(brand) > 0 && json.Valid(brand) && string(brand) != "null" {
		obj["brand"] = json.RawMessage(brand)
	}
	if len(deployCfg) > 0 && json.Valid(deployCfg) && string(deployCfg) != "null" {
		obj["infrastructure"] = json.RawMessage(deployCfg)
	}
	b, _ := json.MarshalIndent(obj, "", "  ")
	return string(b)
}

func extractRoleFromAgentID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	if idx := strings.LastIndex(id, "-"); idx > 0 {
		return strings.TrimSpace(id[:idx])
	}
	return ""
}

func coalesce(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}
