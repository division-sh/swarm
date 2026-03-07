package templateops

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"empireai/internal/models"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/google/uuid"
)

type Service struct {
	DB      *sql.DB
	Mailbox runtimetools.MailboxPersistence
}

func NewService(db *sql.DB, mailbox runtimetools.MailboxPersistence) *Service {
	return &Service{DB: db, Mailbox: mailbox}
}

type templateSnapshot struct {
	Version         string
	Agents          []templateAgent
	BootstrapRoutes []templateRoute
	SeededRoutes    []templateRoute
}

type templateAgent struct {
	Role          string         `json:"role"`
	ParentRole    string         `json:"parent_role"`
	Type          string         `json:"type"`
	SystemPrompt  string         `json:"system_prompt"`
	Tools         []string       `json:"tools"`
	Subscriptions []string       `json:"subscriptions"`
	Constraints   map[string]any `json:"constraints,omitempty"`
}

type templateRoute struct {
	EventPattern   string `json:"event_pattern"`
	SubscriberRole string `json:"subscriber_role"`
	SubscriberID   string `json:"subscriber_id"`
	Reason         string `json:"reason"`
}

type migrationPlan struct {
	VerticalID  string               `json:"vertical_id"`
	FromVersion string               `json:"from_version"`
	ToVersion   string               `json:"to_version"`
	GeneratedBy string               `json:"generated_by"`
	GeneratedAt string               `json:"generated_at"`
	Operations  []migrationOperation `json:"operations"`
	Warnings    []string             `json:"warnings,omitempty"`
}

type migrationOperation struct {
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

func (s *Service) PublishTemplate(
	ctx context.Context,
	version string,
	agents, bootstrapRoutes, seededRoutes []byte,
	createdBy, description string,
) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("template service requires postgres db")
	}
	version = strings.TrimSpace(version)
	if version == "" {
		return fmt.Errorf("template version is required")
	}
	if len(agents) == 0 {
		agents = []byte("[]")
	}
	if len(bootstrapRoutes) == 0 {
		bootstrapRoutes = []byte("[]")
	}
	if len(seededRoutes) == 0 {
		seededRoutes = []byte("[]")
	}
	if strings.TrimSpace(createdBy) == "" {
		createdBy = "factory-cto"
	}

	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO org_templates (version, agents, bootstrap_routes, seeded_routes, created_by, description, created_at)
		VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb, $5, NULLIF($6,''), now())
		ON CONFLICT (version) DO UPDATE
		SET agents = EXCLUDED.agents,
		    bootstrap_routes = EXCLUDED.bootstrap_routes,
		    seeded_routes = EXCLUDED.seeded_routes,
		    created_by = EXCLUDED.created_by,
		    description = EXCLUDED.description
	`, version, string(agents), string(bootstrapRoutes), string(seededRoutes), createdBy, description)
	if err != nil {
		return fmt.Errorf("publish template version %s: %w", version, err)
	}
	if _, err := s.ensureBootstrapVersion(ctx, bootstrapRoutes, createdBy, description); err != nil {
		return err
	}

	// Emit template.version_published for the runtime audit trail.
	payload := mustJSON(map[string]any{
		"version":      version,
		"created_by":   createdBy,
		"description":  strings.TrimSpace(description),
		"published_at": time.Now().UTC().Format(time.RFC3339),
	})
	if _, err := s.DB.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'template.version_published', $2, $3::jsonb, now())
	`, uuid.NewString(), createdBy, string(payload)); err != nil {
		return fmt.Errorf("emit template.version_published: %w", err)
	}
	return nil
}

func (s *Service) ensureBootstrapVersion(
	ctx context.Context,
	bootstrapRoutes []byte,
	createdBy, evidence string,
) (int, error) {
	if len(bootstrapRoutes) == 0 {
		bootstrapRoutes = []byte("[]")
	}

	var existing int
	err := s.DB.QueryRowContext(ctx, `
		SELECT version
		FROM bootstrap_versions
		WHERE routes = $1::jsonb
		ORDER BY created_at DESC, version DESC
		LIMIT 1
	`, string(bootstrapRoutes)).Scan(&existing)
	if err == nil && existing > 0 {
		return existing, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("lookup bootstrap version: %w", err)
	}

	var next int
	if err := s.DB.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM bootstrap_versions`).Scan(&next); err != nil {
		return 0, fmt.Errorf("allocate bootstrap version: %w", err)
	}
	if next <= 0 {
		next = 1
	}

	actor := strings.TrimSpace(createdBy)
	if actor == "" {
		actor = "factory-cto"
	}
	if _, err := s.DB.ExecContext(ctx, `
		INSERT INTO bootstrap_versions (version, routes, proposed_by, approved_by, evidence, created_at)
		VALUES ($1, $2::jsonb, $3, $4, NULLIF($5,''), now())
	`, next, string(bootstrapRoutes), actor, actor, strings.TrimSpace(evidence)); err != nil {
		return 0, fmt.Errorf("insert bootstrap version %d: %w", next, err)
	}
	return next, nil
}

func (s *Service) PlanMigrations(ctx context.Context, toVersion, requestedBy string, limit int) (int, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("template service requires postgres db")
	}
	toVersion = strings.TrimSpace(toVersion)
	if toVersion == "" {
		return 0, fmt.Errorf("to-version is required")
	}
	if strings.TrimSpace(requestedBy) == "" {
		requestedBy = "factory-cto"
	}
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT id::text, COALESCE(template_version, '')
		FROM verticals
		WHERE mode = 'operating'
		  AND (template_version IS NULL OR template_version <> $1)
		ORDER BY created_at ASC
		LIMIT $2
	`, toVersion, limit)
	if err != nil {
		return 0, fmt.Errorf("list migration candidates: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var verticalID, fromVersion string
		if err := rows.Scan(&verticalID, &fromVersion); err != nil {
			return count, fmt.Errorf("scan migration candidate: %w", err)
		}

		plan, err := s.buildMigrationPlan(ctx, verticalID, fromVersion, toVersion, requestedBy)
		if err != nil {
			return count, err
		}
		planJSON := mustJSON(plan)

		migrationID := uuid.NewString()
		if _, err := s.DB.ExecContext(ctx, `
			INSERT INTO template_migrations (id, vertical_id, from_version, to_version, plan, status, created_at)
			VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, 'pending', now())
		`, migrationID, verticalID, fromVersion, toVersion, string(planJSON)); err != nil {
			return count, fmt.Errorf("insert template migration: %w", err)
		}

		var mailboxID string
		if s.Mailbox != nil {
			id, err := s.Mailbox.InsertMailboxItem(ctx, runtimetools.MailboxItem{
				VerticalID: verticalID,
				FromAgent:  requestedBy,
				Type:       "migration_approval",
				Priority:   "normal",
				Status:     "pending",
				Context:    planJSON,
				Summary:    fmt.Sprintf("Template migration approval: %s -> %s", fromVersion, toVersion),
			})
			if err != nil {
				return count, fmt.Errorf("create migration mailbox item: %w", err)
			}
			mailboxID = id
		}
		if mailboxID != "" {
			if _, err := s.DB.ExecContext(ctx, `
				UPDATE template_migrations
				SET mailbox_id = $2::uuid
				WHERE id = $1::uuid
			`, migrationID, mailboxID); err != nil {
				return count, fmt.Errorf("link migration mailbox: %w", err)
			}
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("iterate migration candidates: %w", err)
	}
	return count, nil
}

func (s *Service) buildMigrationPlan(ctx context.Context, verticalID, fromVersion, toVersion, generatedBy string) (migrationPlan, error) {
	oldTemplate, err := s.loadTemplateSnapshot(ctx, fromVersion)
	if err != nil {
		return migrationPlan{}, err
	}
	newTemplate, err := s.loadTemplateSnapshot(ctx, toVersion)
	if err != nil {
		return migrationPlan{}, err
	}

	ops := make([]migrationOperation, 0, 64)
	warnings := make([]string, 0, 8)

	oldAgents := make(map[string]templateAgent, len(oldTemplate.Agents))
	for _, a := range oldTemplate.Agents {
		oldAgents[a.Role] = a
	}
	newAgents := make(map[string]templateAgent, len(newTemplate.Agents))
	for _, a := range newTemplate.Agents {
		newAgents[a.Role] = a
	}

	for _, a := range newTemplate.Agents {
		cfg := agentConfigFromTemplate(verticalID, a)
		if prev, ok := oldAgents[a.Role]; !ok {
			ops = append(ops, migrationOperation{
				Type:    "ADD_AGENT",
				AgentID: cfg.ID,
				Config:  cfg,
			})
		} else if templateAgentSignature(prev) != templateAgentSignature(a) {
			ops = append(ops, migrationOperation{
				Type:    "RECONFIGURE_AGENT",
				AgentID: cfg.ID,
				Config:  cfg,
			})
		}
	}
	for _, a := range oldTemplate.Agents {
		if _, ok := newAgents[a.Role]; !ok {
			ops = append(ops, migrationOperation{
				Type:    "REMOVE_AGENT",
				AgentID: opCoAgentID(a.Role, verticalID),
			})
		}
	}

	installedBy := opCoAgentID("opco-ceo", verticalID)
	oldRoutes := routeOpsFromTemplate(verticalID, oldTemplate, installedBy)
	newRoutes := routeOpsFromTemplate(verticalID, newTemplate, installedBy)
	for key, next := range newRoutes {
		if _, ok := oldRoutes[key]; !ok {
			next.Type = "ADD_ROUTE"
			ops = append(ops, next)
		}
	}
	for key, prev := range oldRoutes {
		if _, ok := newRoutes[key]; ok {
			continue
		}
		if prev.Source == "bootstrap" {
			warnings = append(warnings, fmt.Sprintf("bootstrap route removed via migration: %s -> %s", prev.EventPattern, prev.SubscriberID))
		}
		prev.Type = "REMOVE_ROUTE"
		prev.AllowedRemove = true
		ops = append(ops, prev)
	}

	return migrationPlan{
		VerticalID:  verticalID,
		FromVersion: fromVersion,
		ToVersion:   toVersion,
		GeneratedBy: generatedBy,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Operations:  ops,
		Warnings:    warnings,
	}, nil
}

func routeOpsFromTemplate(verticalID string, snapshot templateSnapshot, installedBy string) map[string]migrationOperation {
	out := make(map[string]migrationOperation, 64)
	add := func(routes []templateRoute, source string) {
		for _, r := range routes {
			pattern := strings.TrimSpace(r.EventPattern)
			if pattern == "" {
				continue
			}
			sub := strings.TrimSpace(r.SubscriberID)
			if sub == "" {
				sub = opCoAgentID(r.SubscriberRole, verticalID)
			}
			if sub == "" {
				continue
			}
			key := pattern + "|" + sub
			out[key] = migrationOperation{
				EventPattern: pattern,
				SubscriberID: sub,
				Reason:       strings.TrimSpace(r.Reason),
				Source:       source,
				InstalledBy:  installedBy,
			}
		}
	}
	add(snapshot.BootstrapRoutes, "bootstrap")
	add(snapshot.SeededRoutes, "seeded")
	return out
}

func templateAgentSignature(a templateAgent) string {
	return string(mustJSON(map[string]any{
		"role":          strings.TrimSpace(a.Role),
		"parent_role":   strings.TrimSpace(a.ParentRole),
		"type":          strings.TrimSpace(a.Type),
		"system_prompt": strings.TrimSpace(a.SystemPrompt),
		"tools":         normalizeStringList(a.Tools),
		"subscriptions": normalizeStringList(a.Subscriptions),
		"constraints":   a.Constraints,
	}))
}

func normalizeStringList(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func agentConfigFromTemplate(verticalID string, a templateAgent) models.AgentConfig {
	role := strings.TrimSpace(a.Role)
	parent := strings.TrimSpace(a.ParentRole)
	cfg := models.AgentConfig{
		ID:            opCoAgentID(role, verticalID),
		Type:          coalesce(strings.TrimSpace(a.Type), "sonnet"),
		Role:          role,
		Mode:          "operating",
		VerticalID:    verticalID,
		ParentAgent:   opCoAgentID(parent, verticalID),
		Subscriptions: normalizeStringList(a.Subscriptions),
	}
	if parent == "" {
		cfg.ParentAgent = ""
	}
	cfg.Config = mustJSON(map[string]any{
		"system_prompt": strings.TrimSpace(a.SystemPrompt),
		"tools":         normalizeStringList(a.Tools),
		"constraints":   a.Constraints,
	})
	return cfg
}

func opCoAgentID(role, verticalID string) string {
	role = strings.TrimSpace(role)
	verticalID = strings.TrimSpace(verticalID)
	if role == "" || verticalID == "" {
		return ""
	}
	return role + "-" + verticalID
}

func coalesce(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func (s *Service) loadTemplateSnapshot(ctx context.Context, version string) (templateSnapshot, error) {
	version = strings.TrimSpace(version)
	if version == "" {
		return templateSnapshot{Version: ""}, nil
	}
	var agentsRaw, bootstrapRaw, seededRaw []byte
	err := s.DB.QueryRowContext(ctx, `
		SELECT agents, bootstrap_routes, seeded_routes
		FROM org_templates
		WHERE version = $1
	`, version).Scan(&agentsRaw, &bootstrapRaw, &seededRaw)
	if err != nil {
		if err == sql.ErrNoRows {
			return templateSnapshot{}, fmt.Errorf("template version not found: %s", version)
		}
		return templateSnapshot{}, fmt.Errorf("load template %s: %w", version, err)
	}

	snap := templateSnapshot{Version: version}
	_ = json.Unmarshal(defaultJSON(agentsRaw, []byte("[]")), &snap.Agents)
	_ = json.Unmarshal(defaultJSON(bootstrapRaw, []byte("[]")), &snap.BootstrapRoutes)
	_ = json.Unmarshal(defaultJSON(seededRaw, []byte("[]")), &snap.SeededRoutes)
	return snap, nil
}

func defaultJSON(raw, fallback []byte) []byte {
	if len(raw) == 0 {
		return fallback
	}
	return raw
}

func (s *Service) ApplyMigration(ctx context.Context, migrationID, executedBy string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("template service requires postgres db")
	}
	// Spec v2.0: applying a migration must execute runtime primitives (spawn/reconfigure/teardown/configure routing)
	// and only then bump template_version. This service is limited to publish/plan; apply is handled by the runtime/CLI.
	return fmt.Errorf("template migration apply must execute runtime primitives; use `empire template apply`")
}

func (s *Service) decodeOrBuildPlan(
	ctx context.Context,
	verticalID, fromVersion, toVersion, generatedBy string,
	planRaw []byte,
) (migrationPlan, error) {
	var plan migrationPlan
	if len(planRaw) > 0 && json.Valid(planRaw) {
		if err := json.Unmarshal(planRaw, &plan); err == nil && len(plan.Operations) > 0 {
			return plan, nil
		}
	}
	return s.buildMigrationPlan(ctx, verticalID, fromVersion, toVersion, generatedBy)
}

func (s *Service) executeOperationTX(
	ctx context.Context,
	tx *sql.Tx,
	verticalID, toVersion, executedBy string,
	op migrationOperation,
) error {
	switch strings.TrimSpace(op.Type) {
	case "ADD_AGENT":
		cfg := op.Config
		if cfg.ID == "" {
			cfg.ID = op.AgentID
		}
		if cfg.ID == "" {
			return fmt.Errorf("ADD_AGENT requires agent_id")
		}
		if cfg.VerticalID == "" {
			cfg.VerticalID = verticalID
		}
		if cfg.Mode == "" {
			cfg.Mode = "operating"
		}
		if cfg.Type == "" {
			cfg.Type = "sonnet"
		}
		cfgJSON := marshalAgentConfig(cfg)
		coordinator := opCoAgentID("opco-ceo", verticalID)
		_, err := tx.ExecContext(ctx, `
			INSERT INTO agents (
				id, type, role, mode, vertical_id, parent_agent_id, status,
				coordinator_id, config, template_version, hired_by, started_at, last_active_at
			)
			VALUES (
				$1, $2, $3, $4, $5::uuid, NULLIF($6,''), 'active',
				NULLIF($7,''), $8::jsonb, $9, $10, now(), now()
			)
			ON CONFLICT (id) DO UPDATE SET
				type = EXCLUDED.type,
				role = EXCLUDED.role,
				mode = EXCLUDED.mode,
				vertical_id = EXCLUDED.vertical_id,
				parent_agent_id = EXCLUDED.parent_agent_id,
				status = 'active',
				coordinator_id = EXCLUDED.coordinator_id,
				config = EXCLUDED.config,
				template_version = EXCLUDED.template_version,
				hired_by = EXCLUDED.hired_by,
				last_active_at = now()
		`, cfg.ID, cfg.Type, cfg.Role, cfg.Mode, cfg.VerticalID, cfg.ParentAgent, coordinator, string(cfgJSON), toVersion, executedBy)
		if err != nil {
			return fmt.Errorf("ADD_AGENT %s failed: %w", cfg.ID, err)
		}
		return nil
	case "REMOVE_AGENT":
		agentID := strings.TrimSpace(op.AgentID)
		if agentID == "" {
			return fmt.Errorf("REMOVE_AGENT requires agent_id")
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE agents
			SET status = 'terminated',
			    last_active_at = now()
			WHERE id = $1
		`, agentID); err != nil {
			return fmt.Errorf("REMOVE_AGENT %s failed: %w", agentID, err)
		}
		return nil
	case "RECONFIGURE_AGENT":
		cfg := op.Config
		agentID := strings.TrimSpace(op.AgentID)
		if agentID == "" {
			agentID = cfg.ID
		}
		if agentID == "" {
			return fmt.Errorf("RECONFIGURE_AGENT requires agent_id")
		}
		if cfg.ID == "" {
			cfg.ID = agentID
		}
		if cfg.VerticalID == "" {
			cfg.VerticalID = verticalID
		}
		if cfg.Mode == "" {
			cfg.Mode = "operating"
		}
		if cfg.Type == "" {
			cfg.Type = "sonnet"
		}
		cfgJSON := marshalAgentConfig(cfg)
		if _, err := tx.ExecContext(ctx, `
			UPDATE agents
			SET type = $2,
			    role = $3,
			    mode = $4,
			    vertical_id = $5::uuid,
			    parent_agent_id = NULLIF($6,''),
			    config = $7::jsonb,
			    template_version = $8,
			    last_active_at = now()
			WHERE id = $1
		`, agentID, cfg.Type, cfg.Role, cfg.Mode, cfg.VerticalID, cfg.ParentAgent, string(cfgJSON), toVersion); err != nil {
			return fmt.Errorf("RECONFIGURE_AGENT %s failed: %w", agentID, err)
		}
		return nil
	case "ADD_ROUTE":
		pattern := strings.TrimSpace(op.EventPattern)
		subscriberID := strings.TrimSpace(op.SubscriberID)
		if pattern == "" || subscriberID == "" {
			return fmt.Errorf("ADD_ROUTE requires event_pattern and subscriber_id")
		}
		installedBy := strings.TrimSpace(op.InstalledBy)
		if installedBy == "" {
			installedBy = opCoAgentID("opco-ceo", verticalID)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO routing_rules (
				vertical_id, event_pattern, subscriber_id, installed_by, reason,
				status, source, bootstrap_version, created_at, deactivated_at
			)
			VALUES (
				$1::uuid, $2, $3, $4, NULLIF($5,''),
				'active', $6, NULL, now(), NULL
			)
			ON CONFLICT (vertical_id, event_pattern, subscriber_id) WHERE status = 'active' DO UPDATE SET
				installed_by = EXCLUDED.installed_by,
				reason = EXCLUDED.reason,
				status = 'active',
				source = EXCLUDED.source,
				deactivated_at = NULL
		`, verticalID, pattern, subscriberID, installedBy, op.Reason, coalesce(op.Source, "template_migration")); err != nil {
			return fmt.Errorf("ADD_ROUTE %s -> %s failed: %w", pattern, subscriberID, err)
		}
		return nil
	case "REMOVE_ROUTE":
		pattern := strings.TrimSpace(op.EventPattern)
		subscriberID := strings.TrimSpace(op.SubscriberID)
		if pattern == "" || subscriberID == "" {
			return fmt.Errorf("REMOVE_ROUTE requires event_pattern and subscriber_id")
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE routing_rules
			SET status = 'deactivated',
			    deactivated_at = now()
			WHERE vertical_id = $1::uuid
			  AND event_pattern = $2
			  AND subscriber_id = $3
			  AND source <> 'bootstrap'
		`, verticalID, pattern, subscriberID)
		if err != nil {
			return fmt.Errorf("REMOVE_ROUTE %s -> %s failed: %w", pattern, subscriberID, err)
		}
		if n, _ := res.RowsAffected(); n == 0 && op.AllowedRemove {
			return fmt.Errorf("REMOVE_ROUTE blocked or route missing: %s -> %s", pattern, subscriberID)
		}
		return nil
	default:
		return fmt.Errorf("unsupported migration operation: %s", op.Type)
	}
}

func marshalAgentConfig(cfg models.AgentConfig) []byte {
	payload := map[string]any{
		"role": cfg.Role,
		"mode": cfg.Mode,
	}
	if len(cfg.Subscriptions) > 0 {
		payload["subscriptions"] = cfg.Subscriptions
	}
	if len(cfg.Config) > 0 && json.Valid(cfg.Config) {
		var extra map[string]any
		if err := json.Unmarshal(cfg.Config, &extra); err == nil {
			for k, v := range extra {
				payload[k] = v
			}
		}
	}
	return mustJSON(payload)
}

func (s *Service) failMigration(
	ctx context.Context,
	migrationID, verticalID, executedBy string,
	cause error,
) error {
	msg := strings.TrimSpace(cause.Error())
	if msg == "" {
		msg = "unknown migration failure"
	}
	_, _ = s.DB.ExecContext(ctx, `
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
	_, _ = s.DB.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'template.migration_failed', $2, $3::uuid, $4::jsonb, now())
	`, uuid.NewString(), executedBy, verticalID, string(payload))

	// Spec v2.0: failures must surface to the human. Migration review items may
	// already exist (approved), but we create a new pending mailbox item so the
	// failure can't be missed.
	if s.Mailbox != nil {
		_, _ = s.Mailbox.InsertMailboxItem(ctx, runtimetools.MailboxItem{
			VerticalID: verticalID,
			FromAgent:  executedBy,
			Type:       "digest",
			Priority:   "normal",
			Status:     "pending",
			Context:    payload,
			Summary:    fmt.Sprintf("Template migration failed: %s", migrationID),
		})
	}

	return fmt.Errorf("template migration failed: %s", msg)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}
