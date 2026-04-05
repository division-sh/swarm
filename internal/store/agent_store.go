package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	runtimesessions "swarm/internal/runtime/sessions"
)

func (s *PostgresStore) UpsertAgent(ctx context.Context, rec runtimemanager.PersistedAgent) error {
	if err := s.ensureSchemaCompatibilityColumns(ctx); err != nil {
		return err
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if rec.Config.ID == "" {
		return fmt.Errorf("agent id is required")
	}
	rec.Config.NormalizeEntityID()
	rec.Config.NormalizeRuntimeDescriptor()
	if _, err := runtimesessions.ValidateAgentSessionScopeConfig(rec.Config); err != nil {
		return fmt.Errorf("invalid agent session scope: %w", err)
	}
	projection, err := projectPersistedAgentConfig(rec.Config, rec.ParentAgentID)
	if err != nil {
		return err
	}

	var startedAt any
	if !rec.StartedAt.IsZero() {
		startedAt = rec.StartedAt
	}
	switch caps.Agents {
	case SchemaFlavorCanonical:
		return s.upsertAgentSpec(ctx, rec, projection, startedAt)
	default:
		return unsupportedSchemaCapability("agents", caps.Agents)
	}
}

func (s *PostgresStore) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	if err := s.ensureSchemaCompatibilityColumns(ctx); err != nil {
		return nil, err
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	switch caps.Agents {
	case SchemaFlavorCanonical:
		return s.loadAgentsSpec(ctx)
	default:
		return nil, unsupportedSchemaCapability("agents", caps.Agents)
	}
}

func (s *PostgresStore) MarkAgentTerminated(ctx context.Context, agentID string) error {
	if strings.TrimSpace(agentID) == "" {
		return fmt.Errorf("agent_id is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mark agent terminated begin tx: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback()
	}()

	const qAgentSpec = `
			UPDATE agents
			SET status = 'terminated',
			    last_active_at = now()
			WHERE agent_id = $1
	`
	switch caps.Agents {
	case SchemaFlavorCanonical:
	default:
		return unsupportedSchemaCapability("agents", caps.Agents)
	}
	if _, err := tx.ExecContext(ctx, qAgentSpec, agentID); err != nil {
		return fmt.Errorf("mark agent terminated: %w", err)
	}

	const qSessions = `
		UPDATE agent_sessions
		SET status = 'terminated',
		    lease_holder = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE agent_id = $1
		  AND status = 'active'
	`
	if _, err := tx.ExecContext(ctx, qSessions, agentID); err != nil {
		return fmt.Errorf("mark agent terminated sessions: %w", err)
	}
	if caps.Conversations.Audits == SchemaFlavorCanonical {
		const qConversationAudits = `
			UPDATE agent_conversation_audits
			SET status = 'terminated',
			    updated_at = now()
			WHERE agent_id = $1
			  AND status = 'active'
		`
		if _, err := tx.ExecContext(ctx, qConversationAudits, agentID); err != nil {
			return fmt.Errorf("mark agent terminated conversation audits: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mark agent terminated commit: %w", err)
	}
	committed = true
	return nil
}

func (s *PostgresStore) upsertAgentSpec(ctx context.Context, rec runtimemanager.PersistedAgent, projection persistedAgentProjection, startedAt any) error {
	const q = `
		INSERT INTO agents (
			agent_id, flow_instance, role, model_tier, llm_backend, conversation_mode,
			parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
			runtime_descriptor, status, turn_count, last_active_at, created_at
		)
		VALUES (
			$1, NULLIF($2,''), $3, $4, $5, $6,
			NULLIF($7,''), NULLIF($8,'')::uuid, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb,
			$14::jsonb, $15, 0, now(), COALESCE($16, now())
		)
		ON CONFLICT (agent_id) DO UPDATE SET
			flow_instance = EXCLUDED.flow_instance,
			role = EXCLUDED.role,
			model_tier = EXCLUDED.model_tier,
			llm_backend = EXCLUDED.llm_backend,
			conversation_mode = EXCLUDED.conversation_mode,
			parent_agent_id = EXCLUDED.parent_agent_id,
			entity_id = EXCLUDED.entity_id,
			config = EXCLUDED.config,
			subscriptions = EXCLUDED.subscriptions,
			emit_events = EXCLUDED.emit_events,
			tools = EXCLUDED.tools,
			permissions = EXCLUDED.permissions,
			runtime_descriptor = EXCLUDED.runtime_descriptor,
			status = EXCLUDED.status,
			last_active_at = now()
	`
	_, err := s.DB.ExecContext(ctx, q,
		projection.AgentID,
		projection.FlowInstance,
		projection.Role,
		projection.ModelTier,
		projection.LLMBackend,
		projection.ConversationMode,
		projection.ParentAgentID,
		projection.EntityID,
		string(projection.ConfigJSON),
		string(projection.SubscriptionsJSON),
		string(projection.EmitEventsJSON),
		string(projection.ToolsJSON),
		string(projection.PermissionsJSON),
		string(projection.RuntimeDescriptor),
		agentPersistedStatus(rec.Status),
		startedAt,
	)
	return err
}

func (s *PostgresStore) loadAgentsSpec(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	const q = `
		SELECT
			agent_id,
			COALESCE(flow_instance, ''),
			role,
			model_tier,
			llm_backend,
			conversation_mode,
			COALESCE(parent_agent_id, ''),
			COALESCE(entity_id::text, ''),
			config,
			COALESCE(runtime_descriptor, '{}'::jsonb),
			COALESCE(subscriptions, '[]'::jsonb),
			COALESCE(emit_events, '[]'::jsonb),
			COALESCE(tools, '[]'::jsonb),
			COALESCE(permissions, '[]'::jsonb),
			COALESCE(status, 'active'),
			COALESCE(created_at, now())
		FROM agents
		WHERE status NOT IN ('terminated', 'ephemeral')
		ORDER BY created_at ASC, agent_id ASC
	`
	rows, err := s.DB.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query agents: %w", err)
	}
	defer rows.Close()

	var out []runtimemanager.PersistedAgent
	for rows.Next() {
		var rec runtimemanager.PersistedAgent
		var row persistedAgentProjection
		if err := rows.Scan(
			&row.AgentID,
			&row.FlowInstance,
			&row.Role,
			&row.ModelTier,
			&row.LLMBackend,
			&row.ConversationMode,
			&row.ParentAgentID,
			&row.EntityID,
			&row.ConfigJSON,
			&row.RuntimeDescriptor,
			&row.SubscriptionsJSON,
			&row.EmitEventsJSON,
			&row.ToolsJSON,
			&row.PermissionsJSON,
			&rec.Status,
			&rec.StartedAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent row: %w", err)
		}
		cfg, err := hydratePersistedAgentConfig(row)
		if err != nil {
			return nil, fmt.Errorf("hydrate agent row %s: %w", strings.TrimSpace(row.AgentID), err)
		}
		rec.ParentAgentID = row.ParentAgentID
		rec.Config = cfg
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agents rows: %w", err)
	}
	return out, nil
}

func agentModelTier(cfg runtimeactors.AgentConfig) string {
	if v := strings.TrimSpace(cfg.ModelTier); v != "" {
		return v
	}
	if v := strings.TrimSpace(cfg.Type); v != "" {
		return v
	}
	return "generic"
}

func agentPersistedType(cfg runtimeactors.AgentConfig, modelTier string) string {
	if v := strings.TrimSpace(cfg.Type); v != "" {
		return v
	}
	if v := strings.TrimSpace(modelTier); v != "" {
		return v
	}
	return "generic"
}

func agentConversationMode(cfg runtimeactors.AgentConfig) string {
	if v := strings.TrimSpace(cfg.ConversationMode); v != "" {
		return runtimesessions.NormalizeConversationRuntimeMode(v)
	}
	return runtimesessions.RuntimeModeTask
}

func agentFlowInstance(cfg runtimeactors.AgentConfig) string {
	if v := strings.TrimSpace(cfg.FlowPath); v != "" {
		return v
	}
	return ""
}

func agentLLMBackend(cfg runtimeactors.AgentConfig) string {
	if v := strings.TrimSpace(cfg.LLMBackend); v != "" {
		return v
	}
	return "api"
}

func agentPersistedStatus(raw string) string {
	switch strings.TrimSpace(raw) {
	case "paused":
		return "paused"
	case "terminated", "ephemeral":
		return "terminated"
	default:
		return "active"
	}
}

func decodeJSONStringList(raw []byte) []string {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	var out []string
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func coalesceStringList(primary, fallback []string) []string {
	if len(primary) > 0 {
		return primary
	}
	return fallback
}

func coalesce(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (s *PostgresStore) EnsureEntitySchema(ctx context.Context, entityID string) error {
	if strings.TrimSpace(entityID) == "" {
		return fmt.Errorf("entity_id is required")
	}
	if _, err := uuid.Parse(strings.TrimSpace(entityID)); err != nil {
		return nil
	}
	var slug string
	err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM entity_state
		WHERE entity_id = $1::uuid
	`, entityID).Scan(&slug)
	if err != nil {
		return fmt.Errorf("lookup entity slug: %w", err)
	}
	schema := sanitizeSchemaIdent(slug)
	if schema == "" {
		return fmt.Errorf("entity %s has no valid slug for schema creation", entityID)
	}
	schema = schema + "_schema"
	if _, err := s.DB.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS `+quoteIdent(schema)); err != nil {
		return fmt.Errorf("create entity schema %s: %w", schema, err)
	}
	return nil
}
