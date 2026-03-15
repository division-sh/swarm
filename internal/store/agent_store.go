package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	runtimeactors "empireai/internal/runtime/core/actors"
	runtimemanager "empireai/internal/runtime/manager"
	runtimesessions "empireai/internal/runtime/sessions"
)

func (s *PostgresStore) UpsertAgent(ctx context.Context, rec runtimemanager.PersistedAgent) error {
	if rec.Config.ID == "" {
		return fmt.Errorf("agent id is required")
	}
	rec.Config.NormalizeEntityID()
	cfgJSON, err := mergeAgentConfigJSON(rec.Config)
	if err != nil {
		return fmt.Errorf("marshal agent config: %w", err)
	}

	var startedAt any
	if !rec.StartedAt.IsZero() {
		startedAt = rec.StartedAt
	}

	specErr := s.upsertAgentSpec(ctx, rec, cfgJSON, startedAt)
	if specErr == nil {
		return nil
	}
	if !shouldFallbackLegacyAgentsSchema(specErr) {
		return fmt.Errorf("upsert agent %s: %w", rec.Config.ID, specErr)
	}

	const q = `
		INSERT INTO agents (
			id, type, role, mode, entity_id, parent_agent_id, status,
			coordinator_id, config, budget_envelope, hired_by, template_version,
			started_at, last_active_at
		)
		VALUES (
			$1, $2, $3, $4, NULLIF($5,'')::uuid, NULLIF($6,''), $7,
			NULLIF($8,''), $9::jsonb, NULLIF($10, 0), NULLIF($11,''), NULLIF($12,''),
			COALESCE($13, now()), now()
		)
		ON CONFLICT (id) DO UPDATE SET
			type = EXCLUDED.type,
			role = EXCLUDED.role,
			mode = EXCLUDED.mode,
			entity_id = EXCLUDED.entity_id,
			parent_agent_id = EXCLUDED.parent_agent_id,
			status = EXCLUDED.status,
			coordinator_id = EXCLUDED.coordinator_id,
			config = EXCLUDED.config,
			budget_envelope = EXCLUDED.budget_envelope,
			hired_by = EXCLUDED.hired_by,
			template_version = EXCLUDED.template_version,
			last_active_at = now()
	`
	_, err = s.DB.ExecContext(ctx, q,
		rec.Config.ID,
		nullable(rec.Config.Type, "generic"),
		rec.Config.Role,
		nullable(rec.Config.Mode, "scoped"),
		rec.Config.EffectiveEntityID(),
		nullable(rec.ParentAgentID, rec.Config.ParentAgent),
		nullable(rec.Status, "active"),
		rec.CoordinatorID,
		string(cfgJSON),
		rec.Config.BudgetEnvelope,
		rec.HiredBy,
		rec.TemplateVersion,
		startedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert agent %s: %w", rec.Config.ID, err)
	}
	return nil
}

func (s *PostgresStore) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	agents, err := s.loadAgentsSpec(ctx)
	if err == nil {
		return agents, nil
	}
	if !shouldFallbackLegacyAgentsSchema(err) {
		return nil, err
	}

	const q = `
		SELECT
			id, type, role, mode,
			COALESCE(entity_id::text, ''),
			COALESCE(parent_agent_id, ''),
			COALESCE(status, 'active'),
			COALESCE(coordinator_id, ''),
			config,
			COALESCE(budget_envelope, 0),
			COALESCE(hired_by, ''),
			COALESCE(template_version, ''),
			COALESCE(started_at, now())
		FROM agents
		WHERE status NOT IN ('terminated', 'ephemeral')
		ORDER BY started_at ASC, id ASC
	`
	rows, err := s.DB.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query agents: %w", err)
	}
	defer rows.Close()

	var out []runtimemanager.PersistedAgent
	for rows.Next() {
		var rec runtimemanager.PersistedAgent
		var cfgRaw []byte
		if err := rows.Scan(
			&rec.Config.ID,
			&rec.Config.Type,
			&rec.Config.Role,
			&rec.Config.Mode,
			&rec.Config.EntityID,
			&rec.ParentAgentID,
			&rec.Status,
			&rec.CoordinatorID,
			&cfgRaw,
			&rec.Config.BudgetEnvelope,
			&rec.HiredBy,
			&rec.TemplateVersion,
			&rec.StartedAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent row: %w", err)
		}

		rec.Config.ParentAgent = rec.ParentAgentID
		rec.Config.NormalizeEntityID()
		rec.Config.Config = cfgRaw
		rec.Config.Subscriptions = extractSubscriptions(cfgRaw)
		rec.Config.Permissions = extractPermissions(cfgRaw)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agents rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) MarkAgentTerminated(ctx context.Context, agentID string) error {
	if strings.TrimSpace(agentID) == "" {
		return fmt.Errorf("agent_id is required")
	}
	useSpecAgents := columnExists(ctx, s.DB, "agents", "agent_id")
	hasLegacyConversations := tableExists(ctx, s.DB, "conversations")
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

	const qAgentLegacy = `
		UPDATE agents
		SET status = 'terminated',
		    last_active_at = now()
		WHERE id = $1
	`
	const qAgentSpec = `
			UPDATE agents
			SET status = 'terminated',
			    last_active_at = now()
			WHERE agent_id = $1
	`
	qAgent := qAgentLegacy
	if useSpecAgents {
		qAgent = qAgentSpec
	}
	if _, err := tx.ExecContext(ctx, qAgent, agentID); err != nil {
		return fmt.Errorf("mark agent terminated: %w", err)
	}

	if hasLegacyConversations {
		const qConversations = `
			UPDATE conversations
			SET status = 'terminated',
			    updated_at = now()
			WHERE agent_id = $1
			  AND status = 'active'
		`
		if _, err := tx.ExecContext(ctx, qConversations, agentID); err != nil && !shouldIgnoreLegacyConversationTable(err) {
			return fmt.Errorf("mark agent terminated conversations: %w", err)
		}
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

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mark agent terminated commit: %w", err)
	}
	committed = true
	return nil
}

func (s *PostgresStore) upsertAgentSpec(ctx context.Context, rec runtimemanager.PersistedAgent, cfgJSON []byte, startedAt any) error {
	modelTier := agentModelTier(rec.Config)
	conversationMode := agentConversationMode(rec.Config)
	emitEvents := extractStringListField(cfgJSON, "emit_events")
	tools := extractStringListField(cfgJSON, "tools")
	permissions := rec.Config.Permissions
	subscriptions := rec.Config.Subscriptions
	flowInstance := agentFlowInstance(rec.Config)
	llmBackend := agentLLMBackend(rec.Config)

	const q = `
		INSERT INTO agents (
			agent_id, flow_instance, role, model_tier, llm_backend, conversation_mode,
			parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
			status, turn_count, last_active_at, created_at
		)
		VALUES (
			$1, NULLIF($2,''), $3, $4, $5, $6,
			NULLIF($7,''), NULLIF($8,'')::uuid, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb,
			$14, 0, now(), COALESCE($15, now())
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
			status = EXCLUDED.status,
			last_active_at = now()
	`
	_, err := s.DB.ExecContext(ctx, q,
		rec.Config.ID,
		flowInstance,
		rec.Config.Role,
		modelTier,
		llmBackend,
		conversationMode,
		nullable(rec.ParentAgentID, rec.Config.ParentAgent),
		rec.Config.EffectiveEntityID(),
		string(cfgJSON),
		mustJSONString(subscriptions),
		mustJSONString(emitEvents),
		mustJSONString(tools),
		mustJSONString(permissions),
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
			COALESCE(subscriptions, '[]'::jsonb),
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
		var (
			flowInstance      string
			modelTier         string
			llmBackend        string
			conversationMode  string
			cfgRaw            []byte
			subscriptionsJSON []byte
			permissionsJSON   []byte
		)
		if err := rows.Scan(
			&rec.Config.ID,
			&flowInstance,
			&rec.Config.Role,
			&modelTier,
			&llmBackend,
			&conversationMode,
			&rec.ParentAgentID,
			&rec.Config.EntityID,
			&cfgRaw,
			&subscriptionsJSON,
			&permissionsJSON,
			&rec.Status,
			&rec.StartedAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent row: %w", err)
		}
		rec.Config.ParentAgent = rec.ParentAgentID
		rec.Config.NormalizeEntityID()
		rec.Config.Config = cfgRaw
		rec.Config.Type = coalesce(extractStringField(cfgRaw, "type"), modelTier)
		rec.Config.Mode = extractStringField(cfgRaw, "mode")
		rec.Config.LLMBackend = llmBackend
		rec.Config.Subscriptions = coalesceStringList(decodeJSONStringList(subscriptionsJSON), extractSubscriptions(cfgRaw))
		rec.Config.Permissions = coalesceStringList(decodeJSONStringList(permissionsJSON), extractPermissions(cfgRaw))
		if rec.Config.Mode == "" && flowInstance != "" {
			rec.Config.Mode = flowInstance
		}
		if extractStringField(cfgRaw, "conversation_mode") == "" && conversationMode != "" {
			rec.Config.Config = withConfigString(cfgRaw, "conversation_mode", conversationMode)
		}
		if extractStringField(cfgRaw, "model_tier") == "" && modelTier != "" {
			rec.Config.Config = withConfigString(rec.Config.Config, "model_tier", modelTier)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agents rows: %w", err)
	}
	return out, nil
}

func agentModelTier(cfg runtimeactors.AgentConfig) string {
	if v := extractStringField(cfg.Config, "model_tier"); v != "" {
		return v
	}
	if v := strings.TrimSpace(cfg.Type); v != "" {
		return v
	}
	return "generic"
}

func agentConversationMode(cfg runtimeactors.AgentConfig) string {
	if v := extractStringField(cfg.Config, "conversation_mode"); v != "" {
		return runtimesessions.NormalizeConversationRuntimeMode(v)
	}
	if len(cfg.Config) > 0 && json.Valid(cfg.Config) {
		var obj map[string]any
		if json.Unmarshal(cfg.Config, &obj) == nil {
			if constraints, ok := obj["constraints"].(map[string]any); ok {
				if raw, _ := constraints["conversation_mode"].(string); strings.TrimSpace(raw) != "" {
					return runtimesessions.NormalizeConversationRuntimeMode(raw)
				}
			}
		}
	}
	if cfg.EffectiveEntityID() == "" {
		return runtimesessions.RuntimeModeTask
	}
	return runtimesessions.RuntimeModeSession
}

func agentFlowInstance(cfg runtimeactors.AgentConfig) string {
	if v := extractStringField(cfg.Config, "flow_instance"); v != "" {
		return v
	}
	if v := extractStringField(cfg.Config, "flow_path"); v != "" {
		return v
	}
	return ""
}

func agentLLMBackend(cfg runtimeactors.AgentConfig) string {
	if v := strings.TrimSpace(cfg.LLMBackend); v != "" {
		return v
	}
	if v := extractStringField(cfg.Config, "llm_backend"); v != "" {
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

func mustJSONString(v any) string {
	b, err := json.Marshal(v)
	if err != nil || len(b) == 0 {
		return "[]"
	}
	return string(b)
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

func withConfigString(raw []byte, key, value string) []byte {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return raw
	}
	obj := map[string]any{}
	if len(raw) > 0 && json.Valid(raw) {
		_ = json.Unmarshal(raw, &obj)
	}
	obj[key] = value
	b, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return b
}

func coalesce(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func shouldFallbackLegacyAgentsSchema(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, `column "agent_id"`) ||
		strings.Contains(msg, `column "id"`) ||
		strings.Contains(msg, `column "model_tier"`) ||
		strings.Contains(msg, `column "conversation_mode"`) ||
		strings.Contains(msg, `column "llm_backend"`) ||
		strings.Contains(msg, `column "flow_instance"`) ||
		strings.Contains(msg, `column "subscriptions"`) ||
		strings.Contains(msg, `column "permissions"`) ||
		strings.Contains(msg, `column "emit_events"`) ||
		strings.Contains(msg, `column "parent_agent_id"`) ||
		strings.Contains(msg, `column "last_active_at"`) ||
		strings.Contains(msg, `column "created_at"`)
}

func shouldIgnoreLegacyConversationTable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, `relation "conversations" does not exist`) ||
		strings.Contains(msg, `column "updated_at" does not exist`)
}

func (s *PostgresStore) EnsureEntitySchema(ctx context.Context, entityID string) error {
	if strings.TrimSpace(entityID) == "" {
		return fmt.Errorf("entity_id is required")
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

func tableExists(ctx context.Context, q rowQueryer, tableName string) bool {
	if q == nil {
		return false
	}
	var exists bool
	if err := q.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'public'
			  AND table_name = $1
		)
	`, tableName).Scan(&exists); err != nil {
		return false
	}
	return exists
}

func shouldFallbackLegacyEntityState(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, `relation "entity_state" does not exist`) ||
		strings.Contains(msg, `column "slug" does not exist`)
}
