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
	cfgJSON, err := mergeAgentConfigJSON(rec.Config)
	if err != nil {
		return fmt.Errorf("marshal agent config: %w", err)
	}
	runtimeDescriptorJSON, err := marshalPersistedAgentRuntimeDescriptor(rec.Config)
	if err != nil {
		return fmt.Errorf("marshal agent runtime descriptor: %w", err)
	}

	var startedAt any
	if !rec.StartedAt.IsZero() {
		startedAt = rec.StartedAt
	}
	switch caps.Agents {
	case SchemaFlavorCanonical:
		return s.upsertAgentSpec(ctx, rec, cfgJSON, runtimeDescriptorJSON, startedAt)
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

func (s *PostgresStore) upsertAgentSpec(ctx context.Context, rec runtimemanager.PersistedAgent, cfgJSON, runtimeDescriptorJSON []byte, startedAt any) error {
	modelTier := agentModelTier(rec.Config)
	conversationMode := agentConversationMode(rec.Config)
	emitEvents := rec.Config.EmitEvents
	tools := rec.Config.Tools
	permissions := rec.Config.Permissions
	subscriptions := rec.Config.Subscriptions
	flowInstance := agentFlowInstance(rec.Config)
	llmBackend := agentLLMBackend(rec.Config)

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
		string(runtimeDescriptorJSON),
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
		var (
			flowInstance      string
			modelTier         string
			llmBackend        string
			conversationMode  string
			cfgRaw            []byte
			runtimeDescriptor []byte
			subscriptionsJSON []byte
			emitEventsJSON    []byte
			toolsJSON         []byte
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
			&runtimeDescriptor,
			&subscriptionsJSON,
			&emitEventsJSON,
			&toolsJSON,
			&permissionsJSON,
			&rec.Status,
			&rec.StartedAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent row: %w", err)
		}
		desc := decodePersistedAgentRuntimeDescriptor(runtimeDescriptor)
		rec.Config.ParentAgent = rec.ParentAgentID
		rec.Config.NormalizeEntityID()
		rec.Config.Config = cfgRaw
		rec.Config.Type = coalesce(desc.Type, modelTier)
		rec.Config.Mode = desc.Mode
		rec.Config.ModelTier = strings.TrimSpace(modelTier)
		rec.Config.LLMBackend = llmBackend
		rec.Config.ConversationMode = strings.TrimSpace(conversationMode)
		rec.Config.MaxTurnsPerTask = desc.MaxTurnsPerTask
		rec.Config.Subscriptions = decodeJSONStringList(subscriptionsJSON)
		rec.Config.EmitEvents = decodeJSONStringList(emitEventsJSON)
		rec.Config.Tools = decodeJSONStringList(toolsJSON)
		rec.Config.Permissions = decodeJSONStringList(permissionsJSON)
		rec.Config.NativeTools = desc.NativeTools
		rec.Config.WorkspaceClass = desc.WorkspaceClass
		rec.Config.ManagerFallback = desc.ManagerFallback
		rec.Config.FlowPath = strings.Trim(strings.TrimSpace(flowInstance), "/")
		rec.Config.NormalizeRuntimeDescriptor()
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

func agentConversationMode(cfg runtimeactors.AgentConfig) string {
	if v := strings.TrimSpace(cfg.ConversationMode); v != "" {
		return runtimesessions.NormalizeConversationRuntimeMode(v)
	}
	if cfg.EffectiveEntityID() == "" {
		return runtimesessions.RuntimeModeTask
	}
	return runtimesessions.RuntimeModeSession
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
