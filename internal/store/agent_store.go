package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

func (s *PostgresStore) UpsertAgent(ctx context.Context, rec runtimemanager.PersistedAgent) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if rec.Config.ID == "" {
		return fmt.Errorf("agent id is required")
	}
	rec.Config.NormalizeEntityID()
	rec.Config.NormalizeRuntimeDescriptor()
	if err := agentmemory.ValidateFlowOwnership(rec.Config.Memory, rec.Config.FlowPath); err != nil {
		return fmt.Errorf("invalid agent memory plan: %w", err)
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

func (s *PostgresStore) upsertAgentSpec(ctx context.Context, rec runtimemanager.PersistedAgent, projection persistedAgentProjection, startedAt any) error {
	const q = `
		INSERT INTO agents (
			agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source,
			parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
			runtime_descriptor, status, turn_count, last_active_at, created_at
		)
		VALUES (
			$1, NULLIF($2,''), $3, $4, $5, $6, $7,
			NULLIF($8,''), NULLIF($9,'')::uuid, $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb,
			$15::jsonb, $16, 0, now(), COALESCE($17, now())
		)
		ON CONFLICT (agent_id) DO UPDATE SET
			flow_instance = EXCLUDED.flow_instance,
			role = EXCLUDED.role,
			model = EXCLUDED.model,
			llm_backend = EXCLUDED.llm_backend,
			memory_enabled = EXCLUDED.memory_enabled,
			memory_source = EXCLUDED.memory_source,
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
		projection.Model,
		projection.LLMBackend,
		projection.MemoryEnabled,
		projection.MemorySource,
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
			model,
			llm_backend,
			memory_enabled,
			memory_source,
			COALESCE(parent_agent_id, ''),
			COALESCE(entity_id::text, ''),
			config,
			COALESCE(runtime_descriptor, '{}'::jsonb),
			COALESCE(subscriptions, '[]'::jsonb),
			COALESCE(emit_events, '[]'::jsonb),
			COALESCE(tools, '[]'::jsonb),
			COALESCE(permissions, '[]'::jsonb),
			COALESCE(status, 'active'),
			COALESCE(created_at, now()),
			lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase, lifecycle_run_mode
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
		var lifecycleGeneration int64
		if err := rows.Scan(
			&row.AgentID,
			&row.FlowInstance,
			&row.Role,
			&row.Model,
			&row.LLMBackend,
			&row.MemoryEnabled,
			&row.MemorySource,
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
			&rec.LifecycleEpoch,
			&lifecycleGeneration,
			&rec.LifecyclePhase,
			&rec.LifecycleRunMode,
		); err != nil {
			return nil, fmt.Errorf("scan agent row: %w", err)
		}
		cfg, err := hydratePersistedAgentConfig(row)
		if err != nil {
			return nil, fmt.Errorf("hydrate agent row %s: %w", strings.TrimSpace(row.AgentID), err)
		}
		rec.ParentAgentID = row.ParentAgentID
		rec.LifecycleGeneration = uint64(lifecycleGeneration)
		rec.Config = cfg
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agents rows: %w", err)
	}
	return out, nil
}

func agentModel(cfg runtimeactors.AgentConfig) (string, error) {
	alias, err := llmselection.RequireModelAlias(cfg.Model)
	if err != nil {
		return "", err
	}
	return alias, nil
}

func agentPersistedType(cfg runtimeactors.AgentConfig, modelAlias string) string {
	if v := strings.TrimSpace(cfg.Type); v != "" {
		return v
	}
	if v := strings.TrimSpace(modelAlias); v != "" {
		return v
	}
	return "generic"
}

func agentFlowInstance(cfg runtimeactors.AgentConfig) string {
	if v := strings.TrimSpace(cfg.FlowPath); v != "" {
		return v
	}
	return ""
}

func agentLLMBackend(cfg runtimeactors.AgentConfig) (string, error) {
	if v := strings.TrimSpace(cfg.LLMBackend); v != "" {
		profile, err := llmselection.ResolvePersistedBackend(v)
		if err != nil {
			return "", err
		}
		return profile.ID, nil
	}
	return llmselection.DefaultBackendID(), nil
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
	identity, err := runtimecurrentstate.RequireIdentity(ctx, entityID)
	if err != nil {
		return fmt.Errorf("lookup entity slug: %w", err)
	}
	var slug string
	err = s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
	`, identity.RunID, identity.EntityID).Scan(&slug)
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
