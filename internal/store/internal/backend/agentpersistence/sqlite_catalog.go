package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func (s *AgentSQLiteOwner) UpsertAgent(ctx context.Context, rec runtimemanager.PersistedAgent) error {
	if err := s.requireCurrentSchema(); err != nil {
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
	projection, err := ProjectPersistedAgentConfig(rec.Config, rec.ParentAgentID)
	if err != nil {
		return err
	}
	startedAt := rec.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	if err := s.backend.RunTransaction(ctx, "sqlite agent upsert", func(txctx context.Context, tx *sql.Tx) error {
		if err := AuthorizeSQLiteRawAgentTopologyMutation(txctx, tx, rec); err != nil {
			return err
		}
		_, err := tx.ExecContext(txctx, `
			INSERT INTO agents (
				agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance,
				role, model, llm_backend, memory_enabled, memory_source,
				parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
				runtime_descriptor, status, turn_count, last_active_at, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
			ON CONFLICT(
				agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance
			) DO UPDATE SET
				role = excluded.role, model = excluded.model, llm_backend = excluded.llm_backend,
				memory_enabled = excluded.memory_enabled, memory_source = excluded.memory_source,
				parent_agent_id = excluded.parent_agent_id, entity_id = excluded.entity_id,
				config = excluded.config, subscriptions = excluded.subscriptions,
				emit_events = excluded.emit_events, tools = excluded.tools,
				permissions = excluded.permissions, runtime_descriptor = excluded.runtime_descriptor,
				status = excluded.status, last_active_at = excluded.last_active_at
		`, projection.Identity.AgentID, projection.Identity.NameOwner, projection.Identity.NameSource, projection.Identity.RoutePresence,
			projection.Identity.FlowScopeKey, projection.Identity.FlowInstanceID, projection.Identity.FlowInstancePath,
			projection.Role, projection.Model, projection.LLMBackend, projection.MemoryEnabled, projection.MemorySource,
			nullString(projection.ParentAgentID), nullUUID(projection.EntityID), string(projection.ConfigJSON), string(projection.SubscriptionsJSON),
			string(projection.EmitEventsJSON), string(projection.ToolsJSON), string(projection.PermissionsJSON), string(projection.RuntimeDescriptor),
			agentPersistedStatus(rec.Status), time.Now().UTC(), startedAt.UTC())
		return err
	}); err != nil {
		return fmt.Errorf("upsert sqlite agent: %w", err)
	}
	return nil
}

func (s *AgentSQLiteOwner) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	rows, err := s.backend.QueryContext(ctx, `
		SELECT agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		       flow_scope_key, flow_instance_id, flow_instance,
		       role, model, llm_backend, memory_enabled, memory_source,
		       COALESCE(parent_agent_id, ''), COALESCE(entity_id, ''), config, COALESCE(runtime_descriptor, '{}'),
		       COALESCE(subscriptions, '[]'), COALESCE(emit_events, '[]'), COALESCE(tools, '[]'), COALESCE(permissions, '[]'),
		       COALESCE(status, 'active'), COALESCE(created_at, CURRENT_TIMESTAMP),
		       lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase, lifecycle_run_mode
		FROM agents
		WHERE status NOT IN ('terminated', 'ephemeral')
		ORDER BY created_at ASC, agent_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query sqlite agents: %w", err)
	}
	defer rows.Close()
	out := make([]runtimemanager.PersistedAgent, 0)
	for rows.Next() {
		var rec runtimemanager.PersistedAgent
		var row PersistedAgentProjection
		var startedAt any
		var lifecycleGeneration int64
		if err := rows.Scan(&row.AgentID, &row.Identity.NameOwner, &row.Identity.NameSource, &row.Identity.RoutePresence,
			&row.Identity.FlowScopeKey, &row.Identity.FlowInstanceID, &row.Identity.FlowInstancePath,
			&row.Role, &row.Model, &row.LLMBackend, &row.MemoryEnabled, &row.MemorySource,
			&row.ParentAgentID, &row.EntityID, &row.ConfigJSON, &row.RuntimeDescriptor, &row.SubscriptionsJSON, &row.EmitEventsJSON,
			&row.ToolsJSON, &row.PermissionsJSON, &rec.Status, &startedAt, &rec.LifecycleEpoch, &lifecycleGeneration, &rec.LifecyclePhase, &rec.LifecycleRunMode); err != nil {
			return nil, fmt.Errorf("scan sqlite agent: %w", err)
		}
		if at, ok, err := sqliteTime(startedAt); err != nil {
			return nil, fmt.Errorf("scan sqlite agent created_at: %w", err)
		} else if ok {
			rec.StartedAt = at
		}
		row.Identity.AgentID = row.AgentID
		row.FlowInstance = row.Identity.FlowInstancePath
		cfg, err := HydratePersistedAgentConfig(row)
		if err != nil {
			return nil, fmt.Errorf("hydrate sqlite agent row %s: %w", strings.TrimSpace(row.AgentID), err)
		}
		rec.ParentAgentID = row.ParentAgentID
		rec.LifecycleGeneration = uint64(lifecycleGeneration)
		rec.Config = cfg
		out = append(out, rec)
	}
	return out, rows.Err()
}

func nullString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func nullUUID(raw string) any {
	return nullString(raw)
}

func sqliteTime(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return value.UTC(), true, nil
	case string:
		return parseSQLiteTime(value)
	case []byte:
		return parseSQLiteTime(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported sqlite time value %T", raw)
	}
}

func parseSQLiteTime(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, layout := range formats {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, fmt.Errorf("parse sqlite time %q: %w", raw, lastErr)
}
