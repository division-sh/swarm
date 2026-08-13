package agentpersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func (s *AgentPostgresOwner) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	return s.loadAgentsSpec(ctx)
}

func (s *AgentPostgresOwner) LoadAgentsSpec(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.loadAgentsSpec(ctx)
}

func (s *AgentPostgresOwner) loadAgentsSpec(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	const q = `
		SELECT
			agent_id,
			agent_name_owner,
			agent_name_source,
			agent_route_presence,
			flow_scope_key,
			flow_instance_id,
			flow_instance,
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
			lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase, lifecycle_run_mode,
			topology_admission
		FROM agents
		WHERE status NOT IN ('terminated', 'ephemeral')
		ORDER BY created_at ASC, agent_id ASC
	`
	rows, err := s.backend.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query agents: %w", err)
	}
	defer rows.Close()

	var out []runtimemanager.PersistedAgent
	for rows.Next() {
		var rec runtimemanager.PersistedAgent
		var row PersistedAgentProjection
		var lifecycleGeneration int64
		var topologyRaw []byte
		if err := rows.Scan(
			&row.AgentID,
			&row.Identity.NameOwner,
			&row.Identity.NameSource,
			&row.Identity.RoutePresence,
			&row.Identity.FlowScopeKey,
			&row.Identity.FlowInstanceID,
			&row.Identity.FlowInstancePath,
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
			&topologyRaw,
		); err != nil {
			return nil, fmt.Errorf("scan agent row: %w", err)
		}
		row.Identity.AgentID = row.AgentID
		row.FlowInstance = row.Identity.FlowInstancePath
		cfg, err := HydratePersistedAgentConfig(row)
		if err != nil {
			return nil, fmt.Errorf("hydrate agent row %s: %w", strings.TrimSpace(row.AgentID), err)
		}
		rec.ParentAgentID = row.ParentAgentID
		rec.LifecycleGeneration = uint64(lifecycleGeneration)
		if err := json.Unmarshal(topologyRaw, &rec.Topology); err != nil {
			return nil, fmt.Errorf("decode agent topology admission: %w", err)
		}
		if err := rec.Topology.Validate(); err != nil {
			return nil, err
		}
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

func agentFlowInstance(cfg runtimeactors.AgentConfig) string {
	if v := strings.TrimSpace(cfg.FlowPath); v != "" {
		return v
	}
	return ""
}

func agentLLMBackend(cfg runtimeactors.AgentConfig) (string, error) {
	v := strings.TrimSpace(cfg.ResolvedLLMBackend)
	if v == "" {
		return "", fmt.Errorf("resolved llm backend is required before persistence")
	}
	profile, err := llmselection.ResolvePersistedBackend(v)
	if err != nil {
		return "", err
	}
	return profile.ID, nil
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
