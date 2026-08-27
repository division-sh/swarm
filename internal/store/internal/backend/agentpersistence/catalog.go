package agentpersistence

import (
	"context"
	"database/sql"
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
	rows, err := s.backend.QueryContext(ctx, postgresAgentRegistryQuery)
	if err != nil {
		return nil, fmt.Errorf("query agents: %w", err)
	}
	return scanPostgresAgents(rows)
}

// LoadPostgresAgentsTx strictly decodes the runtime agent registry from the
// transaction owned by a bounded selected-store read operation.
func LoadPostgresAgentsTx(ctx context.Context, tx *sql.Tx) ([]runtimemanager.PersistedAgent, error) {
	if tx == nil {
		return nil, fmt.Errorf("load postgres agents: transaction is required")
	}
	rows, err := tx.QueryContext(ctx, postgresAgentRegistryQuery)
	if err != nil {
		return nil, fmt.Errorf("query agents: %w", err)
	}
	return scanPostgresAgents(rows)
}

const postgresAgentRegistryQuery = `
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
			runtime_descriptor,
			subscriptions,
			emit_events,
			tools,
			permissions,
			status,
			created_at,
				lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase, lifecycle_run_mode,
				lifecycle_process_authority_id::text, lifecycle_process_owner_id,
				lifecycle_process_boot_id::text, lifecycle_generation_grant_id::text,
				lifecycle_bundle_hash, lifecycle_bundle_source,
				lifecycle_runtime_instance_id::text, lifecycle_runtime_generation,
				topology_admission
		FROM agents
		WHERE status NOT IN ('terminated', 'ephemeral')
		ORDER BY created_at ASC, agent_id ASC
	`

func scanPostgresAgents(rows *sql.Rows) ([]runtimemanager.PersistedAgent, error) {
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
			&rec.ProcessBinding.ProcessAuthorityID,
			&rec.ProcessBinding.ProcessOwnerID,
			&rec.ProcessBinding.ProcessBootID,
			&rec.ProcessBinding.GenerationGrantID,
			&rec.ProcessBinding.BundleHash,
			&rec.ProcessBinding.BundleSource,
			&rec.ProcessBinding.RuntimeInstanceID,
			&rec.ProcessBinding.RuntimeGeneration,
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
		if err := rec.ProcessBinding.Validate(); err != nil {
			return nil, err
		}
		rec.Config = cfg
		if err := validateLoadedAgentRecord(rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agents rows: %w", err)
	}
	return out, nil
}

func validateLoadedAgentRecord(rec runtimemanager.PersistedAgent) error {
	agentID := strings.TrimSpace(rec.Config.ID)
	switch strings.TrimSpace(rec.Status) {
	case "active", "paused", "failed":
	default:
		return fmt.Errorf("agent %s invalid persisted status %q", agentID, rec.Status)
	}
	if rec.StartedAt.IsZero() {
		return fmt.Errorf("agent %s missing created_at", agentID)
	}
	return nil
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

func coalesce(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
