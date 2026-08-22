package agentpersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func (s *AgentSQLiteOwner) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	rows, err := s.backend.QueryContext(ctx, `
		SELECT agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		       flow_scope_key, flow_instance_id, flow_instance,
		       role, model, llm_backend, memory_enabled, memory_source,
		       COALESCE(parent_agent_id, ''), COALESCE(entity_id, ''), config, COALESCE(runtime_descriptor, '{}'),
		       COALESCE(subscriptions, '[]'), COALESCE(emit_events, '[]'), COALESCE(tools, '[]'), COALESCE(permissions, '[]'),
		       COALESCE(status, 'active'), COALESCE(created_at, CURRENT_TIMESTAMP),
			       lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase, lifecycle_run_mode,
			       lifecycle_process_authority_id, lifecycle_process_owner_id,
			       lifecycle_process_boot_id, lifecycle_generation_grant_id,
			       lifecycle_bundle_hash, lifecycle_bundle_source,
			       lifecycle_runtime_instance_id, lifecycle_runtime_generation,
			       topology_admission
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
		var topologyRaw []byte
		if err := rows.Scan(&row.AgentID, &row.Identity.NameOwner, &row.Identity.NameSource, &row.Identity.RoutePresence,
			&row.Identity.FlowScopeKey, &row.Identity.FlowInstanceID, &row.Identity.FlowInstancePath,
			&row.Role, &row.Model, &row.LLMBackend, &row.MemoryEnabled, &row.MemorySource,
			&row.ParentAgentID, &row.EntityID, &row.ConfigJSON, &row.RuntimeDescriptor, &row.SubscriptionsJSON, &row.EmitEventsJSON,
			&row.ToolsJSON, &row.PermissionsJSON, &rec.Status, &startedAt, &rec.LifecycleEpoch, &lifecycleGeneration, &rec.LifecyclePhase, &rec.LifecycleRunMode,
			&rec.ProcessBinding.ProcessAuthorityID, &rec.ProcessBinding.ProcessOwnerID,
			&rec.ProcessBinding.ProcessBootID, &rec.ProcessBinding.GenerationGrantID,
			&rec.ProcessBinding.BundleHash, &rec.ProcessBinding.BundleSource,
			&rec.ProcessBinding.RuntimeInstanceID, &rec.ProcessBinding.RuntimeGeneration,
			&topologyRaw); err != nil {
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
		if err := json.Unmarshal(topologyRaw, &rec.Topology); err != nil {
			return nil, fmt.Errorf("decode sqlite agent topology admission: %w", err)
		}
		if err := rec.Topology.Validate(); err != nil {
			return nil, err
		}
		if err := rec.ProcessBinding.Validate(); err != nil {
			return nil, err
		}
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
