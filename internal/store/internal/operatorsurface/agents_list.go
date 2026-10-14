package operatorsurface

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

var _ runtimebus.ActiveAgentDescriptorLister = (*AgentPostgres)(nil)
var _ runtimebus.ActiveAgentDescriptorLister = (*AgentSQLite)(nil)

const activeAgentPostgresQuery = `
 SELECT run_id::text, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
        flow_scope_key, flow_instance_id, flow_instance, COALESCE(entity_id::text, '')
 FROM agents
 WHERE run_id = $1::uuid AND COALESCE(status, '') <> 'terminated'
 ORDER BY run_id ASC, agent_id ASC
`

const activeAgentSQLiteQuery = `
 SELECT run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
        flow_scope_key, flow_instance_id, flow_instance, COALESCE(entity_id, '')
 FROM agents
 WHERE run_id = ? AND COALESCE(status, '') <> 'terminated'
 ORDER BY run_id ASC, agent_id ASC
`

// ListActiveAgentDescriptors projects the selected run's persisted agent identities.
func (s *AgentPostgres) ListActiveAgentDescriptors(ctx context.Context, runID string) ([]runtimebus.ActiveAgentDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("db unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("active agent descriptors require exact run_id")
	}
	rows, err := s.backend.QueryContext(ctx, activeAgentPostgresQuery, runID)
	if err != nil {
		return nil, err
	}
	return scanActiveAgentDescriptors(rows, true)
}

func (s *AgentSQLite) ListActiveAgentDescriptors(ctx context.Context, runID string) ([]runtimebus.ActiveAgentDescriptor, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("active agent descriptors require exact run_id")
	}
	rows, err := s.backend.QueryContext(ctx, activeAgentSQLiteQuery, runID)
	if err != nil {
		return nil, err
	}
	return scanActiveAgentDescriptors(rows, false)
}

// A native planning owner lends the typed finite reader, never this SQL handle.
func ReadActiveAgentDescriptorsTx(ctx context.Context, tx *sql.Tx, runID string, postgres bool) ([]runtimebus.ActiveAgentDescriptor, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("active agent descriptors require exact run_id")
	}
	query := activeAgentSQLiteQuery
	if postgres {
		query = activeAgentPostgresQuery
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	return scanActiveAgentDescriptors(rows, postgres)
}

func scanActiveAgentDescriptors(rows *sql.Rows, postgres bool) ([]runtimebus.ActiveAgentDescriptor, error) {
	defer rows.Close()
	out := make([]runtimebus.ActiveAgentDescriptor, 0, 64)
	for rows.Next() {
		var descriptor runtimebus.ActiveAgentDescriptor
		var runID, agentID, nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID, flowInstance string
		if err := rows.Scan(&runID, &agentID, &nameOwner, &nameSource, &routePresence, &flowScopeKey, &flowInstanceID, &flowInstance, &descriptor.EntityID); err != nil {
			return nil, err
		}
		identity, err := agentIdentityFromColumns(runID, agentID, nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID, flowInstance)
		if err != nil {
			if postgres {
				return nil, fmt.Errorf("decode active agent descriptor: %w", err)
			}
			return nil, fmt.Errorf("decode sqlite active agent descriptor: %w", err)
		}
		descriptor.Identity = identity
		out = append(out, descriptor.Normalized())
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
