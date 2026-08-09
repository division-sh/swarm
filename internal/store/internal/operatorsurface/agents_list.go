package operatorsurface

import (
	"context"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

var _ runtimebus.ActiveAgentDescriptorLister = (*AgentPostgres)(nil)
var _ runtimebus.ActiveAgentDescriptorLister = (*AgentSQLite)(nil)

// ListActiveAgentDescriptors implements runtime.ActiveAgentDescriptorLister for
// explicit runtime delivery planning against persisted agent metadata.
func (s *AgentPostgres) ListActiveAgentDescriptors(ctx context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("db unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	query := `
		SELECT
			agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance,
			COALESCE(entity_id::text, '')
		FROM agents
		WHERE COALESCE(status, '') <> 'terminated'
		ORDER BY agent_id ASC
	`
	rows, err := s.backend.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]runtimebus.ActiveAgentDescriptor, 0, 64)
	for rows.Next() {
		var descriptor runtimebus.ActiveAgentDescriptor
		var agentID, nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID, flowInstance string
		if err := rows.Scan(&agentID, &nameOwner, &nameSource, &routePresence, &flowScopeKey, &flowInstanceID, &flowInstance, &descriptor.EntityID); err != nil {
			return nil, err
		}
		descriptor.Identity, err = agentIdentityFromColumns(agentID, nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID, flowInstance)
		if err != nil {
			return nil, fmt.Errorf("decode active agent descriptor: %w", err)
		}
		out = append(out, descriptor.Normalized())
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListActiveAgentDescriptors gives SQLite the same transaction-visible
// delivery-planning surface as PostgreSQL.
func (s *AgentSQLite) ListActiveAgentDescriptors(ctx context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		       flow_scope_key, flow_instance_id, flow_instance, COALESCE(entity_id, '')
		FROM agents
		WHERE COALESCE(status, '') <> 'terminated'
		ORDER BY agent_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]runtimebus.ActiveAgentDescriptor, 0, 64)
	for rows.Next() {
		var descriptor runtimebus.ActiveAgentDescriptor
		var agentID, nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID, flowInstance string
		if err := rows.Scan(&agentID, &nameOwner, &nameSource, &routePresence, &flowScopeKey, &flowInstanceID, &flowInstance, &descriptor.EntityID); err != nil {
			return nil, err
		}
		descriptor.Identity, err = agentIdentityFromColumns(agentID, nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID, flowInstance)
		if err != nil {
			return nil, fmt.Errorf("decode sqlite active agent descriptor: %w", err)
		}
		out = append(out, descriptor.Normalized())
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
