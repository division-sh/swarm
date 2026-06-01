package store

import (
	"context"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

// ListActiveAgentDescriptors implements runtime.ActiveAgentDescriptorLister for
// explicit runtime delivery planning against persisted agent metadata.
func (s *PostgresStore) ListActiveAgentDescriptors(ctx context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("db unavailable")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	query := `
		SELECT
			agent_id,
			COALESCE(entity_id::text, ''),
			COALESCE(flow_instance, '')
		FROM agents
		WHERE COALESCE(status, '') <> 'terminated'
		ORDER BY agent_id ASC
	`
	switch caps.Agents {
	case SchemaFlavorCanonical:
	default:
		return nil, unsupportedSchemaCapability("agents", caps.Agents)
	}
	rows, err := s.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]runtimebus.ActiveAgentDescriptor, 0, 64)
	for rows.Next() {
		var descriptor runtimebus.ActiveAgentDescriptor
		if err := rows.Scan(&descriptor.AgentID, &descriptor.EntityID, &descriptor.FlowInstance); err != nil {
			return nil, err
		}
		if descriptor.AgentID != "" {
			out = append(out, descriptor.Normalized())
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
