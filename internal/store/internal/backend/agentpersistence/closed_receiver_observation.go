package agentpersistence

import (
	"context"
	"database/sql"
)

type ClosedReceiverAgentStorage struct {
	AgentID, NameOwner, NameSource, RoutePresence, Scope, InstanceID, FlowInstance, EntityID, Phase, RunMode, Status string
	Generation                                                                                                       int64
}

func ReadClosedReceiverAgentStorageTx(ctx context.Context, tx *sql.Tx) ([]ClosedReceiverAgentStorage, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		       flow_scope_key, flow_instance_id, flow_instance, COALESCE(CAST(entity_id AS TEXT), ''),
		       lifecycle_phase, lifecycle_generation, lifecycle_run_mode, status
		FROM agents
		WHERE agent_id IN ('upstream-agent', 'downstream-agent')
		ORDER BY agent_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClosedReceiverAgentStorage
	for rows.Next() {
		var row ClosedReceiverAgentStorage
		if err := rows.Scan(&row.AgentID, &row.NameOwner, &row.NameSource, &row.RoutePresence, &row.Scope, &row.InstanceID, &row.FlowInstance, &row.EntityID, &row.Phase, &row.Generation, &row.RunMode, &row.Status); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
