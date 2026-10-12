package effectpersistence

import (
	"context"
	"database/sql"
)

type ClosedReceiverAuthorityStorage struct {
	AgentID, AuthorityKind, State, SurfaceActor, ExecutionKind string
}

func ReadClosedReceiverAuthorityStorageTx(ctx context.Context, tx *sql.Tx) ([]ClosedReceiverAuthorityStorage, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT o.agent_id, o.authority_kind, a.state, s.actor_id, s.execution_kind
		FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
		JOIN managed_agent_capability_surfaces s ON s.surface_id = a.capability_surface_id
		ORDER BY o.agent_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClosedReceiverAuthorityStorage
	for rows.Next() {
		var row ClosedReceiverAuthorityStorage
		if err := rows.Scan(&row.AgentID, &row.AuthorityKind, &row.State, &row.SurfaceActor, &row.ExecutionKind); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
