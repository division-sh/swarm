package llmpersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

// SeedReplayDeliverySessionTx preserves the historical session association used
// by delivery, replay and trace witnesses. It does not acquire a live lease or mint a grant.
func SeedReplayDeliverySessionTx(ctx context.Context, tx *sql.Tx, postgres bool, sessionID string, identity agentidentity.Identity) error {
	fields, err := identity.StorageFields()
	if err != nil {
		return err
	}
	query := `INSERT INTO agent_sessions (
		session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		flow_scope_key, flow_instance_id, flow_instance
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if postgres {
		query = `INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance
		) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9)`
	}
	_, err = tx.ExecContext(ctx, query, sessionID, fields.RunID,
		fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	return err
}
