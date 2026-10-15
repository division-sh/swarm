package llmpersistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

// These are historical source rows, not current leases or conversation grants.
// The two writes remain separate fixture commits at their original clock cuts.
func SeedSelectedForkSourceSessionTx(ctx context.Context, tx *sql.Tx, sessionID string, identity agentidentity.Identity, at time.Time) error {
	fields, err := identity.StorageFields()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO agent_sessions (
		session_id, run_id, agent_id, agent_name_owner, agent_name_source,
		agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled,
		status, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, TRUE, 'active', $10, $10)`,
		sessionID, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, at)
	return err
}

func SeedSelectedForkSourceConversationAuditTx(ctx context.Context, tx *sql.Tx, auditID, entityID string, identity agentidentity.Identity, at time.Time) error {
	fields, err := identity.StorageFields()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO agent_conversation_audits (
		session_id, run_id, agent_id, agent_name_owner, agent_name_source,
		agent_route_presence, flow_scope_key, flow_instance_id, entity_id, flow_instance, memory_enabled,
		runtime_state, status, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, FALSE, '{}', 'active', $11, $11)`,
		auditID, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, entityID, fields.FlowInstancePath, at)
	return err
}

func CountForkOrSourceSessionStorageTx(ctx context.Context, tx *sql.Tx, forkRunID, sourceSessionID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE run_id=$1 OR session_id=$2`, forkRunID, sourceSessionID).Scan(&count)
	return count, err
}

func CountForkOrSourceConversationAuditStorageTx(ctx context.Context, tx *sql.Tx, forkRunID, sourceAuditID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_conversation_audits WHERE run_id=$1 OR session_id=$2`, forkRunID, sourceAuditID).Scan(&count)
	return count, err
}

func CountForkOrSourceTurnStorageTx(ctx context.Context, tx *sql.Tx, forkRunID, sourceTurnID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE run_id=$1 OR turn_id=$2`, forkRunID, sourceTurnID).Scan(&count)
	return count, err
}
