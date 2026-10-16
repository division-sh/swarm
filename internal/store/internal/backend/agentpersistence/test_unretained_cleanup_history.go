package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func RequireUnretainedCleanupActorTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity) error {
	fields, err := identity.StorageFields()
	if err != nil {
		return err
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents
		WHERE run_id=$1 AND agent_id=$2 AND agent_name_owner=$3 AND agent_name_source=$4
		AND agent_route_presence=$5 AND flow_scope_key=$6 AND flow_instance_id=$7
		AND flow_instance=$8 AND status='active'`, fields.RunID, fields.AgentID, fields.NameOwner,
		fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("unretained cleanup requires its exact original historical agent")
	}
	return nil
}
