package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func SetOperatorSnapshotAgentActiveTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity, active bool) error {
	value := "terminated"
	if active {
		value = "active"
	}
	return mutateOperatorSnapshotAgentTx(ctx, tx, identity, `status=$1`, value)
}

func CorruptOperatorSnapshotSubscriptionsTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity) error {
	return mutateOperatorSnapshotAgentTx(ctx, tx, identity, `subscriptions=$1`, `{}`)
}

func CorruptOperatorSnapshotConfigTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity) error {
	return mutateOperatorSnapshotAgentTx(ctx, tx, identity, `config=$1`, `[]`)
}

func mutateOperatorSnapshotAgentTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity, assignment string, value any) error {
	fields, err := IdentityFields(identity)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE agents SET `+assignment+` WHERE run_id=$2 AND agent_id=$3 AND agent_name_owner=$4 AND agent_name_source=$5 AND agent_route_presence=$6 AND flow_scope_key=$7 AND flow_instance_id=$8 AND flow_instance=$9`, value, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("operator snapshot agent mutation changed%d rows, want1", n)
	}
	return nil
}
