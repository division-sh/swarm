package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/effects"
)

func ReadStartFailedCompensationCountForTest(ctx context.Context, selected any, token effects.LifecycleToken) (int, error) {
	if !token.Valid() {
		return 0, fmt.Errorf("compensation evidence requires an exact lifecycle token")
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_lifecycle_operations o JOIN agent_lifecycle_transition_facts f ON f.operation_id=o.operation_id
			WHERE o.run_id=$1 AND o.agent_id=$2 AND o.flow_instance=$3 AND o.operation_kind='self_release' AND o.target_generation=$4
			AND o.target_phase='registered' AND o.run_mode='stopped' AND o.state='succeeded' AND f.trigger='start_failed'
			AND f.previous_generation=$4 AND f.next_generation=$4`, token.Identity.RunID, token.Identity.AgentID(), token.Identity.FlowInstance(), token.Generation).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
