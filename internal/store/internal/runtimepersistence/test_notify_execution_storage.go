package runtimepersistence

import (
	"context"
	"database/sql"
)

func ReadNotifyAgentDeliveryStatusForTest(ctx context.Context, selected any, runID, agentID, instance string) (string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT status FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND subscriber_id=$2 AND agent_flow_instance_path=$3 ORDER BY created_at DESC,delivery_id DESC LIMIT 1`, runID, agentID, instance).Scan(&out)
	})
	if err != nil {
		return "", err
	}
	return out, nil
}
