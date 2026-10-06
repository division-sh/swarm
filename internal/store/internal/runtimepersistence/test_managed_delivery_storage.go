package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
)

type ManagedDeliveryFailureStorage struct {
	DeliveryID, EventID string
	Failure             json.RawMessage
}
type ManagedDeliveryStorage struct {
	AgentDeliveries, Delivered int
	Failures                   []ManagedDeliveryFailureStorage
}

func ReadManagedDeliveryStorageForTest(ctx context.Context, selected any, runID, agentID string) (ManagedDeliveryStorage, error) {
	var out ManagedDeliveryStorage
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return out, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return out, err
	}
	where, args := `d.run_id=$1 AND d.subscriber_type='agent'`, []any{runID}
	if agentID != "" {
		where += ` AND d.subscriber_id=$2`
		args = append(args, agentID)
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN d.status='delivered' THEN 1 ELSE 0 END),0) FROM event_deliveries d WHERE `+where, args...).Scan(&out.AgentDeliveries, &out.Delivered); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT d.delivery_id,d.event_id,a.failure FROM event_deliveries d JOIN event_delivery_attempts a
			ON a.delivery_id=d.delivery_id AND a.claim_version=d.claim_version AND a.closure_kind='settled'
			WHERE `+where+` AND d.status='dead_letter' ORDER BY d.created_at,d.delivery_id`, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row ManagedDeliveryFailureStorage
			var failure []byte
			if err := rows.Scan(&row.DeliveryID, &row.EventID, &failure); err != nil {
				return err
			}
			row.Failure = failure
			out.Failures = append(out.Failures, row)
		}
		return rows.Err()
	})
	if err != nil {
		return ManagedDeliveryStorage{}, err
	}
	return out, nil
}
