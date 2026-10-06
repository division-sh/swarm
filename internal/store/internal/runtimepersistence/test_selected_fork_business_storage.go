package runtimepersistence

import (
	"context"
	"database/sql"
)

func ReadVersionOneDeliveredSettlementCountForTest(ctx context.Context, selected any, deliveryID string) (int, error) {
	if err := validateSelectedForkStorageIdentity(deliveryID); err != nil {
		return 0, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts
			WHERE closure_kind='settled' AND delivery_id=$1 AND claim_version=1 AND outcome='delivered'`, deliveryID).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadSelectedForkAuthoredMutationForTest(ctx context.Context, selected any, runID, entityID, eventID string) (string, error) {
	for _, id := range []string{runID, entityID, eventID} {
		if err := validateSelectedForkStorageIdentity(id); err != nil {
			return "", err
		}
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	var value string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT CAST(new_value AS TEXT) FROM entity_mutations
			WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='processed_token'
			AND writer_type='platform' AND writer_id='workflow_engine' AND handler_step='mutate'`, runID, entityID, eventID).Scan(&value)
	})
	if err != nil {
		return "", err
	}
	return value, nil
}
