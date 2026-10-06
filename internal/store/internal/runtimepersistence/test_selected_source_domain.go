package runtimepersistence

import (
	"context"
	"database/sql"
)

// The inventory is fixed and run-scoped. Lifecycle/freeze and fork lineage
// remain outside this source-business immutability witness.
func ReadSelectedForkSourceDomainForTest(ctx context.Context, selected any, runID string) (map[string]SelectedForkStorageTableSnapshot, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	queries := map[string]string{
		"events":                                 `SELECT * FROM events WHERE run_id=$1`,
		"entity_state":                           `SELECT * FROM entity_state WHERE run_id=$1`,
		"entity_mutations":                       `SELECT * FROM entity_mutations WHERE run_id=$1`,
		"event_deliveries":                       `SELECT * FROM event_deliveries WHERE run_id=$1`,
		"event_receipts":                         `SELECT * FROM event_receipts WHERE event_id IN (SELECT event_id FROM events WHERE run_id=$1)`,
		"event_delivery_attempts":                `SELECT * FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)`,
		"event_delivery_handler_rule_selections": `SELECT * FROM event_delivery_handler_rule_selections WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)`,
	}
	out := make(map[string]SelectedForkStorageTableSnapshot, len(queries))
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		for table, query := range queries {
			rows, err := tx.QueryContext(ctx, query, runID)
			if err != nil {
				return err
			}
			snapshot, err := readSelectedForkSnapshotRows(rows)
			if err != nil {
				return err
			}
			out[table] = snapshot
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
