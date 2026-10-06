package runtimepersistence

import (
	"context"
	"database/sql"
	"sort"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
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
		"events":           `SELECT * FROM events WHERE run_id=$1`,
		"entity_state":     `SELECT * FROM entity_state WHERE run_id=$1`,
		"entity_mutations": `SELECT * FROM entity_mutations WHERE run_id=$1`,
		"event_receipts":   `SELECT * FROM event_receipts WHERE event_id IN (SELECT event_id FROM events WHERE run_id=$1)`,
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
		tables, err := delivery.ReadSourceDeliveryStorageTables(ctx, tx, runID)
		if err != nil {
			return err
		}
		for table, snapshot := range tables {
			evidence := SelectedForkStorageTableSnapshot{Columns: snapshot.Columns, Rows: []string{}}
			for _, values := range snapshot.Rows {
				encoded, err := encodeSelectedForkSnapshotValues(values)
				if err != nil {
					return err
				}
				evidence.Rows = append(evidence.Rows, encoded)
			}
			sort.Strings(evidence.Rows)
			out[table] = evidence
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
