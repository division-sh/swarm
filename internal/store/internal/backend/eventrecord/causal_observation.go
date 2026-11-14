package eventrecord

import (
	"context"
	"database/sql"
)

// CausalObservationRow is a physical witness. PayloadEntityID is a payload
// reference used by catalog assertions, not canonical envelope authority.
type CausalObservationRow struct {
	ID, Name, SourceEventID, PayloadEntityID string
}

type GlobalEventChronologyRow struct {
	Name, EntityID, FlowInstance string
}

type SourceRouteSettlementStorage struct {
	Events, DistinctLedgers, Bytes int
}

func CountScatterGatherDomainEvents(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_name IN ('batch.submitted','batch.finished','item.registered','item.finished','batch.opened') OR event_name LIKE 'workers/%/item.reported')`, runID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
