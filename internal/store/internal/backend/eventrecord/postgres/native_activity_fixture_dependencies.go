package postgres

import (
	"context"
)

// This fixed fixture observation reads the authoritative column without
// substituting the diagnostic JSON projection or granting event admission.
func ReadActivityForkPayloadBytesStorage(ctx context.Context, q RowQueryer, runID, eventID string) ([]byte, error) {
	var raw []byte
	if err := q.QueryRowContext(ctx, `SELECT payload_bytes FROM events WHERE event_id=$1::uuid AND run_id=$2::uuid`, eventID, runID).Scan(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}
