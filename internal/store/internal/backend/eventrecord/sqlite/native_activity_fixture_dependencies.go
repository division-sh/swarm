package sqlite

import (
	"context"
)

func ReadActivityForkPayloadBytesStorage(ctx context.Context, q RowQueryer, runID, eventID string) ([]byte, error) {
	var raw []byte
	if err := q.QueryRowContext(ctx, `SELECT payload_bytes FROM events WHERE event_id=$1 AND run_id=$2`, eventID, runID).Scan(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}
