package eventrecord

import (
	"context"
	"database/sql"
)

func ReadActivityForkRequestPayloadStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) ([]byte, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT payload FROM events WHERE run_id=$1 AND event_id=$2`, runID, eventID).Scan(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

type ActivityForkResultStorage struct {
	EventName string
	Payload   []byte
}

func ReadActivityForkResultStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (ActivityForkResultStorage, error) {
	var out ActivityForkResultStorage
	if err := tx.QueryRowContext(ctx, `SELECT event_name,payload FROM events WHERE run_id=$1 AND event_id=$2`, runID, eventID).Scan(&out.EventName, &out.Payload); err != nil {
		return ActivityForkResultStorage{}, err
	}
	return out, nil
}
