package eventrecord

import (
	"context"
	"database/sql"
	"errors"
)

// The existing exact-one consumer needs only an ambiguity witness. The
// ordered history consumer must retain every row at its original physical cut.
func ReadRunNamedEventIdentityCandidatesTx(ctx context.Context, tx *sql.Tx, runID, eventName string) ([]string, error) {
	return readRunNamedEventIDs(ctx, tx, `SELECT CAST(event_id AS TEXT) FROM events WHERE run_id=$1 AND event_name=$2 LIMIT 2`, runID, eventName)
}

func ReadRunNamedEventIdentityHistoryTx(ctx context.Context, tx *sql.Tx, runID, eventName string) ([]string, error) {
	return readRunNamedEventIDs(ctx, tx, `SELECT CAST(event_id AS TEXT) FROM events WHERE run_id=$1 AND event_name=$2 ORDER BY created_at,event_id`, runID, eventName)
}

func readRunNamedEventIDs(ctx context.Context, tx *sql.Tx, query, runID, eventName string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, runID, eventName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, 2)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return ids, nil
}
