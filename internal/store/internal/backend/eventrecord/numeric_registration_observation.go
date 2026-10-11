package eventrecord

import (
	"context"
	"database/sql"
	"errors"
)

type NumericRegistrationStorage struct {
	ID      string
	Payload []byte
}

// Preserve the physical registration cut, not the fan-out outcome catalog or
// canonical event decoding. Repeated account rows remain ordered evidence.
func ReadNumericRegistrationStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]NumericRegistrationStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(event_id AS TEXT),payload FROM events WHERE run_id=$1 AND event_name=$2 ORDER BY created_at,event_id`, runID, "portfolio/account.registered")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NumericRegistrationStorage
	for rows.Next() {
		var row NumericRegistrationStorage
		if err := rows.Scan(&row.ID, &row.Payload); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		row.Payload = append([]byte(nil), row.Payload...)
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}
