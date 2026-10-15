package eventrecord

import (
	"context"
	"database/sql"
)

func ReadDirectivePayloadOperationIDStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (string, error) {
	query := `SELECT json_extract(payload, '$.operation_id') FROM events WHERE event_id = ?`
	if postgres {
		query = `SELECT payload->>'operation_id' FROM events WHERE event_id = $1::uuid`
	}
	var out string
	err := tx.QueryRowContext(ctx, query, eventID).Scan(&out)
	return out, err
}
