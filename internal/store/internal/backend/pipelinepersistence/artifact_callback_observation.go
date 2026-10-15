package pipelinepersistence

import (
	"context"
	"database/sql"
)

func ReadPostgresArtifactCallbackReceiptsTx(ctx context.Context, tx *sql.Tx, event string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(r))::text, '[]') FROM event_receipts r WHERE event_id = $1::uuid`, event).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}
