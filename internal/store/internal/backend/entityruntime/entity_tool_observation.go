package entitystore

import (
	"context"
	"database/sql"
)

func CountRunEntityStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}

func ReadEntityToolFieldWireTx(ctx context.Context, tx *sql.Tx, runID, entityID, field string) ([]byte, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(fields -> $2, 'null'::jsonb)
		FROM entity_state WHERE run_id=$1::uuid AND entity_id=$3::uuid`, runID, field, entityID).Scan(&raw)
	return raw, err
}

type EntityToolStatusRevisionStorage struct {
	Status   string
	Revision int
}

func ReadEntityToolStatusTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (string, error) {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT fields->>'status' FROM entity_state WHERE run_id=$1::uuid AND entity_id=$2::uuid`, runID, entityID).Scan(&status)
	return status, err
}

func ReadEntityToolStatusRevisionTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (EntityToolStatusRevisionStorage, error) {
	var out EntityToolStatusRevisionStorage
	err := tx.QueryRowContext(ctx, `SELECT fields->>'status', revision FROM entity_state WHERE run_id=$1::uuid AND entity_id=$2::uuid`, runID, entityID).Scan(&out.Status, &out.Revision)
	return out, err
}
