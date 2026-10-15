package entitystore

import (
	"context"
	"database/sql"
)

type NumericRunEntityStorage struct{ Fields, State string }

func ReadNumericRunEntityStorageTx(ctx context.Context, tx *sql.Tx, runID string) (NumericRunEntityStorage, error) {
	var out NumericRunEntityStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(fields AS TEXT), current_state FROM entity_state WHERE CAST(run_id AS TEXT)=$1`, runID).Scan(&out.Fields, &out.State)
	return out, err
}

func ReadNumericRunEntityIdentityStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var id string
	err := tx.QueryRowContext(ctx, `SELECT entity_id FROM entity_state WHERE run_id=$1`, runID).Scan(&id)
	return id, err
}

func ReadReceiverParentEntityIdentityStorageTx(ctx context.Context, tx *sql.Tx, runID, instance string) (string, error) {
	var id string
	err := tx.QueryRowContext(ctx, `SELECT entity_id FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, runID, instance).Scan(&id)
	return id, err
}
