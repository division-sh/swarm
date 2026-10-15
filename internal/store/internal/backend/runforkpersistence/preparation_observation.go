package runforkpersistence

import (
	"context"
	"database/sql"
)

type SelectedPreparationStorage struct {
	Binding     []byte
	Fingerprint string
}

func ReadSelectedPreparationStorageTx(ctx context.Context, tx *sql.Tx, executionID string) (SelectedPreparationStorage, error) {
	var out SelectedPreparationStorage
	err := tx.QueryRowContext(ctx, `SELECT preparation_binding, preparation_fingerprint FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, executionID).Scan(&out.Binding, &out.Fingerprint)
	return out, err
}
