package budgetspendstore

import (
	"context"
	"database/sql"
)

func CountExecutionModeSpendStorageTx(ctx context.Context, tx *sql.Tx, mode string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM spend_ledger WHERE execution_mode = $1`, mode).Scan(&count)
	return count, err
}
