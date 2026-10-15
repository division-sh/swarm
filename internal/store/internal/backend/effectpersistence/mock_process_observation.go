package effectpersistence

import (
	"context"
	"database/sql"
)

func CountMockPythonSettledAttemptsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE adapter = 'mock_python' AND execution_mode = 'mock' AND state = 'settled'`).Scan(&count)
	return count, err
}

func CountNonMockExternalAttemptsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE execution_mode <> 'mock'`).Scan(&count)
	return count, err
}
