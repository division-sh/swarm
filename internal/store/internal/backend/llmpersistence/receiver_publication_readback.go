package llmpersistence

import (
	"context"
	"database/sql"
)

func CountReceiverLiveParsedTurnsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE run_id=$1 AND execution_mode='live' AND parse_ok=TRUE`, runID).Scan(&count)
	return count, err
}

func CountReceiverRunTurnsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}
