package standingdisposition

import (
	"context"
	"database/sql"
)

func ReadChannelCurrentStandingRunStorageTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var result string
	err := tx.QueryRowContext(ctx, `SELECT current_run_id FROM standing_services WHERE current_run_id IS NOT NULL`).Scan(&result)
	return result, err
}
