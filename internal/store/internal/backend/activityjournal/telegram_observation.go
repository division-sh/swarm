package activityjournal

import (
	"context"
	"database/sql"
	"fmt"
)

func CountSucceededTelegramModeAttemptsStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, executionMode string) (int, error) {
	query := `SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message' AND execution_mode = ? AND status = 'succeeded'`
	if postgres {
		query = `SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message' AND execution_mode = $1 AND status = 'succeeded'`
	}
	var count int
	err := tx.QueryRowContext(ctx, query, executionMode).Scan(&count)
	return count, err
}

func CountTelegramToolAttemptsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message'`).Scan(&count)
	return count, err
}

func CountLiveTelegramAttemptsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts WHERE tool = 'telegram.send_message' AND execution_mode = 'live'`).Scan(&count)
	return count, err
}

func ReadMockConnectorAttemptDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT tool, execution_mode, status, CAST(COALESCE(failure, '{}') AS TEXT) FROM activity_attempts ORDER BY started_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var tool, mode, status, failure string
		if err := rows.Scan(&tool, &mode, &status, &failure); err != nil {
			out = append(out, "scan_error="+err.Error())
			break
		}
		out = append(out, fmt.Sprintf("%s/%s/%s=%s", tool, mode, status, failure))
	}
	return out, nil
}
