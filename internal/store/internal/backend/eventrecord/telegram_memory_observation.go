package eventrecord

import (
	"context"
	"database/sql"
	"fmt"
)

func CountTelegramRequestedReplyStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = 'telegram.reply_requested' OR event_name LIKE '%/telegram.reply_requested'`).Scan(&count)
	return count, err
}

func CountMockTelegramReplyStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name LIKE '%telegram.reply_requested' AND execution_mode = 'mock'`).Scan(&count)
	return count, err
}

func ReadMockConnectorEventDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_name, execution_mode, CAST(payload AS TEXT) FROM events WHERE event_name LIKE '%telegram%' OR event_name LIKE '%activity%' ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name, mode, payload string
		if err := rows.Scan(&name, &mode, &payload); err != nil {
			out = append(out, "scan_error="+err.Error())
			break
		}
		out = append(out, fmt.Sprintf("%s/%s=%s", name, mode, payload))
	}
	return out, nil
}
