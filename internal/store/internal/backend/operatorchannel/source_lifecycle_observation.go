package operatorchannel

import (
	"context"
	"database/sql"
)

func CountSettledChannelActionIntentsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_action_intents WHERE state='settled'`).Scan(&count)
	return count, err
}

func CountSettledChannelTextIntentsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_text_intents WHERE state='settled'`).Scan(&count)
	return count, err
}
