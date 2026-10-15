package channelonboarding

import (
	"context"
	"database/sql"
)

func ReadChannelCurrentActivationOperationStorageTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var result string
	err := tx.QueryRowContext(ctx, `SELECT operation_id FROM connected_channel_activations WHERE status='current'`).Scan(&result)
	return result, err
}

func ReadChannelActivationConversationStorageTx(ctx context.Context, tx *sql.Tx, operation string) (string, error) {
	var result string
	err := tx.QueryRowContext(ctx, `SELECT conversation_reference FROM connected_channel_activations WHERE operation_id=$1`, operation).Scan(&result)
	return result, err
}

func CountChannelCurrentActivationsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var result int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM connected_channel_activations WHERE status='current'`).Scan(&result)
	return result, err
}
