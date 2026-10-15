package effectpersistence

import (
	"context"
	"database/sql"
)

type ChannelAcknowledgmentAttemptStorage struct{ OperationID, State string }

func ReadChannelAcknowledgmentAttemptStorageTx(ctx context.Context, tx *sql.Tx) (ChannelAcknowledgmentAttemptStorage, error) {
	var result ChannelAcknowledgmentAttemptStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(o.operation_id AS TEXT),a.state FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id WHERE o.authority_kind='channel_action_ack'`).Scan(&result.OperationID, &result.State)
	return result, err
}
