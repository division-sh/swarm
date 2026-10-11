package eventpersistence

import (
	"context"
	"database/sql"
)

func ReadLifecycleDiagnosticProjection(ctx context.Context, tx *sql.Tx, outboxID string) ([]byte, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT projection FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1`, outboxID).Scan(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func CountPendingLifecycleDiagnostic(ctx context.Context, tx *sql.Tx, outboxID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1 AND projected_at IS NULL`, outboxID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
