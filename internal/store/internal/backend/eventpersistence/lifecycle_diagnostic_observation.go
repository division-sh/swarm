package eventpersistence

import (
	"context"
	"database/sql"
	"errors"
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

type SourceRevisionDiagnosticStorage struct {
	OutboxID   string
	Projected  bool
	Projection []byte
}

func ReadSourceRevisionDiagnosticStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]SourceRevisionDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT d.outbox_id,d.projected_at,d.projection
FROM agent_lifecycle_diagnostic_outbox d
JOIN agent_lifecycle_operations o ON o.operation_id=d.operation_id
WHERE d.run_id=$1 AND o.operation_kind IN ('source_set_rebind','source_set_retire','process_takeover')`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SourceRevisionDiagnosticStorage
	for rows.Next() {
		var row SourceRevisionDiagnosticStorage
		var at any
		if err := rows.Scan(&row.OutboxID, &at, &row.Projection); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		row.Projected = at != nil
		row.Projection = append([]byte(nil), row.Projection...)
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}
