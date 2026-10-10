package eventrecord

import (
	"context"
	"database/sql"
)

type LifecycleDiagnosticEventLineage struct {
	RunID, SourceEventID string
}

// Outbox identity is deliberately global: neither run nor event-name filtering
// may conceal an extra or wrongly attributed physical diagnostic.
func ReadLifecycleDiagnosticEventLineage(ctx context.Context, tx *sql.Tx, postgres bool, outboxID string) ([]LifecycleDiagnosticEventLineage, error) {
	query := `SELECT run_id,source_event_id FROM events WHERE json_extract(payload,'$.details.outbox_id')=$1`
	if postgres {
		query = `SELECT run_id::text,source_event_id::text FROM events WHERE payload->'details'->>'outbox_id'=$1`
	}
	rows, err := tx.QueryContext(ctx, query, outboxID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LifecycleDiagnosticEventLineage
	for rows.Next() {
		var row LifecycleDiagnosticEventLineage
		if err := rows.Scan(&row.RunID, &row.SourceEventID); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func CountLifecycleDiagnosticEvents(ctx context.Context, tx *sql.Tx, postgres bool, outboxID string) (int, error) {
	query := `SELECT COUNT(*) FROM events WHERE json_extract(payload,'$.details.outbox_id')=$1`
	if postgres {
		query = `SELECT COUNT(*) FROM events WHERE payload->'details'->>'outbox_id'=$1`
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, outboxID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
