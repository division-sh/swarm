package eventrecord

import (
	"context"
	"database/sql"
)

type ClosedReceiverEmissionStorage struct{ EventID, ProducedBy, ProducedType, SourceEventID string }

func ReadClosedReceiverEmissionStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, eventName string) (ClosedReceiverEmissionStorage, error) {
	query := `SELECT event_id, produced_by, produced_by_type, COALESCE(source_event_id, '') FROM events WHERE run_id = ? AND event_name = ?`
	if postgres {
		query = `SELECT event_id::text, produced_by, produced_by_type, COALESCE(source_event_id::text, '') FROM events WHERE run_id = $1::uuid AND event_name = $2`
	}
	var out ClosedReceiverEmissionStorage
	err := tx.QueryRowContext(ctx, query, runID, eventName).Scan(&out.EventID, &out.ProducedBy, &out.ProducedType, &out.SourceEventID)
	return out, err
}

func ReadClosedReceiverRuntimeLogStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]string, error) {
	query := `SELECT CAST(payload AS TEXT) FROM events WHERE run_id = ? AND event_name = 'platform.runtime_log' ORDER BY created_at`
	if postgres {
		query = `SELECT payload::text FROM events WHERE run_id = $1::uuid AND event_name = 'platform.runtime_log' ORDER BY created_at`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		out = append(out, payload)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
