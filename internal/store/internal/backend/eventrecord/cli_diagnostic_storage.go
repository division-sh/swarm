package eventrecord

import (
	"context"
	"database/sql"
)

func CountPostgresForkTypedRuntimeDiagnosticTx(ctx context.Context, tx *sql.Tx, run, event, owner string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events
		WHERE run_id=$1 AND event_name='platform.runtime_log' AND source_event_id=$2
		AND payload->'details'->>'runtime_lineage_owner'=$3`, run, event, owner).Scan(&count)
	return count, err
}

func ReadPostgresGlobalRuntimeDiagnosticStorageTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var rows string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object(
		'event_id', event_id, 'run_id', run_id, 'source_event_id', source_event_id, 'payload', payload
		) ORDER BY created_at)::text, '[]') FROM events WHERE event_name='platform.runtime_log'`).Scan(&rows)
	return rows, err
}
