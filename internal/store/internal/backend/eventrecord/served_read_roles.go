package eventrecord

import (
	"context"
	"database/sql"
)

func CountSourceNonDiagnosticEventsStorageTx(ctx context.Context, tx *sql.Tx, runID, sourceEventID, diagnosticClass, diagnosticName string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND NOT (event_class=$3 AND event_name=$4)`, runID, sourceEventID, diagnosticClass, diagnosticName).Scan(&count)
	return count, err
}

func CountGlobalBusinessEventsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name NOT LIKE 'platform.%'`).Scan(&count)
	return count, err
}

type ServedForkRuntimeDiagnosticRow struct{ EventID, RunID, Payload string }
type ServedForkRuntimeDiagnosticsStorage struct {
	Rows                      []ServedForkRuntimeDiagnosticRow
	ScanError, IterationError error
}

func ReadServedForkRuntimeDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx) (ServedForkRuntimeDiagnosticsStorage, error) {
	var out ServedForkRuntimeDiagnosticsStorage
	rows, err := tx.QueryContext(ctx, `SELECT event_id,COALESCE(CAST(run_id AS TEXT),''),CAST(payload AS TEXT) FROM events WHERE event_name='platform.runtime_log' ORDER BY created_at,event_id`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var row ServedForkRuntimeDiagnosticRow
		if err := rows.Scan(&row.EventID, &row.RunID, &row.Payload); err != nil {
			out.ScanError = err
			return out, nil
		}
		out.Rows = append(out.Rows, row)
	}
	out.IterationError = rows.Err()
	return out, nil
}

type ServedTemplateCompletionDiagnosticStorage struct {
	Columns                                 []string
	Rows                                    [][]sql.NullString
	ColumnsError, ScanError, IterationError error
}

func scanServedPhysicalDiagnosticRows(rows *sql.Rows) (ServedTemplateCompletionDiagnosticStorage, error) {
	var out ServedTemplateCompletionDiagnosticStorage
	defer rows.Close()
	out.Columns, out.ColumnsError = rows.Columns()
	if out.ColumnsError != nil {
		return out, nil
	}
	for rows.Next() {
		values := make([]sql.NullString, len(out.Columns))
		args := make([]any, len(values))
		for i := range values {
			args[i] = &values[i]
		}
		if err := rows.Scan(args...); err != nil {
			out.ScanError = err
			return out, nil
		}
		out.Rows = append(out.Rows, values)
	}
	out.IterationError = rows.Err()
	return out, nil
}
func ReadTemplateCompletionEventDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id,event_name,CAST(payload AS TEXT),routing_source_kind,CAST(source_route AS TEXT),CAST(target_route AS TEXT),CAST(route_settlement AS TEXT) FROM events WHERE run_id=$1 AND (event_name LIKE '%work.completed' OR event_name='mailbox.card_decided') ORDER BY created_at,event_id`, runID)
	if err != nil {
		return ServedTemplateCompletionDiagnosticStorage{}, err
	}
	return scanServedPhysicalDiagnosticRows(rows)
}
func ReadReporterEventSnapshotStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id,event_name,CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name <> 'platform.runtime_log' ORDER BY event_id`, runID)
	if err != nil {
		return ServedTemplateCompletionDiagnosticStorage{}, err
	}
	return scanServedPhysicalDiagnosticRows(rows)
}

func CountPhysicalEventsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count)
	return count, err
}
