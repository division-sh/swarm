package pipelinepersistence

import (
	"context"
	"database/sql"
)

// These are physical readback witnesses, not flow admission or reconstruction.
func ReadEntityAccumulatorTextStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (string, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&raw)
	return raw, err
}

func ReadTemplateEntityAccumulatorTextStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID, template string) (string, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT CAST(accumulator AS TEXT) FROM flow_instances WHERE run_id=$1 AND entity_id=$2 AND flow_template=$3`, runID, entityID, template).Scan(&raw)
	return raw, err
}

func ReadInstanceTemplateStorageTx(ctx context.Context, tx *sql.Tx, runID, instance string) (string, error) {
	var template string
	err := tx.QueryRowContext(ctx, `SELECT flow_template FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, runID, instance).Scan(&template)
	return template, err
}

func ReadWorkflowTimerDueValueStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (any, error) {
	var raw any
	err := tx.QueryRowContext(ctx, `SELECT fire_at FROM timers WHERE run_id = $1 AND entity_id = $2 AND task_type = 'workflow_timer'`, runID, entityID).Scan(&raw)
	if bytes, ok := raw.([]byte); ok {
		raw = append([]byte(nil), bytes...)
	}
	return raw, err
}

func ReadInstanceClockAdvancedStorageTx(ctx context.Context, tx *sql.Tx, runID, instance string, previous any) (bool, error) {
	var advanced bool
	err := tx.QueryRowContext(ctx, `SELECT updated_at>$3 FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, runID, instance, previous).Scan(&advanced)
	return advanced, err
}

func ReadRunEventMutationOwnersStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT entity_id FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2`, runID, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func CountRunUnselectedTemplateActivationsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='on_demand'`, runID).Scan(&count)
	return count, err
}

type ServedForkWorkflowDiagnosticRow struct{ Path, Template, Mode, Config, Status string }
type ServedForkWorkflowDiagnosticsStorage struct {
	Rows                      []ServedForkWorkflowDiagnosticRow
	ScanError, IterationError error
}

func ReadServedForkWorkflowDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ServedForkWorkflowDiagnosticsStorage, error) {
	var out ServedForkWorkflowDiagnosticsStorage
	rows, err := tx.QueryContext(ctx, `SELECT instance_path,flow_template,mode,CAST(config AS TEXT),status FROM flow_instances WHERE run_id=$1 ORDER BY instance_path`, runID)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var row ServedForkWorkflowDiagnosticRow
		if err := rows.Scan(&row.Path, &row.Template, &row.Mode, &row.Config, &row.Status); err != nil {
			out.ScanError = err
			return out, nil
		}
		out.Rows = append(out.Rows, row)
	}
	out.IterationError = rows.Err()
	return out, nil
}

// Fixed, detached diagnostic evidence retains NULL presence, physical columns,
// successful prefixes and the phase of the original transport error.
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

func ReadReporterIntentSnapshotStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT triggering_delivery_id,flow_path,declaration_family,semantic_path,cardinality,cursor,status,claim_owner,claim_generation,CAST(lease_expires_at AS TEXT),blocked_reason,CAST(retry_ready_at AS TEXT),CAST(retry_failure AS TEXT),CAST(updated_at AS TEXT) FROM fan_out_intents WHERE run_id=$1 ORDER BY triggering_delivery_id,flow_path,declaration_family,semantic_path`, runID)
	if err != nil {
		return ServedTemplateCompletionDiagnosticStorage{}, err
	}
	return scanServedPhysicalDiagnosticRows(rows)
}

func ReadTemplateCompletionReceiptDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT r.event_id,r.subscriber_id,r.outcome,COALESCE(r.reason_code,''),CAST(r.side_effects AS TEXT) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND (e.event_name LIKE '%work.completed' OR e.event_name='mailbox.card_decided') ORDER BY r.processed_at`, runID)
	if err != nil {
		return ServedTemplateCompletionDiagnosticStorage{}, err
	}
	return scanServedPhysicalDiagnosticRows(rows)
}

func ReadReporterOutcomeSnapshotStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,CAST(event_id AS TEXT),CAST(failure AS TEXT),CAST(created_at AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 ORDER BY triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal`, runID)
	if err != nil {
		return ServedTemplateCompletionDiagnosticStorage{}, err
	}
	return scanServedPhysicalDiagnosticRows(rows)
}
