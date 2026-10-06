package runtimepersistence

import (
	"context"
	"database/sql"
)

type SelectedForkLifecycleDiagnosticReceipt struct {
	OutboxID   string
	Projection []byte
}

type SelectedForkLifecycleDiagnosticLog struct {
	RunID      string
	RunPresent bool
	Payload    []byte
}

type SelectedForkLifecycleDiagnosticStorage struct {
	Receipts []SelectedForkLifecycleDiagnosticReceipt
	Logs     []SelectedForkLifecycleDiagnosticLog
}

// Receipts are exact-run evidence. Logs deliberately remain global so a duplicate
// outbox ID or foreign physical run cannot disappear behind a run filter.
func ReadSelectedForkLifecycleDiagnosticStorageForTest(ctx context.Context, selected any, runID string) (SelectedForkLifecycleDiagnosticStorage, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return SelectedForkLifecycleDiagnosticStorage{}, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedForkLifecycleDiagnosticStorage{}, err
	}
	var out SelectedForkLifecycleDiagnosticStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Receipts, err = readSelectedForkLifecycleDiagnosticReceipts(ctx, tx, runID)
		if err != nil {
			return err
		}
		out.Logs, err = readSelectedForkLifecycleDiagnosticLogs(ctx, tx)
		return err
	})
	if err != nil {
		return SelectedForkLifecycleDiagnosticStorage{}, err
	}
	return out, nil
}

func readSelectedForkLifecycleDiagnosticReceipts(ctx context.Context, tx *sql.Tx, runID string) ([]SelectedForkLifecycleDiagnosticReceipt, error) {
	rows, err := tx.QueryContext(ctx, `SELECT outbox_id,projection FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1 ORDER BY outbox_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SelectedForkLifecycleDiagnosticReceipt
	for rows.Next() {
		var row SelectedForkLifecycleDiagnosticReceipt
		if err := rows.Scan(&row.OutboxID, &row.Projection); err != nil {
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

func readSelectedForkLifecycleDiagnosticLogs(ctx context.Context, tx *sql.Tx) ([]SelectedForkLifecycleDiagnosticLog, error) {
	rows, err := tx.QueryContext(ctx, `SELECT run_id,payload FROM events WHERE event_name='platform.runtime_log' ORDER BY event_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SelectedForkLifecycleDiagnosticLog
	for rows.Next() {
		var row SelectedForkLifecycleDiagnosticLog
		var run sql.NullString
		if err := rows.Scan(&run, &row.Payload); err != nil {
			return nil, err
		}
		row.RunID, row.RunPresent = run.String, run.Valid
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
