package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type SelectedDeploymentForkDiagnosticCell struct {
	Text    string
	Present bool
}

type SelectedDeploymentForkDiagnosticSection struct {
	Name    string
	Columns []string
	Rows    [][]SelectedDeploymentForkDiagnosticCell
	Failure string
}

type SelectedDeploymentResourceWaitStorage struct {
	Intents, Cursor, Outcomes, Events, Deliveries, ReceiverInstances int
	Status, IntentBundle, Origin                                     string
	SourceKind, FeedID, Blocked, Claimant, Retry                     SelectedDeploymentForkDiagnosticCell
}

type SelectedDeploymentResourceWakeStorage struct {
	Events, Grants        int
	RunStatus, BundleHash string
}

func readSelectedDeploymentResourceEventTx(ctx context.Context, tx *sql.Tx, runID string) (string, bool, error) {
	var eventID string
	err := tx.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='root.ready' ORDER BY event_id LIMIT 1`, runID).Scan(&eventID)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	return eventID, err == nil, err
}

func (s *RunForkPostgresOwner) ReadSelectedDeploymentResourceEventForTest(ctx context.Context, runID string) (string, error) {
	var eventID string
	found := false
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		eventID, found, err = readSelectedDeploymentResourceEventTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "", sql.ErrNoRows
	}
	return eventID, nil
}

func (s *RunForkSQLiteOwner) ReadSelectedDeploymentResourceEventForTest(ctx context.Context, runID string) (string, error) {
	var eventID string
	found := false
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		eventID, found, err = readSelectedDeploymentResourceEventTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "", sql.ErrNoRows
	}
	return eventID, nil
}

func readSelectedDeploymentResourceWaitTx(ctx context.Context, tx *sql.Tx, runID string) (SelectedDeploymentResourceWaitStorage, error) {
	var out SelectedDeploymentResourceWaitStorage
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(cursor),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Intents, &out.Cursor); err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, runID).Scan(&out.Outcomes); err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&out.Events); err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, runID).Scan(&out.Deliveries); err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, runID).Scan(&out.ReceiverInstances); err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	var sourceKind, feedID, blocked, claimant, retry sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT status,bundle_hash,origin_kind,source_kind,deployment_feed_id,blocked_reason,claim_owner,retry_ready_at FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Status, &out.IntentBundle, &out.Origin, &sourceKind, &feedID, &blocked, &claimant, &retry); err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	out.SourceKind = SelectedDeploymentForkDiagnosticCell{Text: sourceKind.String, Present: sourceKind.Valid}
	out.FeedID = SelectedDeploymentForkDiagnosticCell{Text: feedID.String, Present: feedID.Valid}
	out.Blocked = SelectedDeploymentForkDiagnosticCell{Text: blocked.String, Present: blocked.Valid}
	out.Claimant = SelectedDeploymentForkDiagnosticCell{Text: claimant.String, Present: claimant.Valid}
	out.Retry = SelectedDeploymentForkDiagnosticCell{Text: retry.String, Present: retry.Valid}
	return out, nil
}

func (s *RunForkPostgresOwner) ReadSelectedDeploymentResourceWaitForTest(ctx context.Context, runID string) (SelectedDeploymentResourceWaitStorage, error) {
	var out SelectedDeploymentResourceWaitStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readSelectedDeploymentResourceWaitTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	return out, nil
}

func (s *RunForkSQLiteOwner) ReadSelectedDeploymentResourceWaitForTest(ctx context.Context, runID string) (SelectedDeploymentResourceWaitStorage, error) {
	var out SelectedDeploymentResourceWaitStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readSelectedDeploymentResourceWaitTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return SelectedDeploymentResourceWaitStorage{}, err
	}
	return out, nil
}

func readSelectedDeploymentResourceWakeTx(ctx context.Context, tx *sql.Tx, runID string) (SelectedDeploymentResourceWakeStorage, error) {
	var out SelectedDeploymentResourceWakeStorage
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='root.ready'`, runID).Scan(&out.Events); err != nil {
		return SelectedDeploymentResourceWakeStorage{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT status,bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&out.RunStatus, &out.BundleHash); err != nil {
		return SelectedDeploymentResourceWakeStorage{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generation_grants WHERE bundle_hash=$1 AND state='admitted' AND selected_binding_id IS NULL`, out.BundleHash).Scan(&out.Grants); err != nil {
		return SelectedDeploymentResourceWakeStorage{}, err
	}
	return out, nil
}

func (s *RunForkPostgresOwner) ReadSelectedDeploymentResourceWakeForTest(ctx context.Context, runID string) (SelectedDeploymentResourceWakeStorage, error) {
	var out SelectedDeploymentResourceWakeStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readSelectedDeploymentResourceWakeTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return SelectedDeploymentResourceWakeStorage{}, err
	}
	return out, nil
}

func (s *RunForkSQLiteOwner) ReadSelectedDeploymentResourceWakeForTest(ctx context.Context, runID string) (SelectedDeploymentResourceWakeStorage, error) {
	var out SelectedDeploymentResourceWakeStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readSelectedDeploymentResourceWakeTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return SelectedDeploymentResourceWakeStorage{}, err
	}
	return out, nil
}

func readSelectedDeploymentForkDiagnosticsTx(ctx context.Context, tx *sql.Tx, runID string) ([]SelectedDeploymentForkDiagnosticSection, error) {
	var sections []SelectedDeploymentForkDiagnosticSection
	for _, query := range []struct{ name, sql string }{
		{"run_control", `SELECT r.status,c.control_status FROM runs r JOIN run_control_state c ON c.run_id=r.run_id WHERE r.run_id=$1`},
		{"feeds", `SELECT CAST(deployment_feed_id AS TEXT),status,cursor,cardinality,CAST(claim_owner AS TEXT),claim_generation,CAST(lease_expires_at AS TEXT),CAST(retry_ready_at AS TEXT),CAST(retry_failure AS TEXT),CAST(blocked_reason AS TEXT) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' ORDER BY deployment_feed_id`},
		{"executions", `SELECT CAST(execution_id AS TEXT),generation,state,CAST(lease_expires_at AS TEXT),CAST(failure AS TEXT) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 ORDER BY generation`},
		{"outcomes", `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`},
		{"events", `SELECT COUNT(*) FROM events WHERE run_id=$1`},
		{"pipeline_receipts", `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'`},
		{"deliveries", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`},
		{"pending_deliveries", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress')`},
		{"failed_deliveries", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('failed','dead_letter')`},
		{"stamped_handoffs", `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND continuation_handoff_at IS NOT NULL`},
		{"receiver_instances", `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`},
		{"receipts", `SELECT CAST(r.event_id AS TEXT),r.subscriber_type,r.subscriber_id,r.outcome,CAST(r.reason_code AS TEXT),CAST(r.failure AS TEXT) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 ORDER BY e.insertion_sequence,r.subscriber_type,r.subscriber_id`},
		{"delivery_rows", `SELECT CAST(delivery_id AS TEXT),CAST(event_id AS TEXT),subscriber_type,subscriber_id,status,retry_count,claim_version,CAST(next_eligible_at AS TEXT),CAST(continuation_handoff_at AS TEXT),CAST(reason_code AS TEXT),CAST(failure AS TEXT) FROM event_deliveries WHERE run_id=$1 ORDER BY created_at,delivery_id`},
		{"attempts", `SELECT CAST(a.delivery_id AS TEXT),a.claim_version,a.closure_kind,CAST(a.outcome AS TEXT),CAST(a.reason_code AS TEXT),CAST(a.failure AS TEXT) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 ORDER BY a.delivery_id,a.claim_version`},
		{"runtime_logs", `SELECT CAST(event_id AS TEXT),CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name='platform.runtime_log' ORDER BY insertion_sequence DESC LIMIT 20`},
	} {
		rows, err := tx.QueryContext(ctx, query.sql, runID)
		if err != nil {
			return nil, fmt.Errorf("read selected fork %s: %w", query.name, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		section := SelectedDeploymentForkDiagnosticSection{Name: query.name, Columns: columns}
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			scan := make([]any, len(values))
			for i := range values {
				scan[i] = &values[i]
			}
			if err := rows.Scan(scan...); err != nil {
				return nil, errors.Join(err, rows.Close())
			}
			cells := make([]SelectedDeploymentForkDiagnosticCell, len(values))
			for i, value := range values {
				cells[i] = SelectedDeploymentForkDiagnosticCell{Text: value.String, Present: value.Valid}
			}
			section.Rows = append(section.Rows, cells)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return nil, fmt.Errorf("read selected fork %s rows: %w", query.name, err)
		}
		if query.name == "run_control" && len(section.Rows) != 1 {
			section.Failure = sql.ErrNoRows.Error()
		}
		sections = append(sections, section)
	}
	return sections, nil
}

func (s *RunForkPostgresOwner) ReadSelectedDeploymentForkDiagnosticsForTest(ctx context.Context, runID string) ([]SelectedDeploymentForkDiagnosticSection, error) {
	var sections []SelectedDeploymentForkDiagnosticSection
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		sections, err = readSelectedDeploymentForkDiagnosticsTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return sections, nil
}

func (s *RunForkSQLiteOwner) ReadSelectedDeploymentForkDiagnosticsForTest(ctx context.Context, runID string) ([]SelectedDeploymentForkDiagnosticSection, error) {
	var sections []SelectedDeploymentForkDiagnosticSection
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		sections, err = readSelectedDeploymentForkDiagnosticsTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return sections, nil
}
