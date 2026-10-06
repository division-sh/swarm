package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

type NotifyAllChildrenItemStorage struct {
	ID, Payload, CreatedAt string
	Ordinal                int
}

type NotifyAllChildrenDiagnosticStorage struct {
	Columns []string
	Rows    [][]any
	Failure string
}

// Each diagnostic section is an independent observation. A failed PostgreSQL
// section must not poison later sections, and diagnostics cannot grant success.
func ReadNotifyAllChildrenDiagnosticStorageForTest(ctx context.Context, selected any) ([]NotifyAllChildrenDiagnosticStorage, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return nil, fmt.Errorf("observation requires an initialized postgres read owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return nil, err
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return nil, fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return nil, err
		}
		read = owner.backend.RunReadTransaction
	default:
		return nil, fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	queries := []string{
		`SELECT event_name, event_id, payload FROM events ORDER BY created_at, event_id`,
		`SELECT event_id, subscriber_type, subscriber_id, outcome, COALESCE(reason_code, ''), COALESCE(CAST(failure AS TEXT), '') FROM event_receipts ORDER BY event_id, subscriber_type, subscriber_id`,
		`SELECT event_id, subscriber_type, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(CAST(failure AS TEXT), ''), COALESCE(CAST(delivery_target_route AS TEXT), '') FROM event_deliveries ORDER BY event_id, subscriber_type, subscriber_id`,
		`SELECT flow_instance, current_state, fields FROM entity_state ORDER BY flow_instance`,
		`SELECT run_id, instance_path, flow_template, status, config FROM flow_instances ORDER BY run_id, instance_path`,
		`SELECT original_event_id, failure FROM dead_letters ORDER BY created_at`,
	}
	var evidence []NotifyAllChildrenDiagnosticStorage
	for _, query := range queries {
		var section NotifyAllChildrenDiagnosticStorage
		err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, query)
			if err != nil {
				return err
			}
			defer rows.Close()
			section.Columns, err = rows.Columns()
			if err != nil {
				return err
			}
			for rows.Next() {
				values, destinations := make([]any, len(section.Columns)), make([]any, len(section.Columns))
				for i := range values {
					destinations[i] = &values[i]
				}
				if err := rows.Scan(destinations...); err != nil {
					return err
				}
				for i, value := range values {
					if raw, ok := value.([]byte); ok {
						values[i] = string(raw)
					}
				}
				section.Rows = append(section.Rows, values)
			}
			if err := rows.Err(); err != nil {
				return err
			}
			return rows.Close()
		})
		if err != nil {
			section = NotifyAllChildrenDiagnosticStorage{Failure: err.Error()}
		}
		evidence = append(evidence, section)
	}
	return evidence, nil
}

func ReadNotifyAllChildrenItemStorageForTest(ctx context.Context, selected any, runID, sourceEventID string) ([]NotifyAllChildrenItemStorage, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return nil, fmt.Errorf("observation requires an initialized postgres read owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return nil, err
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return nil, fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return nil, err
		}
		read = owner.backend.RunReadTransaction
	default:
		return nil, fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	query := `SELECT e.event_id::text,e.payload,e.created_at,o.ordinal FROM fan_out_outcomes o JOIN events e ON e.event_id=o.event_id AND e.run_id=o.run_id WHERE o.run_id=$1::uuid AND e.event_name=$2 AND e.source_event_id=$3::uuid AND o.outcome_kind='committed' ORDER BY o.ordinal`
	if _, sqlite := selected.(*SQLiteRuntimeStore); sqlite {
		query = `SELECT e.event_id,e.payload,e.created_at,o.ordinal FROM fan_out_outcomes o JOIN events e ON e.event_id=o.event_id AND e.run_id=o.run_id WHERE o.run_id=? AND e.event_name=? AND e.source_event_id=? AND o.outcome_kind='committed' ORDER BY o.ordinal`
	}
	var evidence []NotifyAllChildrenItemStorage
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, query, runID, "portfolio/account.notify.requested", sourceEventID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row NotifyAllChildrenItemStorage
			var raw, at any
			if err := rows.Scan(&row.ID, &raw, &at, &row.Ordinal); err != nil {
				return err
			}
			switch value := raw.(type) {
			case []byte:
				row.Payload = string(value)
			case string:
				row.Payload = value
			default:
				row.Payload = fmt.Sprint(raw)
			}
			row.CreatedAt = fmt.Sprint(at)
			evidence = append(evidence, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	})
	if err != nil {
		return nil, err
	}
	return evidence, nil
}

func ReadNotifyAllChildrenMetadataStorageForTest(ctx context.Context, selected any, instancePath string) (string, error) {
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return "", fmt.Errorf("observation requires an initialized postgres read owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return "", err
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return "", fmt.Errorf("observation requires an initialized sqlite read owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return "", err
		}
		read = owner.backend.RunReadTransaction
	default:
		return "", fmt.Errorf("observation requires the original native owner, got %T", selected)
	}
	var raw any
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT fields FROM entity_state WHERE flow_instance = $1 ORDER BY updated_at DESC LIMIT 1`, instancePath).Scan(&raw)
	})
	if err != nil {
		return "", err
	}
	switch value := raw.(type) {
	case []byte:
		return string(value), nil
	case string:
		return value, nil
	default:
		return fmt.Sprint(raw), nil
	}
}
