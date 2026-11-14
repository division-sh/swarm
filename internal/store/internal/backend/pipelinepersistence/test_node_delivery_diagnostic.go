package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

type NodeDeliveryDiagnosticStorage struct {
	Rows, DeadLetters, Receipts []string
}

func (s *PipelinePostgresOwner) ReadNodeDeliveryDiagnosticStorageForTest(ctx context.Context, eventID string) (NodeDeliveryDiagnosticStorage, error) {
	var out NodeDeliveryDiagnosticStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readNodeDeliveryDiagnosticStorageTx(ctx, tx, true, eventID)
		return err
	})
	if err != nil {
		return NodeDeliveryDiagnosticStorage{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ReadNodeDeliveryDiagnosticStorageForTest(ctx context.Context, eventID string) (NodeDeliveryDiagnosticStorage, error) {
	var out NodeDeliveryDiagnosticStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readNodeDeliveryDiagnosticStorageTx(ctx, tx, false, eventID)
		return err
	})
	if err != nil {
		return NodeDeliveryDiagnosticStorage{}, err
	}
	return out, nil
}

func readNodeDeliveryDiagnosticStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (NodeDeliveryDiagnosticStorage, error) {
	var out NodeDeliveryDiagnosticStorage
	var err error
	out.Rows, err = readNodeDeliveryDiagnosticLinesTx(ctx, tx, postgres, eventID)
	if err != nil {
		return NodeDeliveryDiagnosticStorage{}, fmt.Errorf("delivery dump: %w", err)
	}
	out.DeadLetters, err = readNodeDeadLetterDiagnosticLinesTx(ctx, tx, postgres, eventID)
	if err != nil {
		return NodeDeliveryDiagnosticStorage{}, fmt.Errorf("dead letter dump: %w", err)
	}
	out.Receipts, err = readNodeReceiptDiagnosticLinesTx(ctx, tx, postgres, eventID)
	if err != nil {
		return NodeDeliveryDiagnosticStorage{}, fmt.Errorf("receipt dump: %w", err)
	}
	return out, nil
}

func readNodeDeliveryDiagnosticLinesTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) ([]string, error) {
	query := `SELECT subscriber_id, COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(delivery_target_route, '')
FROM event_deliveries WHERE event_id = ? ORDER BY subscriber_id, delivery_target_route`
	if postgres {
		query = `SELECT subscriber_id, COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(delivery_target_route::text, '')
FROM event_deliveries WHERE event_id = $1::uuid ORDER BY subscriber_id, delivery_target_route::text`
	}
	rows, err := tx.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var subscriber, status, reason, target string
		if err := rows.Scan(&subscriber, &status, &reason, &target); err != nil {
			return nil, err
		}
		out = append(out, subscriber+" status="+status+" reason="+reason+" target="+target)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func readNodeDeadLetterDiagnosticLinesTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) ([]string, error) {
	query := `SELECT COALESCE(handler_node, ''), COALESCE(json_extract(failure, '$.class'), ''), COALESCE(failure, '')
FROM dead_letters WHERE original_event_id = ? ORDER BY created_at ASC`
	if postgres {
		query = `SELECT COALESCE(handler_node, ''), COALESCE(failure->>'class', ''), COALESCE(failure::text, '')
FROM dead_letters WHERE original_event_id = $1::uuid ORDER BY created_at ASC`
	}
	rows, err := tx.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var node, failure, message string
		if err := rows.Scan(&node, &failure, &message); err != nil {
			return nil, err
		}
		out = append(out, node+" failure="+failure+" detail="+message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func readNodeReceiptDiagnosticLinesTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) ([]string, error) {
	query := `SELECT subscriber_type || '/' || subscriber_id,
outcome || ' reason=' || COALESCE(reason_code, '') || ' failure=' || COALESCE(failure, '')
FROM event_receipts WHERE event_id = ? ORDER BY subscriber_type, subscriber_id`
	if postgres {
		query = `SELECT subscriber_type || '/' || subscriber_id,
outcome || ' reason=' || COALESCE(reason_code, '') || ' failure=' || COALESCE(failure::text, '')
FROM event_receipts WHERE event_id = $1::uuid ORDER BY subscriber_type, subscriber_id`
	}
	rows, err := tx.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var subscriber, outcome string
		if err := rows.Scan(&subscriber, &outcome); err != nil {
			return nil, err
		}
		out = append(out, subscriber+" outcome="+outcome)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}
