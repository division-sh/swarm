package pipelinepersistence

import (
	"context"
	"database/sql"
	"strings"
)

func CountClosedReceiverPipelineSuccessStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) (int, error) {
	query := `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = ? AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline' AND r.outcome = 'success'`
	if postgres {
		query = `SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline' AND r.outcome = 'success'`
	}
	var out int
	err := tx.QueryRowContext(ctx, query, runID).Scan(&out)
	return out, err
}

type ClosedReceiverReceiptStorage struct{ EventName, SubscriberType, SubscriberID, Outcome, Reason, Failure string }

func ReadClosedReceiverReceiptStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]ClosedReceiverReceiptStorage, error) {
	query := `
		SELECT e.event_name, r.subscriber_type, r.subscriber_id, r.outcome,
		       COALESCE(r.reason_code, ''), COALESCE(CAST(r.failure AS TEXT), '')
		FROM event_receipts r
		JOIN events e ON e.event_id = r.event_id
		WHERE e.run_id = ?
		ORDER BY e.created_at, r.subscriber_type, r.subscriber_id`
	if postgres {
		query = strings.Replace(query, "e.run_id = ?", "e.run_id = $1::uuid", 1)
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClosedReceiverReceiptStorage
	for rows.Next() {
		var row ClosedReceiverReceiptStorage
		if err := rows.Scan(&row.EventName, &row.SubscriberType, &row.SubscriberID, &row.Outcome, &row.Reason, &row.Failure); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
