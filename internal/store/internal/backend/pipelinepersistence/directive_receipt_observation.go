package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func CountSuccessfulDirectivePipelineReceiptsStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1 AND subscriber_type = 'platform' AND subscriber_id = 'pipeline' AND outcome = 'success'`, eventID).Scan(&count)
	return count, err
}

func ReadDirectivePipelineFailureStorageTx(ctx context.Context, tx *sql.Tx, key string) ([]byte, error) {
	var failure []byte
	err := tx.QueryRowContext(ctx, `SELECT er.failure FROM event_receipts er JOIN agent_directive_operations op ON op.directive_event_id = er.event_id WHERE op.method = 'agent.send_directive' AND op.idempotency_key = $1 AND er.subscriber_type = 'platform' AND er.subscriber_id = 'pipeline'`, key).Scan(&failure)
	return failure, err
}

func ReadServedPipelineObligationDebugTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]string, string, error) {
	query := `SELECT e.event_id, e.event_name, COALESCE(r.outcome, ''), COALESCE(o.status, '') FROM events e LEFT JOIN event_receipts r ON r.event_id = e.event_id AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline' LEFT JOIN decision_card_route_obligations o ON o.event_id = e.event_id WHERE e.run_id = ? ORDER BY e.created_at, e.event_id LIMIT 12`
	if postgres {
		query = `SELECT e.event_id::text, e.event_name, COALESCE(r.outcome, ''), COALESCE(o.status, '') FROM events e LEFT JOIN event_receipts r ON r.event_id = e.event_id AND r.subscriber_type = 'platform' AND r.subscriber_id = 'pipeline' LEFT JOIN decision_card_route_obligations o ON o.event_id = e.event_id WHERE e.run_id = $1::uuid ORDER BY e.created_at, e.event_id LIMIT 12`
	}
	out := []string{}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return out, "", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return out, " columns", err
	}
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scan := make([]any, len(values))
		for i := range values {
			scan[i] = &values[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return out, " scan", err
		}
		cols := make([]string, len(values))
		for i, value := range values {
			if value.Valid {
				cols[i] = value.String
			}
		}
		out = append(out, fmt.Sprintf("%v", cols))
	}
	if err := rows.Err(); err != nil {
		return out, " rows", err
	}
	return out, "", nil
}
