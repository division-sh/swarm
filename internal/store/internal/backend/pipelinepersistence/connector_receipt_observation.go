package pipelinepersistence

import (
	"context"
	"database/sql"
)

func ReadConnectorLatestActivityReceiptDiagnosticTx(ctx context.Context, tx *sql.Tx, postgres bool, run string) (string, error) {
	query := `SELECT COALESCE(r.outcome,'') || ':' || COALESCE(json_extract(r.failure,'$.class'),'') || ':' || COALESCE(json_extract(r.failure,'$.detail.code'),'') FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND e.event_name='platform.activity_requested' AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' ORDER BY r.processed_at DESC LIMIT 1`
	if postgres {
		query = `SELECT COALESCE(r.outcome,'') || ':' || COALESCE(r.failure->>'class','') || ':' || COALESCE(r.failure->'detail'->>'code','') FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1::uuid AND e.event_name='platform.activity_requested' AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' ORDER BY r.processed_at DESC LIMIT 1`
	}
	var out string
	if err := tx.QueryRowContext(ctx, query, run).Scan(&out); err != nil {
		return "", err
	}
	return out, nil
}
