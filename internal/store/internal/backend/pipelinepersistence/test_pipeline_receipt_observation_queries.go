package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

func countClosedSourceFanOutIssuance(ctx context.Context, tx *sql.Tx, runID, eventID string, cardinality int) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND source_event_id=$2 AND status='closed' AND cardinality=$3 AND cursor=$3 AND claim_owner IS NULL`, runID, eventID, cardinality).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func readSourceFanOutIntentDiagnosticRows(ctx context.Context, tx *sql.Tx, runID, eventID string) ([]SourceFanOutIntentDiagnosticRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT status,cardinality,cursor,COALESCE(claim_owner,''),claim_generation,CAST(lease_expires_at AS TEXT) FROM fan_out_intents WHERE run_id=$1 AND source_event_id=$2`, runID, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SourceFanOutIntentDiagnosticRow
	for rows.Next() {
		var row SourceFanOutIntentDiagnosticRow
		if err := rows.Scan(&row.Status, &row.Cardinality, &row.Cursor, &row.Owner, &row.Generation, &row.LeaseExpiry); err != nil {
			return nil, err
		}
		if value, ok := row.LeaseExpiry.([]byte); ok {
			row.LeaseExpiry = append([]byte(nil), value...)
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

func readEarliestEventPipelineReceiptRows(ctx context.Context, tx *sql.Tx) ([]EventPipelineReceiptObservationRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(e.event_id AS TEXT),e.event_name,COALESCE(r.outcome,''),COALESCE(r.reason_code,'')
FROM events e LEFT JOIN event_receipts r ON r.event_id=e.event_id
AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'
ORDER BY e.created_at,e.event_id LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventPipelineReceiptObservationRow
	for rows.Next() {
		var row EventPipelineReceiptObservationRow
		if err := rows.Scan(&row.ID, &row.Name, &row.Outcome, &row.Reason); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func readLatestPlatformPipelineReceiptStorage(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (LatestPipelineReceiptStorage, error) {
	query := `SELECT subscriber_id,outcome,COALESCE(side_effects::text,''),failure
FROM event_receipts WHERE event_id=$1::uuid AND subscriber_type='platform'
AND (subscriber_id='pipeline' OR subscriber_id LIKE 'pipeline:%') ORDER BY processed_at DESC LIMIT 1`
	if !postgres {
		query = `SELECT subscriber_id,outcome,COALESCE(side_effects,''),failure
FROM event_receipts WHERE event_id=? AND subscriber_type='platform'
AND (subscriber_id='pipeline' OR subscriber_id LIKE 'pipeline:%') ORDER BY processed_at DESC LIMIT 1`
	}
	var out LatestPipelineReceiptStorage
	var failure sql.NullString
	err := tx.QueryRowContext(ctx, query, eventID).Scan(&out.SubscriberID, &out.Outcome, &out.SideEffects, &failure)
	if errors.Is(err, sql.ErrNoRows) {
		return out, nil
	}
	if err != nil {
		return LatestPipelineReceiptStorage{}, err
	}
	out.Found = true
	if failure.Valid {
		out.Failure = make(json.RawMessage, len(failure.String))
		copy(out.Failure, failure.String)
	}
	return out, nil
}
