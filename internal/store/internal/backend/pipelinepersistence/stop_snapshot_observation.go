package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
)

type StopFanOutIntentStorage struct {
	Path                                          string
	Cardinality, Cursor                           int
	Status, Claim                                 string
	Generation                                    int
	Lease, Reason, RetryAt, RetryFailure, Updated string
}

type StopFanOutOutcomeStorage struct {
	Path, Kind, EventID, Created string
	Ordinal                      int
}

func ReadStopFanOutIntentsStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]StopFanOutIntentStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT semantic_path,cardinality,cursor,status,COALESCE(claim_owner,''),claim_generation,COALESCE(CAST(lease_expires_at AS TEXT),''),COALESCE(blocked_reason,''),COALESCE(CAST(retry_ready_at AS TEXT),''),COALESCE(CAST(retry_failure AS TEXT),''),CAST(updated_at AS TEXT) FROM fan_out_intents WHERE run_id=$1 ORDER BY semantic_path`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StopFanOutIntentStorage
	for rows.Next() {
		var row StopFanOutIntentStorage
		if err := rows.Scan(&row.Path, &row.Cardinality, &row.Cursor, &row.Status, &row.Claim, &row.Generation, &row.Lease, &row.Reason, &row.RetryAt, &row.RetryFailure, &row.Updated); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadStopFanOutOutcomesStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]StopFanOutOutcomeStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT semantic_path,ordinal,outcome_kind,CAST(event_id AS TEXT),CAST(created_at AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 ORDER BY semantic_path,ordinal`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StopFanOutOutcomeStorage
	for rows.Next() {
		var row StopFanOutOutcomeStorage
		if err := rows.Scan(&row.Path, &row.Ordinal, &row.Kind, &row.EventID, &row.Created); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}
