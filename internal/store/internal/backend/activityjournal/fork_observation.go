package activityjournal

import (
	"context"
	"database/sql"
)

func CountConnectorRunActivityAttemptsTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts WHERE run_id=$1`, run).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func CountChannelSucceededToolAttemptsTx(ctx context.Context, tx *sql.Tx, run, tool string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts WHERE run_id=$1 AND tool=$2 AND status='succeeded'`, run, tool).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func ReadActivityForkAttemptStatusStorageTx(ctx context.Context, tx *sql.Tx, runID, requestID string) (string, error) {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT status FROM activity_attempts WHERE request_event_id=$1 AND run_id=$2),'')`, requestID, runID).Scan(&status)
	return status, err
}

func CountActivityForkAttemptStorageTx(ctx context.Context, tx *sql.Tx, requestID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts WHERE request_event_id=$1`, requestID).Scan(&count)
	return count, err
}

type ActivityForkAttemptStorage struct {
	RunID, Status, ResultID string
	Generation, Failure     []byte
}

func ReadActivityForkAttemptStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, requestID string) (ActivityForkAttemptStorage, error) {
	query := `SELECT CAST(run_id AS TEXT),status,CAST(result_event_id AS TEXT),loop_generation,COALESCE(failure,'null') FROM activity_attempts WHERE request_event_id=$1`
	if postgres {
		query = `SELECT CAST(run_id AS TEXT),status,CAST(result_event_id AS TEXT),loop_generation,COALESCE(failure,'null'::jsonb) FROM activity_attempts WHERE request_event_id=$1`
	}
	var out ActivityForkAttemptStorage
	if err := tx.QueryRowContext(ctx, query, requestID).Scan(&out.RunID, &out.Status, &out.ResultID, &out.Generation, &out.Failure); err != nil {
		return ActivityForkAttemptStorage{}, err
	}
	return out, nil
}
