package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"
)

type StandaloneRunStorage struct {
	RunID, Status, TriggerEventType string
}

type StopRunControlStorage struct{ Status, Control string }

func HoldPostgresRunTableReadBarrierTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout='5s'`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `LOCK TABLE runs IN ACCESS EXCLUSIVE MODE`)
	return err
}

func ReadPostgresRunOriginLockCountTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'
  AND btrim(regexp_replace(query, '[[:space:]]+', ' ', 'g'))
      LIKE 'SELECT r.run_id::text, lower(r.status), r.bundle_hash, r.origin_kind,%'
`).Scan(&count)
	return count, err
}

func ReadPostgresDatabaseLockCountTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'`).Scan(&count)
	return count, err
}

func RemovePausedRunControlFixtureTx(ctx context.Context, tx *sql.Tx, runID string) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM run_control_state WHERE run_id=$1`, runID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("missing-control fault requires one exact run control: rows=%d", count)
	}
	return nil
}

func ContradictPausedRunControlFixtureTx(ctx context.Context, tx *sql.Tx, runID string) error {
	result, err := tx.ExecContext(ctx, `UPDATE run_control_state SET control_status='running' WHERE run_id=$1`, runID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("contradictory-control fault requires one exact run control: rows=%d", count)
	}
	return nil
}

func ReadStopRunControlStorageTx(ctx context.Context, tx *sql.Tx, runID string) (StopRunControlStorage, error) {
	var out StopRunControlStorage
	if err := tx.QueryRowContext(ctx, `SELECT r.status,COALESCE(c.control_status,'') FROM runs r LEFT JOIN run_control_state c ON c.run_id=r.run_id WHERE r.run_id=$1`, runID).Scan(&out.Status, &out.Control); err != nil {
		return StopRunControlStorage{}, err
	}
	return out, nil
}

func ReadStandaloneRunStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (StandaloneRunStorage, error) {
	if tx == nil {
		return StandaloneRunStorage{}, fmt.Errorf("standalone run evidence requires its selected read transaction")
	}
	var out StandaloneRunStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(CAST(r.run_id AS TEXT),''), COALESCE(r.status,''), COALESCE(r.trigger_event_type,'')
FROM events e INNER JOIN runs r ON r.run_id=e.run_id WHERE e.event_id=$1`, eventID).Scan(&out.RunID, &out.Status, &out.TriggerEventType)
	if err != nil {
		return StandaloneRunStorage{}, err
	}
	return out, nil
}
