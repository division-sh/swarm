package eventrecord

import (
	"context"
	"database/sql"
)

func CountGlobalNamedEventStorageTx(ctx context.Context, tx *sql.Tx, eventName string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name=$1`, eventName).Scan(&count)
	return count, err
}

func CountRunNamedEventStorageTx(ctx context.Context, tx *sql.Tx, runID, eventName string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2`, runID, eventName).Scan(&out)
	return out, err
}

func CountRunExactEventStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, runID, eventID).Scan(&out)
	return out, err
}

func CountRunEventOrSourceEventStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_id=$2 OR source_event_id=$2)`, runID, eventID).Scan(&count)
	return count, err
}

func CountRunEventStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}

func CountApplicationRunEventStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name NOT LIKE 'platform.%'`, runID).Scan(&count)
	return count, err
}

func CountSourceEventForeignRunStorageTx(ctx context.Context, tx *sql.Tx, eventID, sourceRunID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE source_event_id=$1 AND run_id<>$2`, eventID, sourceRunID).Scan(&count)
	return count, err
}
