package eventrecord

import (
	"context"
	"database/sql"
	"errors"
)

func ReadLifecycleProjectionLogPayloadsTx(ctx context.Context, tx *sql.Tx) ([][]byte, error) {
	rows, err := tx.QueryContext(ctx, `SELECT payload FROM events WHERE event_name='platform.runtime_log'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, append([]byte(nil), raw...))
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadRunLifecycleProjectionLogPayloadsTx(ctx context.Context, tx *sql.Tx, runID string) ([][]byte, error) {
	rows, err := tx.QueryContext(ctx, `SELECT payload FROM events WHERE event_name='platform.runtime_log' AND run_id=$1`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, append([]byte(nil), raw...))
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}
