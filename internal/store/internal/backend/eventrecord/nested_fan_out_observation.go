package eventrecord

import (
	"context"
	"database/sql"
	"errors"
)

func ReadNestedSourceEventIDsTx(ctx context.Context, tx *sql.Tx, run, name, parent string) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2 AND source_event_id=$3 ORDER BY event_id`, run, name, parent)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func ReadNestedRunNamedEventIDsTx(ctx context.Context, tx *sql.Tx, run, name string) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2`, run, name)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func ReadNestedSiblingSourceEventIDTx(ctx context.Context, tx *sql.Tx, run, event string) (string, error) {
	var id string
	err := tx.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND source_event_id=(SELECT source_event_id FROM events WHERE event_id=$2) AND event_name=(SELECT event_name FROM events WHERE event_id=$2) AND event_id<>$2`, run, event).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

func CountNestedRunEventIdentityTx(ctx context.Context, tx *sql.Tx, run, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, run, event).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadRepeatedFanOutCompletionEventIDsTx(ctx context.Context, tx *sql.Tx, run string) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='portfolio/portfolio.notify.completed' ORDER BY event_id`, run)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
