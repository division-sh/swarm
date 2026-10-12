package eventrecord

import (
	"context"
	"database/sql"
	"errors"
)

func CountGateSourceNamedEventsTx(ctx context.Context, tx *sql.Tx, name, source string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = $1 AND source_event_id = $2`, name, source).Scan(&count)
	return count, err
}

func ReadGateRunEventNamesTx(ctx context.Context, tx *sql.Tx, run string) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_name FROM events WHERE run_id = $1 ORDER BY created_at`, run)
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
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func ReadTemplateNamedEventIdentityTx(ctx context.Context, tx *sql.Tx, name string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name=$1`, name).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadTemplateSourceNamedEventIdentityTx(ctx context.Context, tx *sql.Tx, name, source string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name=$1 AND source_event_id=$2`, name, source).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadTemplateEventPayloadTextTx(ctx context.Context, tx *sql.Tx, event string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT CAST(payload AS TEXT) FROM events WHERE event_id=$1`, event).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

func CountTemplateEventIdentityTx(ctx context.Context, tx *sql.Tx, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1`, event).Scan(&count)
	return count, err
}

// ReadStageLifecycleRuntimeLogPayloadsTx preserves physical payload text, not
// decoded event bytes, for the bounded whole-store failure diagnostic.
func ReadStageLifecycleRuntimeLogPayloadsTx(ctx context.Context, tx *sql.Tx) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT payload FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at DESC LIMIT 20`)
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
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		out = append(out, payload)
	}
	return out, rows.Err()
}
