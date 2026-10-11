package eventrecord

import (
	"context"
	"database/sql"
)

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
