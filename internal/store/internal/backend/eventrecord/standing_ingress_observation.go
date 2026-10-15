package eventrecord

import (
	"context"
	"database/sql"
)

type StandingTelegramLineageStorage struct{ Events, WrongRun int }

func ReadEventNameStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (string, error) {
	query := `SELECT event_name FROM events WHERE event_id = ?`
	if postgres {
		query = `SELECT event_name FROM events WHERE event_id = $1::uuid`
	}
	var name string
	err := tx.QueryRowContext(ctx, query, eventID).Scan(&name)
	return name, err
}

func CountInboundNamedEventsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name LIKE 'inbound.%'`).Scan(&count)
	return count, err
}

func ReadStandingTelegramLineageStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) (StandingTelegramLineageStorage, error) {
	query := `SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = ? THEN 0 ELSE 1 END), 0)
		FROM events WHERE event_name = 'inbound.telegram.text_message'`
	if postgres {
		query = `SELECT COUNT(*), COALESCE(SUM(CASE WHEN run_id = $1::uuid THEN 0 ELSE 1 END), 0)
			FROM events WHERE event_name = 'inbound.telegram.text_message'`
	}
	var out StandingTelegramLineageStorage
	err := tx.QueryRowContext(ctx, query, runID).Scan(&out.Events, &out.WrongRun)
	return out, err
}
