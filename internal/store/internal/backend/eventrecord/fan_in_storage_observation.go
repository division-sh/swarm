package eventrecord

import (
	"context"
	"database/sql"
	"errors"
)

type FanInDiagnosticEventStorage struct {
	EventID, EventName, Payload string
}

func ReadFanInPeriodReportedEventIDsTx(ctx context.Context, tx *sql.Tx) ([]string, error) {
	return readFanInReportedEventIDs(ctx, tx, "%period.reported")
}

func ReadFanInOperatingReportedEventIDsTx(ctx context.Context, tx *sql.Tx) ([]string, error) {
	return readFanInReportedEventIDs(ctx, tx, "%operating.reported")
}

// These witnesses are deliberately global and retain every LIKE match and tie.
func readFanInReportedEventIDs(ctx context.Context, tx *sql.Tx, suffix string) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name LIKE $1 ORDER BY created_at, event_id`, suffix)
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

func ReadFanInEventRouteSettlementTx(ctx context.Context, tx *sql.Tx, eventID string) (string, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT CAST(route_settlement AS TEXT) FROM events WHERE event_id=$1`, eventID).Scan(&raw)
	if err != nil {
		return "", err
	}
	return raw, nil
}

func ReadFanInDiagnosticEventsTx(ctx context.Context, tx *sql.Tx) (out []FanInDiagnosticEventStorage, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(event_id AS TEXT), event_name, CAST(payload AS TEXT) FROM events ORDER BY created_at, event_id`)
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
		var row FanInDiagnosticEventStorage
		if err := rows.Scan(&row.EventID, &row.EventName, &row.Payload); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
