package eventrecord

import (
	"context"
	"database/sql"
	"errors"
)

type StandingEventNameDiagnostic struct {
	Name  string
	Count int
}

// These are display-only global projections, not success or quiescence evidence.
func ReadStandingEventNameDiagnosticsTx(ctx context.Context, tx *sql.Tx, postgres bool) ([]StandingEventNameDiagnostic, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_name, COUNT(*) FROM events GROUP BY event_name ORDER BY event_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StandingEventNameDiagnostic
	for rows.Next() {
		var row StandingEventNameDiagnostic
		if err := rows.Scan(&row.Name, &row.Count); err != nil {
			if !postgres {
				return nil, errors.Join(err, rows.Close())
			}
			continue
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadStandingRuntimeLogDiagnosticsTx(ctx context.Context, tx *sql.Tx, postgres bool) ([]string, error) {
	query := `SELECT payload FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at`
	if postgres {
		query = `SELECT payload::text FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var payload string
		if rows.Scan(&payload) == nil {
			out = append(out, payload)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}
