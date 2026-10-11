package eventrecord

import (
	"context"
	"database/sql"
)

type BusRoutingEventDiagnostic struct{ Name, EntityID, FlowInstance, Payload string }

func CountUndeclaredBusChildEventsTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name IN ('child/validate.start', 'child/validation.done')`).Scan(&count)
	return count, err
}

func ReadBusRoutingEventDiagnosticsTx(ctx context.Context, tx *sql.Tx, postgres bool) ([]BusRoutingEventDiagnostic, error) {
	query := `SELECT event_name, COALESCE(entity_id,''), COALESCE(flow_instance,''), COALESCE(CAST(payload AS TEXT),'') FROM events ORDER BY created_at ASC, event_id ASC`
	if postgres {
		query = `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,''), COALESCE(payload::text,'') FROM events ORDER BY created_at ASC, event_id ASC`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BusRoutingEventDiagnostic
	for rows.Next() {
		var row BusRoutingEventDiagnostic
		if err := rows.Scan(&row.Name, &row.EntityID, &row.FlowInstance, &row.Payload); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func ReadBusRoutingEventNamesTx(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_name FROM events ORDER BY created_at ASC, event_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, nil
}
