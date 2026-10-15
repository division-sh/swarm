package eventrecord

import (
	"context"
	"database/sql"
)

func ReadNumericCompletionPayloadStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT CAST(payload AS TEXT) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='numeric.completed'`, runID).Scan(&raw)
	return raw, err
}

func CountNumericRunNamedEventStorageTx(ctx context.Context, tx *sql.Tx, runID, name string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name=$2`, runID, name).Scan(&count)
	return count, err
}

// The mixed-projection witness orders by event identity, not creation time.
func ReadOrderedConnectionReadyEventIDsStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='work.ready' ORDER BY event_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

type PublicationResultSourceStorage struct{ ID, Name, Payload, Kind, Route, Cause string }

type ReceiverFinishedEffectStorage struct{ ID, Source, Payload string }

type ActivityPublicationSourceStorage struct{ Name, Kind, Route, Payload string }

func ReadActivityPublicationSourceStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (ActivityPublicationSourceStorage, error) {
	var out ActivityPublicationSourceStorage
	err := tx.QueryRowContext(ctx, `SELECT event_name,routing_source_kind,CAST(source_route AS TEXT),CAST(payload AS TEXT) FROM events WHERE event_id=$1 AND run_id=$2`, eventID, runID).Scan(&out.Name, &out.Kind, &out.Route, &out.Payload)
	return out, err
}

func CountActivityPublicationResultArmsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_name LIKE 'send.%' OR event_name LIKE '%/send.%')`, runID).Scan(&count)
	return count, err
}

func ReadReceiverFinishedEffectStorageTx(ctx context.Context, tx *sql.Tx, runID, name, sourceEventID string) (ReceiverFinishedEffectStorage, error) {
	var out ReceiverFinishedEffectStorage
	err := tx.QueryRowContext(ctx, `SELECT event_id,CAST(source_route AS TEXT),CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name=$2 AND source_event_id=$3`, runID, name, sourceEventID).Scan(&out.ID, &out.Source, &out.Payload)
	return out, err
}

func ReadPublicationResultSourcesStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]PublicationResultSourceStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id,event_name,CAST(payload AS TEXT),routing_source_kind,CAST(source_route AS TEXT),COALESCE(CAST(source_event_id AS TEXT),'') FROM events WHERE run_id=$1 AND (event_name LIKE 'result.%' OR event_name LIKE '%/result.%')`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PublicationResultSourceStorage
	for rows.Next() {
		var row PublicationResultSourceStorage
		if err := rows.Scan(&row.ID, &row.Name, &row.Payload, &row.Kind, &row.Route, &row.Cause); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
