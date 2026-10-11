package eventrecord

import (
	"context"
	"database/sql"
)

func ReadConnectorNormalizedProviderEventIDTx(ctx context.Context, tx *sql.Tx, postgres bool, run, entity, name, providerID string) (string, error) {
	query := `SELECT normalized.event_id FROM events normalized
	JOIN events evidence ON evidence.run_id=normalized.run_id AND evidence.event_name='platform.inbound_recorded'
	WHERE normalized.run_id=$1 AND normalized.entity_id=$2 AND normalized.event_name=$3
	AND json_extract(evidence.payload,'$.provider_event_id')=$4
	AND EXISTS (SELECT 1 FROM json_each(evidence.payload,'$.event_ids') ids WHERE ids.value=normalized.event_id)
	ORDER BY normalized.created_at DESC LIMIT 1`
	if postgres {
		query = `SELECT normalized.event_id::text FROM events normalized
		JOIN events evidence ON evidence.run_id=normalized.run_id AND evidence.event_name='platform.inbound_recorded'
		WHERE normalized.run_id=$1::uuid AND normalized.entity_id=$2::uuid AND normalized.event_name=$3
		AND evidence.payload->>'provider_event_id'=$4
		AND EXISTS (SELECT 1 FROM jsonb_array_elements_text(evidence.payload->'event_ids') ids(event_id) WHERE ids.event_id=normalized.event_id::text)
		ORDER BY normalized.created_at DESC LIMIT 1`
	}
	var id string
	if err := tx.QueryRowContext(ctx, query, run, entity, name, providerID).Scan(&id); err != nil {
		return "", err
	}
	return id, nil
}

func CountConnectorNamedSourceEventTx(ctx context.Context, tx *sql.Tx, run, name, source string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2 AND source_event_id=$3`, run, name, source).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func CountConnectorNamedEventIdentityTx(ctx context.Context, tx *sql.Tx, id, name string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1 AND event_name=$2`, id, name).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func ReadConnectorEventChronologyTx(ctx context.Context, tx *sql.Tx, postgres bool, run string) (string, error) {
	query := `SELECT COALESCE(group_concat(event_id || ':' || event_name, ','),'') FROM (SELECT event_id,event_name FROM events WHERE run_id=$1 ORDER BY created_at)`
	if postgres {
		query = `SELECT COALESCE(string_agg(event_id::text || ':' || event_name,',' ORDER BY created_at),'') FROM events WHERE run_id=$1::uuid`
	}
	var out string
	if err := tx.QueryRowContext(ctx, query, run).Scan(&out); err != nil {
		return "", err
	}
	return out, nil
}

func ReadChannelFirstActivitySourceRouteTx(ctx context.Context, tx *sql.Tx, run string) ([]byte, error) {
	var route []byte
	if err := tx.QueryRowContext(ctx, `SELECT source_route FROM events WHERE run_id=$1 AND event_name='platform.activity_requested' ORDER BY created_at,event_id LIMIT 1`, run).Scan(&route); err != nil {
		return nil, err
	}
	return route, nil
}
