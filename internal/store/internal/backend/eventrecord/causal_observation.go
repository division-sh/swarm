package eventrecord

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// CausalObservationRow is a physical witness. PayloadEntityID is a payload
// reference used by catalog assertions, not canonical envelope authority.
type CausalObservationRow struct {
	ID, Name, SourceEventID, PayloadEntityID string
}

type GlobalEventChronologyRow struct {
	Name, EntityID, FlowInstance string
}

func ReadGlobalEventChronologyTx(ctx context.Context, tx *sql.Tx, postgres bool) ([]GlobalEventChronologyRow, error) {
	query := `SELECT event_name, COALESCE(entity_id,''), COALESCE(flow_instance,'') FROM events ORDER BY created_at ASC, event_id ASC`
	if postgres {
		query = `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,'') FROM events ORDER BY created_at ASC, event_id ASC`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GlobalEventChronologyRow
	for rows.Next() {
		var row GlobalEventChronologyRow
		if err := rows.Scan(&row.Name, &row.EntityID, &row.FlowInstance); err != nil {
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

type SourceRouteSettlementStorage struct {
	Events, DistinctLedgers, Bytes int
}

func CountScatterGatherDomainEvents(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_name IN ('batch.submitted','batch.finished','item.registered','item.finished','batch.opened') OR event_name LIKE 'workers/%/item.reported')`, runID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func ReadSourceRouteSettlementStorage(ctx context.Context, tx *sql.Tx, runID, eventID string) (SourceRouteSettlementStorage, error) {
	var out SourceRouteSettlementStorage
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT CAST(route_settlement AS TEXT)),COALESCE(SUM(LENGTH(CAST(route_settlement AS TEXT))),0) FROM events WHERE run_id=$1 AND source_event_id=$2`, runID, eventID).Scan(&out.Events, &out.DistinctLedgers, &out.Bytes); err != nil {
		return SourceRouteSettlementStorage{}, err
	}
	return out, nil
}

func CountRunEventNameStorage(ctx context.Context, tx *sql.Tx, run, name string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE run_id=$1 AND event_name=$2`, run, name).Scan(&count)
	return count, err
}

func CountChainDepthDiagnosticStorage(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID, handlerNode string) (int, error) {
	query := `SELECT COUNT(*) FROM events e
WHERE e.run_id=$1::uuid AND e.event_name='platform.dead_letter'
AND e.payload->>'entity_id'=$2
AND e.payload->'failure'->>'class'='platform.chain_depth_exceeded'
AND (e.payload->>'chain_depth')::integer=6 AND e.payload->>'handler_node'=$3`
	if !postgres {
		query = `SELECT COUNT(*) FROM events e
WHERE e.run_id=? AND e.event_name='platform.dead_letter'
AND json_extract(e.payload,'$.entity_id')=?
AND json_extract(e.payload,'$.failure.class')='platform.chain_depth_exceeded'
AND CAST(json_extract(e.payload,'$.chain_depth') AS INTEGER)=6
AND json_extract(e.payload,'$.handler_node')=?`
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, runID, entityID, handlerNode).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func ReadLatestHandlerErrorLog(ctx context.Context, tx *sql.Tx, postgres bool) (json.RawMessage, error) {
	query := `SELECT payload FROM events WHERE event_name='platform.runtime_log'
AND payload->'details'->>'action'='handler_error' ORDER BY created_at DESC LIMIT 1`
	if !postgres {
		query = `SELECT payload FROM events WHERE event_name='platform.runtime_log'
AND json_extract(payload,'$.details.action')='handler_error' ORDER BY created_at DESC LIMIT 1`
	}
	var out []byte
	if err := tx.QueryRowContext(ctx, query).Scan(&out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return append(json.RawMessage(nil), out...), nil
}

func ReadCausalObservationSince(ctx context.Context, tx *sql.Tx, postgres bool, since time.Time) ([]CausalObservationRow, error) {
	query := `SELECT event_id::text,event_name,COALESCE(source_event_id::text,''),
COALESCE(NULLIF(payload->>'entity_id',''),COALESCE(entity_id::text,''))
FROM events WHERE created_at >= $1 ORDER BY created_at ASC,event_id ASC`
	if !postgres {
		query = `SELECT event_id,event_name,COALESCE(source_event_id,''),
COALESCE(NULLIF(json_extract(payload,'$.entity_id'),''),COALESCE(entity_id,''))
FROM events WHERE created_at >= ? ORDER BY created_at ASC,event_id ASC`
	}
	rows, err := tx.QueryContext(ctx, query, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []CausalObservationRow{}
	for rows.Next() {
		var row CausalObservationRow
		if err := rows.Scan(&row.ID, &row.Name, &row.SourceEventID, &row.PayloadEntityID); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
