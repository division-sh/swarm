package eventpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

func readGlobalEventChronologyTx(ctx context.Context, tx *sql.Tx, postgres bool) ([]eventrecord.GlobalEventChronologyRow, error) {
	query := `SELECT event_name, COALESCE(entity_id,''), COALESCE(flow_instance,'') FROM events ORDER BY created_at ASC, event_id ASC`
	if postgres {
		query = `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,'') FROM events ORDER BY created_at ASC, event_id ASC`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eventrecord.GlobalEventChronologyRow
	for rows.Next() {
		var row eventrecord.GlobalEventChronologyRow
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

func readSourceRouteSettlementStorage(ctx context.Context, tx *sql.Tx, runID, eventID string) (eventrecord.SourceRouteSettlementStorage, error) {
	var out eventrecord.SourceRouteSettlementStorage
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT CAST(route_settlement AS TEXT)),COALESCE(SUM(LENGTH(CAST(route_settlement AS TEXT))),0) FROM events WHERE run_id=$1 AND source_event_id=$2`, runID, eventID).Scan(&out.Events, &out.DistinctLedgers, &out.Bytes); err != nil {
		return eventrecord.SourceRouteSettlementStorage{}, err
	}
	return out, nil
}

func countRunEventNameStorage(ctx context.Context, tx *sql.Tx, run, name string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE run_id=$1 AND event_name=$2`, run, name).Scan(&count)
	return count, err
}

func countChainDepthDiagnosticStorage(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID, handlerNode string) (int, error) {
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

func readLatestHandlerErrorLog(ctx context.Context, tx *sql.Tx, postgres bool) (json.RawMessage, error) {
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

func readCausalObservationSince(ctx context.Context, tx *sql.Tx, postgres bool, since time.Time) ([]eventrecord.CausalObservationRow, error) {
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
	out := []eventrecord.CausalObservationRow{}
	for rows.Next() {
		var row eventrecord.CausalObservationRow
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

// Outbox identity remains global; neither run nor event-name filtering may hide extra rows.
func readLifecycleDiagnosticEventLineage(ctx context.Context, tx *sql.Tx, postgres bool, outboxID string) ([]eventrecord.LifecycleDiagnosticEventLineage, error) {
	query := `SELECT run_id,source_event_id FROM events WHERE json_extract(payload,'$.details.outbox_id')=$1`
	if postgres {
		query = `SELECT run_id::text,source_event_id::text FROM events WHERE payload->'details'->>'outbox_id'=$1`
	}
	rows, err := tx.QueryContext(ctx, query, outboxID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eventrecord.LifecycleDiagnosticEventLineage
	for rows.Next() {
		var row eventrecord.LifecycleDiagnosticEventLineage
		if err := rows.Scan(&row.RunID, &row.SourceEventID); err != nil {
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

func countLifecycleDiagnosticEvents(ctx context.Context, tx *sql.Tx, postgres bool, outboxID string) (int, error) {
	query := `SELECT COUNT(*) FROM events WHERE json_extract(payload,'$.details.outbox_id')=$1`
	if postgres {
		query = `SELECT COUNT(*) FROM events WHERE payload->'details'->>'outbox_id'=$1`
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, outboxID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func readLifecycleDiagnosticProjection(ctx context.Context, tx *sql.Tx, outboxID string) ([]byte, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT projection FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1`, outboxID).Scan(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func countPendingLifecycleDiagnostic(ctx context.Context, tx *sql.Tx, outboxID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1 AND projected_at IS NULL`, outboxID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
