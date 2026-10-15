package eventrecord

import (
	"context"
	"database/sql"
	"errors"
)

type SelectedExecutionEventStorage struct {
	RunID, EventName, SourceEventID, ExecutionMode string
}

func ReadSelectedExecutionEventStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (SelectedExecutionEventStorage, error) {
	var out SelectedExecutionEventStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT), event_name, COALESCE(CAST(source_event_id AS TEXT), ''), execution_mode
		FROM events WHERE event_id=$1`, eventID).Scan(&out.RunID, &out.EventName, &out.SourceEventID, &out.ExecutionMode)
	return out, err
}

type SelectedExecutionFollowUpCounts struct{ Total, Mock int }

func ReadSelectedExecutionFollowUpCountsTx(ctx context.Context, tx *sql.Tx, runID, sourceEventID string) (SelectedExecutionFollowUpCounts, error) {
	var out SelectedExecutionFollowUpCounts
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE execution_mode='mock')
		FROM events WHERE run_id=$1 AND event_name='item.processed' AND source_event_id=$2`, runID, sourceEventID).Scan(&out.Total, &out.Mock)
	return out, err
}

func ReadSelectedExecutionPayloadsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (out [][]byte, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT payload FROM events WHERE run_id=$1`, runID)
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
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		out = append(out, payload)
	}
	return out, rows.Err()
}

func CountSelectedAgentFollowUpStorageTx(ctx context.Context, tx *sql.Tx, runID, sourceEventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='task.completed'
		AND source_event_id=$2 AND produced_by='test-agent'`, runID, sourceEventID).Scan(&count)
	return count, err
}

func ReadPostgresSelectedForkEventTimelineStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(string_agg(e.event_name || ':' || COALESCE(e.payload::text,'-'),
		';' ORDER BY e.created_at,e.event_id),'') FROM events e WHERE e.run_id=$1::uuid`, runID).Scan(&out)
	return out, err
}

func ReadPostgresRunRuntimeLogArrayStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(payload ORDER BY created_at)::text,'[]')
		FROM events WHERE run_id=$1::uuid AND event_name='platform.runtime_log'`, runID).Scan(&out)
	return out, err
}

func CountPostgresSelectedForkLocalRuntimeDiagnosticsStorageTx(ctx context.Context, tx *sql.Tx, runID, sourceEventID, lineageOwner string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1::uuid AND event_name='platform.runtime_log'
		AND source_event_id=$2::uuid AND payload->'details'->>'runtime_lineage_owner'=$3
		AND payload->'details'->>'runtime_lineage_row_category'='diagnostic'
		AND payload->'details'->>'runtime_lineage_classification'='fork_local'`, runID, sourceEventID, lineageOwner).Scan(&count)
	return count, err
}
