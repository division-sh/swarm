package pipelinepersistence

import (
	"context"
	"database/sql"
)

type GateCompletionFlowStorage struct{ State, Accumulator, Config string }

func ReadGateCompletionFlowStorageTx(ctx context.Context, tx *sql.Tx, runID string) (GateCompletionFlowStorage, error) {
	var out GateCompletionFlowStorage
	err := tx.QueryRowContext(ctx, `SELECT e.current_state,CAST(e.accumulator AS TEXT),CAST(f.config AS TEXT)
		FROM entity_state e JOIN flow_instances f ON e.run_id=f.run_id AND e.flow_instance=f.instance_path WHERE e.run_id=$1`, runID).Scan(&out.State, &out.Accumulator, &out.Config)
	return out, err
}

func ReadGateCompletionReceiptsStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([][5]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id,subscriber_type,subscriber_id,outcome,reason_code
		FROM event_receipts WHERE event_id IN (SELECT event_id FROM events WHERE run_id=$1) ORDER BY event_id,subscriber_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][5]string
	for rows.Next() {
		var row [5]string
		if err := rows.Scan(&row[0], &row[1], &row[2], &row[3], &row[4]); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
