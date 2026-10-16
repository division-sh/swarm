package eventrecord

import (
	"context"
	"database/sql"
)

func CountChannelForkSupersessionLineageStorageTx(ctx context.Context, tx *sql.Tx, run, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events e
		JOIN run_fork_selected_contract_executions x ON x.fork_event_id=e.source_event_id AND x.fork_run_id=e.run_id
		WHERE e.run_id=$1 AND e.event_name='mailbox.card_superseded' AND x.source_event_id=$2`, run, event).Scan(&count)
	return count, err
}

func ReadChannelForkSupersessionPayloadStorageTx(ctx context.Context, tx *sql.Tx, run string) ([]byte, error) {
	var payload []byte
	err := tx.QueryRowContext(ctx, `SELECT payload FROM events WHERE run_id=$1 AND event_name='mailbox.card_superseded'`, run).Scan(&payload)
	return payload, err
}
