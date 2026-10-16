package llmpersistence

import (
	"context"
	"database/sql"
	"time"
)

func TerminateDeliveryConformanceSessionTx(ctx context.Context, tx *sql.Tx, id string, at time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET status='terminated',termination_reason='normal',terminated_at=$2,updated_at=$2 WHERE session_id=$1`, id, at)
	return err
}
