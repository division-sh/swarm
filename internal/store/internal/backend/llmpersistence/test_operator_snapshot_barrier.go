package llmpersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func HoldOperatorSessionSnapshotTx(ctx context.Context, tx *sql.Tx, postgres bool, id int64) error {
	if postgres {
		_, err := tx.ExecContext(ctx, `LOCK TABLE agent_sessions IN ACCESS EXCLUSIVE MODE`)
		return err
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE agent_sessions RENAME TO agent_sessions_snapshot_source`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW agent_sessions AS SELECT * FROM agent_sessions_snapshot_source WHERE agent_snapshot_barrier(%d)=1`, id))
	return err
}

func RestoreOperatorSessionSnapshotTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `DROP VIEW agent_sessions`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `ALTER TABLE agent_sessions_snapshot_source RENAME TO agent_sessions`)
	return err
}

func HoldOperatorTurnSnapshotTx(ctx context.Context, tx *sql.Tx, postgres bool, id int64) error {
	if postgres {
		_, err := tx.ExecContext(ctx, `LOCK TABLE agent_turns IN ACCESS EXCLUSIVE MODE`)
		return err
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE agent_turns RENAME TO agent_turns_snapshot_source`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW agent_turns AS SELECT * FROM agent_turns_snapshot_source WHERE agent_snapshot_barrier(%d)=1`, id))
	return err
}

func RestoreOperatorTurnSnapshotTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `DROP VIEW agent_turns`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `ALTER TABLE agent_turns_snapshot_source RENAME TO agent_turns`)
	return err
}
