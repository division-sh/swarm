package eventrecord

import (
	"context"
	"database/sql"
	"fmt"
)

// The fixture deliberately reaches the physical CHECK constraint; validating
// depth here would conceal whether the selected schema rejects corrupt rows.
func SetFixtureEventChainDepthTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string, depth int) error {
	query := `UPDATE events SET chain_depth = ? WHERE event_id = ?`
	if postgres {
		query = `UPDATE events SET chain_depth = $1 WHERE event_id = $2::uuid`
	}
	result, err := tx.ExecContext(ctx, query, depth, eventID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("chain-depth fixture changed %d rows, want one exact event", changed)
	}
	return nil
}
