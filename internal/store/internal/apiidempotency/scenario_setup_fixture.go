package apiidempotency

import (
	"context"
	"database/sql"
)

// Preserve the original setup replay cut: all transport receipts are removed,
// not a simulated expiry or a method/key-selected deletion.
func RemoveScenarioSetupReplayReceiptsTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM api_idempotency`)
	return err
}
