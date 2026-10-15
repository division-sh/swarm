package operatorchannel

import "context"

// These physical witnesses intentionally do not interpret onboarding state.
func CountActionIntentStorageTx(ctx context.Context, tx queryer) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_action_intents`).Scan(&count)
	return count, err
}

func ReadTextIntentFactStorageTx(ctx context.Context, tx queryer, providerEventID string, postgres bool) ([]byte, error) {
	d := dialectSQLite
	if postgres {
		d = dialectPostgres
	}
	var fact []byte
	err := tx.QueryRowContext(ctx, d.bind(`SELECT fact FROM operator_channel_text_intents WHERE provider_event_id=?`), providerEventID).Scan(&fact)
	return fact, err
}
