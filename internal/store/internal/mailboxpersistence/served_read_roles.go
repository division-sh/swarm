package mailboxpersistence

import (
	"context"
	"database/sql"
)

func CountSourceNoticeStorageTx(ctx context.Context, tx *sql.Tx, sourceEventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE source_event_id=$1`, sourceEventID).Scan(&count)
	return count, err
}
