package mailboxpersistence

import (
	"context"
	"database/sql"
)

type RecoveryNoticeStorage struct{ ItemID, EntityID, Severity, Producer, Payload, Summary string }

func ReadRecoveryNoticeStorageTx(ctx context.Context, tx *sql.Tx) (RecoveryNoticeStorage, error) {
	var result RecoveryNoticeStorage
	err := tx.QueryRowContext(ctx, `SELECT item_id,COALESCE(CAST(entity_id AS TEXT),''),severity,from_agent,CAST(payload AS TEXT),summary
		FROM mailbox WHERE item_type='alert' AND summary LIKE 'Runtime recovery failed:%'`).Scan(&result.ItemID, &result.EntityID, &result.Severity, &result.Producer, &result.Payload, &result.Summary)
	return result, err
}

func CountRecoveryNoticeStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE item_type='alert' AND summary LIKE 'Runtime recovery failed:%'`).Scan(&count)
	return count, err
}

func ReadUncertainNoticeAcknowledgmentStorageTx(ctx context.Context, tx *sql.Tx, id string) (bool, error) {
	var notified bool
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(notified,false) FROM mailbox WHERE item_id=$1`, id).Scan(&notified)
	return notified, err
}
