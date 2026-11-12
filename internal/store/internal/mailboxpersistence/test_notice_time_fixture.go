package mailboxpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *MailboxPostgresOwner) SetMailboxNoticeTimeForTest(ctx context.Context, itemID string, at time.Time) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return setMailboxNoticeTimeTx(ctx, tx, itemID, at)
	})
}

func (s *MailboxSQLiteOwner) SetMailboxNoticeTimeForTest(ctx context.Context, itemID string, at time.Time) error {
	return s.backend.RunTransaction(ctx, "mailbox fixture", func(ctx context.Context, tx *sql.Tx) error {
		return setMailboxNoticeTimeTx(ctx, tx, itemID, at)
	})
}

func setMailboxNoticeTimeTx(ctx context.Context, tx *sql.Tx, itemID string, at time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE mailbox SET created_at=$1 WHERE item_id=$2`, at, itemID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("mailbox fixture requires exactly one row, changed %d", n)
	}
	return nil
}
