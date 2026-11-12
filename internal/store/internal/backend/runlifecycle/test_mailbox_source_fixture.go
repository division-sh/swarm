package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *RunLifecyclePostgresOwner) SwapMailboxRunSourceForTest(ctx context.Context, runID, from, to string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return swapMailboxRunSourceTx(ctx, tx, runID, from, to)
	})
}

func (s *RunLifecycleSQLiteOwner) SwapMailboxRunSourceForTest(ctx context.Context, runID, from, to string) error {
	return s.backend.RunTransaction(ctx, "mailbox fixture", func(ctx context.Context, tx *sql.Tx) error {
		return swapMailboxRunSourceTx(ctx, tx, runID, from, to)
	})
}

func swapMailboxRunSourceTx(ctx context.Context, tx *sql.Tx, runID, from, to string) error {
	result, err := tx.ExecContext(ctx, `UPDATE runs SET bundle_hash=$1 WHERE run_id=$2 AND bundle_hash=$3`, to, runID, from)
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
	var got string
	if err := tx.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&got); err != nil {
		return err
	}
	if got != to {
		return fmt.Errorf("mailbox source fixture witness mismatch: %q != %q", got, to)
	}
	return nil
}
