package channelonboarding

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *PostgresOwner) ReplaceRetainedChannelRuntimeForTest(ctx context.Context, activation, from, to string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE connected_channel_activations SET runtime_instance_id=$1 WHERE activation_id=$2 AND runtime_instance_id=$3`, to, activation, from)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return fmt.Errorf("mailbox fixture requires exactly one row, changed %d", changed)
		}
		return nil
	})
}

func (s *SQLiteOwner) ReplaceRetainedChannelRuntimeForTest(ctx context.Context, activation, from, to string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE connected_channel_activations SET runtime_instance_id=$1 WHERE activation_id=$2 AND runtime_instance_id=$3`, to, activation, from)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return fmt.Errorf("mailbox fixture requires exactly one row, changed %d", changed)
		}
		return nil
	})
}
