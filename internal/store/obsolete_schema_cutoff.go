package store

import (
	"context"
	"database/sql"
	"fmt"
)

const obsoleteDecisionCardLifecycleOutbox = "decision_card_lifecycle_outbox"

func enforcePostgresDecisionCardLifecycleOutboxCutoff(ctx context.Context, tx *sql.Tx) error {
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('public.decision_card_lifecycle_outbox') IS NOT NULL`).Scan(&exists); err != nil {
		return fmt.Errorf("inspect obsolete PostgreSQL table %s: %w", obsoleteDecisionCardLifecycleOutbox, err)
	}
	if !exists {
		return nil
	}
	var populated bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM decision_card_lifecycle_outbox LIMIT 1)`).Scan(&populated); err != nil {
		return fmt.Errorf("inspect obsolete PostgreSQL table %s rows: %w", obsoleteDecisionCardLifecycleOutbox, err)
	}
	if populated {
		return fmt.Errorf("unsupported pre-1.0 PostgreSQL database: obsolete table %s contains legacy rows; provision or select a fresh empty database; existing database state is not migrated", obsoleteDecisionCardLifecycleOutbox)
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE decision_card_lifecycle_outbox`); err != nil {
		return fmt.Errorf("drop empty obsolete PostgreSQL table %s: %w", obsoleteDecisionCardLifecycleOutbox, err)
	}
	return nil
}

func enforceSQLiteDecisionCardLifecycleOutboxCutoff(ctx context.Context, conn *sql.Conn) error {
	var exists int
	if err := conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'decision_card_lifecycle_outbox')`).Scan(&exists); err != nil {
		return fmt.Errorf("inspect obsolete SQLite table %s: %w", obsoleteDecisionCardLifecycleOutbox, err)
	}
	if exists == 0 {
		return nil
	}
	var populated int
	if err := conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM decision_card_lifecycle_outbox LIMIT 1)`).Scan(&populated); err != nil {
		return fmt.Errorf("inspect obsolete SQLite table %s rows: %w", obsoleteDecisionCardLifecycleOutbox, err)
	}
	if populated != 0 {
		return fmt.Errorf("unsupported pre-1.0 SQLite database: obsolete table %s contains legacy rows; recreate the configured SQLite database file or select a fresh empty database; existing database state is not migrated", obsoleteDecisionCardLifecycleOutbox)
	}
	if _, err := conn.ExecContext(ctx, `DROP TABLE decision_card_lifecycle_outbox`); err != nil {
		return fmt.Errorf("drop empty obsolete SQLite table %s: %w", obsoleteDecisionCardLifecycleOutbox, err)
	}
	return nil
}
