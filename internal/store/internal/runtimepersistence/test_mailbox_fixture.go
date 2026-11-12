package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CorruptHumanTaskRequesterForTest captures, installs and witnesses one exact
// nullable coordinate in the selected writer. Restoration uses the same owner.
func CorruptHumanTaskRequesterForTest(ctx context.Context, selected any, runID, cardID, coordinate, hostile string) (func(context.Context) error, error) {
	switch coordinate {
	case "requester_flow_id", "requester_flow_instance", "requester_entity_id":
	default:
		return nil, fmt.Errorf("unsupported requester fixture coordinate %q", coordinate)
	}
	if runID == "" || cardID == "" {
		return nil, fmt.Errorf("requester fixture requires exact run and card")
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return nil, fmt.Errorf("postgres fixture store is required")
		}
		return store.decisionPostgresOwner.CorruptHumanTaskRequesterForTest(ctx, runID, cardID, coordinate, hostile)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return nil, fmt.Errorf("sqlite fixture store is required")
		}
		return store.decisionSQLiteOwner.CorruptHumanTaskRequesterForTest(ctx, runID, cardID, coordinate, hostile)
	default:
		return nil, fmt.Errorf("unsupported mailbox fixture store %T", selected)
	}
}

func SwapMailboxRunSourceForTest(ctx context.Context, selected any, runID, from, to string) error {
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres fixture store is required")
		}
		return store.runLifecyclePostgresOwner.SwapMailboxRunSourceForTest(ctx, runID, from, to)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite fixture store is required")
		}
		return store.runLifecycleSQLiteOwner.SwapMailboxRunSourceForTest(ctx, runID, from, to)
	default:
		return fmt.Errorf("unsupported mailbox fixture store %T", selected)
	}
}

func SetMailboxNoticeTimeForTest(ctx context.Context, selected any, itemID string, at time.Time) error {
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres fixture store is required")
		}
		return store.mailboxPostgresOwner.SetMailboxNoticeTimeForTest(ctx, itemID, at)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite fixture store is required")
		}
		return store.mailboxSQLiteOwner.SetMailboxNoticeTimeForTest(ctx, itemID, at)
	default:
		return fmt.Errorf("unsupported mailbox fixture store %T", selected)
	}
}

func requireMailboxFixtureRow(result sql.Result) error {
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("mailbox fixture requires exactly one row, changed %d", n)
	}
	return nil
}

func runMailboxFixtureTransaction(ctx context.Context, selected any, apply func(context.Context, *sql.Tx) error) error {
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres fixture store is required")
		}
		return store.backend.RunTransaction(ctx, apply)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite fixture store is required")
		}
		return store.backend.RunTransaction(ctx, "mailbox fixture", apply)
	default:
		return fmt.Errorf("unsupported mailbox fixture store %T", selected)
	}
}
