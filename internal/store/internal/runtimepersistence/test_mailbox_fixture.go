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
	query := `SELECT CAST(` + coordinate + ` AS TEXT) FROM human_task_continuations WHERE card_id=$1 AND run_id=$2`
	if _, postgres := selected.(*PostgresStore); postgres {
		query += ` FOR UPDATE`
	}
	update := `UPDATE human_task_continuations SET ` + coordinate + `=$1 WHERE card_id=$2 AND run_id=$3`
	read := func(ctx context.Context, tx *sql.Tx) (sql.NullString, error) {
		var value sql.NullString
		err := tx.QueryRowContext(ctx, query, cardID, runID).Scan(&value)
		return value, err
	}
	write := func(ctx context.Context, tx *sql.Tx, value sql.NullString) error {
		result, err := tx.ExecContext(ctx, update, value, cardID, runID)
		if err != nil {
			return err
		}
		if err := requireMailboxFixtureRow(result); err != nil {
			return err
		}
		got, err := read(ctx, tx)
		if err != nil {
			return err
		}
		if got != value {
			return fmt.Errorf("requester fixture witness mismatch: got %+v, want %+v", got, value)
		}
		return nil
	}
	var original sql.NullString
	installed := sql.NullString{String: hostile, Valid: true}
	err := runMailboxFixtureTransaction(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		original, err = read(ctx, tx)
		if err != nil {
			return err
		}
		return write(ctx, tx, installed)
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		return runMailboxFixtureTransaction(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			current, err := read(ctx, tx)
			if err != nil {
				return err
			}
			if current != installed {
				return fmt.Errorf("requester fixture changed before restoration: got %+v, want %+v", current, installed)
			}
			return write(ctx, tx, original)
		})
	}, nil
}

func SwapMailboxRunSourceForTest(ctx context.Context, selected any, runID, from, to string) error {
	return runMailboxFixtureTransaction(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE runs SET bundle_hash=$1 WHERE run_id=$2 AND bundle_hash=$3`, to, runID, from)
		if err != nil {
			return err
		}
		if err := requireMailboxFixtureRow(result); err != nil {
			return err
		}
		var got string
		if err := tx.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&got); err != nil {
			return err
		}
		if got != to {
			return fmt.Errorf("mailbox source fixture witness mismatch: %q != %q", got, to)
		}
		return nil
	})
}

func SetMailboxNoticeTimeForTest(ctx context.Context, selected any, itemID string, at time.Time) error {
	return runMailboxFixtureTransaction(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE mailbox SET created_at=$1 WHERE item_id=$2`, at, itemID)
		if err != nil {
			return err
		}
		return requireMailboxFixtureRow(result)
	})
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
