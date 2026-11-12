package decisionpersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *DecisionPostgresOwner) CorruptHumanTaskRequesterForTest(ctx context.Context, runID, cardID, coordinate, hostile string) (func(context.Context) error, error) {
	return corruptHumanTaskRequesterForTest(ctx, s.backend.RunTransaction, true, runID, cardID, coordinate, hostile)
}

func (s *DecisionSQLiteOwner) CorruptHumanTaskRequesterForTest(ctx context.Context, runID, cardID, coordinate, hostile string) (func(context.Context) error, error) {
	run := func(ctx context.Context, apply func(context.Context, *sql.Tx) error) error {
		return s.backend.RunTransaction(ctx, "mailbox fixture", apply)
	}
	return corruptHumanTaskRequesterForTest(ctx, run, false, runID, cardID, coordinate, hostile)
}

func corruptHumanTaskRequesterForTest(ctx context.Context, run func(context.Context, func(context.Context, *sql.Tx) error) error, postgres bool, runID, cardID, coordinate, hostile string) (func(context.Context) error, error) {
	switch coordinate {
	case "requester_flow_id", "requester_flow_instance", "requester_entity_id":
	default:
		return nil, fmt.Errorf("unsupported requester fixture coordinate %q", coordinate)
	}
	if runID == "" || cardID == "" {
		return nil, fmt.Errorf("requester fixture requires exact run and card")
	}
	query := `SELECT CAST(` + coordinate + ` AS TEXT) FROM human_task_continuations WHERE card_id=$1 AND run_id=$2`
	if postgres {
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
		if err := requireMailboxRequesterFixtureRow(result); err != nil {
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
	err := run(ctx, func(ctx context.Context, tx *sql.Tx) error {
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
		return run(ctx, func(ctx context.Context, tx *sql.Tx) error {
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

func requireMailboxRequesterFixtureRow(result sql.Result) error {
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("mailbox fixture requires exactly one row, changed %d", n)
	}
	return nil
}
