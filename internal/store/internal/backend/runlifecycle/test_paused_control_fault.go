package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *RunLifecyclePostgresOwner) RemovePausedRunControlForTest(ctx context.Context, runID string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM run_control_state WHERE run_id=$1`, runID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("missing-control fault requires one exact run control: rows=%d", count)
		}
		return nil
	})
}

func (s *RunLifecycleSQLiteOwner) RemovePausedRunControlForTest(ctx context.Context, runID string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM run_control_state WHERE run_id=$1`, runID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("missing-control fault requires one exact run control: rows=%d", count)
		}
		return nil
	})
}

func (s *RunLifecyclePostgresOwner) ContradictPausedRunControlForTest(ctx context.Context, runID string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE run_control_state SET control_status='running' WHERE run_id=$1`, runID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("contradictory-control fault requires one exact run control: rows=%d", count)
		}
		return nil
	})
}

func (s *RunLifecycleSQLiteOwner) ContradictPausedRunControlForTest(ctx context.Context, runID string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE run_control_state SET control_status='running' WHERE run_id=$1`, runID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("contradictory-control fault requires one exact run control: rows=%d", count)
		}
		return nil
	})
}
