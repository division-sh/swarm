package runlifecycle

import (
	"context"
	"database/sql"
)

func (s *RunLifecyclePostgresOwner) RemovePausedRunControlForTest(ctx context.Context, runID string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return RemovePausedRunControlFixtureTx(ctx, tx, runID)
	})
}

func (s *RunLifecycleSQLiteOwner) RemovePausedRunControlForTest(ctx context.Context, runID string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return RemovePausedRunControlFixtureTx(ctx, tx, runID)
	})
}

func (s *RunLifecyclePostgresOwner) ContradictPausedRunControlForTest(ctx context.Context, runID string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return ContradictPausedRunControlFixtureTx(ctx, tx, runID)
	})
}

func (s *RunLifecycleSQLiteOwner) ContradictPausedRunControlForTest(ctx context.Context, runID string) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return ContradictPausedRunControlFixtureTx(ctx, tx, runID)
	})
}
