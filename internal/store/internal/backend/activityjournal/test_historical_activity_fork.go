package activityjournal

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func (s *ActivityPostgresOwner) SeedHistoricalActivityForkAttemptForTest(ctx context.Context, record pipeline.ActivityAttemptRecord) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return InsertHistoricalActivityForkAttemptTx(ctx, tx, true, record)
	})
}

func (s *ActivitySQLiteOwner) SeedHistoricalActivityForkAttemptForTest(ctx context.Context, record pipeline.ActivityAttemptRecord) error {
	return s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) error {
		return InsertHistoricalActivityForkAttemptTx(ctx, tx, false, record)
	})
}
