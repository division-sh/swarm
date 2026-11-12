package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
)

func SeedHistoricalActivityForkAttemptForTest(ctx context.Context, selected any, record pipeline.ActivityAttemptRecord) error {
	for _, id := range []string{record.RunID, record.EntityID, record.RequestEventID, record.SourceEventID, record.ResultEventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return err
		}
	}
	if err := pipeline.ValidateActivityAttemptStart(record); err != nil {
		return err
	}
	if err := pipeline.ValidateActivityAttemptTerminal(record); err != nil {
		return err
	}
	if record.ExecutionMode != executionmode.Live || !record.Generation.Valid() || record.Generation != record.Generation.Normalize() || record.LoopStage == "" || record.ParentEventID != "" || record.ReplyContextID != "" {
		return fmt.Errorf("historical activity fork requires exact live loop evidence without invented parent or reply identity")
	}
	if record.StartedAt.IsZero() || record.CompletedAt == nil || !record.StartedAt.Equal(*record.CompletedAt) || !record.StartedAt.Equal(record.UpdatedAt) {
		return fmt.Errorf("historical activity fork requires its identical start, completion and update clock")
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return activityjournal.InsertHistoricalActivityForkAttemptTx(ctx, tx, postgres, record)
	})
}
