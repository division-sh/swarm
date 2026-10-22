package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func SeedHistoricalActivityForkLoopForTest(ctx context.Context, selected any, runID, entityID, requestID string, activation loopruntime.Activation, at time.Time) error {
	for _, id := range []string{runID, entityID, requestID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return err
		}
	}
	if at.IsZero() {
		return fmt.Errorf("historical activity loop requires its original clock")
	}
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		return err
	}
	accumulator, err := json.Marshal(buckets)
	if err != nil {
		return err
	}
	loops, err := json.Marshal(buckets[loopruntime.BucketKey])
	if err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	// Preserve the original two commits, rather than invent an atomic pair or a
	// live loop transition from these historical fixture facts.
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.SetHistoricalActivityForkAccumulatorTx(ctx, tx, postgres, runID, entityID, string(accumulator))
	}); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.InsertHistoricalActivityForkLoopMutationTx(ctx, tx, postgres, runID, entityID, requestID, string(loops), at)
	})
}

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
