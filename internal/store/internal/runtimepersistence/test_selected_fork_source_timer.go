package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func SeedSelectedForkSourceTimerForTest(ctx context.Context, selected any, runID, entityID string, ref timeridentity.WorkflowTimerActivationRef, at time.Time) error {
	for _, id := range []string{runID, entityID, ref.ActivationID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return err
		}
	}
	if !ref.Valid() || at.IsZero() {
		return fmt.Errorf("selected source timer fixture requires its exact history identity and clock")
	}
	_, postgres := selected.(*PostgresStore)
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return genericschedule.SeedSelectedForkSourceTimerTx(ctx, tx, postgres, runID, entityID, ref, at)
		})
	})
}

func ReadSelectedForkSourceTimerStatusForTest(ctx context.Context, selected any, timerID, runID string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{timerID, runID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return genericschedule.ReadSelectedForkSourceTimerStatusTx(ctx, tx, timerID, runID)
	})
}
