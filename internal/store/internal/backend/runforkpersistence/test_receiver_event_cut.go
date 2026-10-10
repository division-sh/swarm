package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

// Physical inspection consumes the same fixed-cut planner without granting
// runtime schema admission. The backend retains the inspection snapshot lifetime.
func (s *RunForkPostgresOwner) ReadEntityAtEventCutForTest(ctx context.Context, runID, entityID, eventID string) (runfork.RunForkEntityState, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkEntityState{}, fmt.Errorf("receiver event-cut observation requires an initialized postgres read owner")
	}
	var entity runfork.RunForkEntityState
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		plan, err := planRunForkSnapshot(ctx, tx, runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID}, runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint)
		if err != nil {
			return err
		}
		entity, err = receiverEntityAtExactEventCut(plan, entityID, eventID)
		return err
	})
	if err != nil {
		return runfork.RunForkEntityState{}, err
	}
	return entity, nil
}

func (s *RunForkSQLiteOwner) ReadEntityAtEventCutForTest(ctx context.Context, runID, entityID, eventID string) (runfork.RunForkEntityState, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkEntityState{}, fmt.Errorf("receiver event-cut observation requires an initialized sqlite read owner")
	}
	var entity runfork.RunForkEntityState
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		plan, err := planRunForkSnapshot(ctx, tx, runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID}, runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint)
		if err != nil {
			return err
		}
		entity, err = receiverEntityAtExactEventCut(plan, entityID, eventID)
		return err
	})
	if err != nil {
		return runfork.RunForkEntityState{}, err
	}
	return entity, nil
}

func receiverEntityAtExactEventCut(plan runfork.RunForkPlan, entityID, eventID string) (runfork.RunForkEntityState, error) {
	if eventID == "" || plan.ForkPoint.EventID != eventID {
		return runfork.RunForkEntityState{}, fmt.Errorf("receiver witness requires the exact selected event cut")
	}
	for _, entity := range plan.Entities {
		if entity.EntityID == entityID {
			return entity, nil
		}
	}
	return runfork.RunForkEntityState{}, fmt.Errorf("event cut %s lacks receiver %s", eventID, entityID)
}
