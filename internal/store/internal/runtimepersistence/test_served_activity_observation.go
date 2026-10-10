package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

// This fixed observation consumes the canonical journal reader under the
// original independently owned inspection snapshot. It grants no execution.
func ReadServedActivityAttemptForTest(ctx context.Context, selected any, requestID string) (pipeline.ActivityAttemptRecord, bool, error) {
	id, err := uuid.Parse(requestID)
	if err != nil || id == uuid.Nil || id.String() != requestID {
		return pipeline.ActivityAttemptRecord{}, false, fmt.Errorf("served activity observation requires an exact request identity")
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return pipeline.ActivityAttemptRecord{}, false, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.LoadActivityAttempt(ctx, requestID)
	case *SQLiteRuntimeStore:
		return owner.LoadActivityAttempt(ctx, requestID)
	default:
		return pipeline.ActivityAttemptRecord{}, false, fmt.Errorf("served activity observation requires the original native read owner")
	}
}
