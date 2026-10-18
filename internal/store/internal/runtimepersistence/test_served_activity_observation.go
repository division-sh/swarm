package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/channelonboarding"
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

// Only the opaque connection coordinate is needed by the served successor
// oracle; private account/state evidence and mutation capabilities stay closed.
func ReadServedChannelConnectionForTest(ctx context.Context, selected any, operationID string) (string, error) {
	if uuid.Validate(operationID) != nil {
		return "", fmt.Errorf("served connection observation requires an exact operation identity")
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	_, postgres := selected.(*PostgresStore)
	var connection string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		connection, err = channelonboarding.ReadSessionConnectionForTest(ctx, tx, postgres, operationID)
		return err
	})
	return connection, err
}
