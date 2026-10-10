package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

func ReadReceiverEntityAtEventCutForTest(ctx context.Context, selected any, runID, entityID, eventID string) (runfork.RunForkEntityState, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return runfork.RunForkEntityState{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.runForkPostgresOwner.ReadEntityAtEventCutForTest(ctx, runID, entityID, eventID)
	case *SQLiteRuntimeStore:
		return owner.runForkSQLiteOwner.ReadEntityAtEventCutForTest(ctx, runID, entityID, eventID)
	}
	return runfork.RunForkEntityState{}, fmt.Errorf("receiver witness requires its original selected read owner")
}

func ReadReceiverHistoricalEntityStateForTest(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID string, revision int64) (runfork.RunForkEntityState, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return runfork.RunForkEntityState{}, err
	}
	var state runfork.RunForkEntityState
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		state, err = runforkpersistence.ReadConstructedEntityAtRevisionTx(ctx, tx, owner, entityID, revision)
		return err
	})
	if err != nil {
		return runfork.RunForkEntityState{}, err
	}
	return state, nil
}

func ReadEventIdempotencyCardinalityForTest(ctx context.Context, selected any, key string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = pipelinepersistence.ReadEventIdempotencyCardinalityTx(ctx, tx, key)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
