package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type RunStopStorage struct {
	Status, Control string
	Pending         int
	Revisions       int64
}

func RemovePausedRunControlForTest(ctx context.Context, selected any, runID string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return runlifecycle.RemovePausedRunControlFixtureTx(ctx, tx, runID)
	})
}

func ContradictPausedRunControlForTest(ctx context.Context, selected any, runID string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return runlifecycle.ContradictPausedRunControlFixtureTx(ctx, tx, runID)
	})
}

func ReadRunStopStorageForTest(ctx context.Context, selected any, runID string) (RunStopStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return RunStopStorage{}, err
	}
	var out RunStopStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		state, err := runlifecycle.ReadStopRunControlStorageTx(ctx, tx, runID)
		if err != nil {
			return err
		}
		out.Status, out.Control = state.Status, state.Control
		out.Pending, err = delivery.ReadStopPendingDeliveryCountTx(ctx, tx, runID)
		if err != nil {
			return err
		}
		out.Revisions, err = runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return RunStopStorage{}, err
	}
	return out, nil
}
