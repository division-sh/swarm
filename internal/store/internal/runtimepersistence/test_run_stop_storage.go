package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type RunStopStorage struct {
	Status, Control string
	// Pending is the historical field name for all nonterminal delivery rows.
	Pending   int
	Revisions int64
}

type StopFanOutIntentStorage = pipelinepersistence.StopFanOutIntentStorage
type StopFanOutOutcomeStorage = pipelinepersistence.StopFanOutOutcomeStorage
type StopRunControlClockStorage = runlifecycle.StopRunControlClockStorage

func ReadStopFanOutIntentsStorageForTest(ctx context.Context, selected any, runID string) ([]StopFanOutIntentStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) ([]StopFanOutIntentStorage, error) {
		return pipelinepersistence.ReadStopFanOutIntentsStorageTx(ctx, tx, runID)
	})
}

func ReadStopFanOutOutcomesStorageForTest(ctx context.Context, selected any, runID string) ([]StopFanOutOutcomeStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) ([]StopFanOutOutcomeStorage, error) {
		return pipelinepersistence.ReadStopFanOutOutcomesStorageTx(ctx, tx, runID)
	})
}

func ReadStopRunControlClockStorageForTest(ctx context.Context, selected any, runID string) (StopRunControlClockStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (StopRunControlClockStorage, error) {
		return runlifecycle.ReadStopRunControlClockStorageTx(ctx, tx, runID)
	})
}

func CountRunStopRevisionRowsForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		count, err := runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)
		if err != nil {
			return 0, err
		}
		if int64(int(count)) != count {
			return 0, fmt.Errorf("stop revision count exceeds native int")
		}
		return int(count), nil
	})
}

func CountRunStopFactRevisionRowsForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return runforkrevision.CountRunFactRevisionRowsForTest(ctx, tx, runID)
	})
}

func CountRunStopNonterminalDeliveriesForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = delivery.ReadStopPendingDeliveryCountTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
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
