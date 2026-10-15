package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type ExactPipelineReceiptOutcomeReason = pipelinepersistence.ExactPipelineReceiptOutcomeReason

type HistoricalReplayScopeStorage = pipelinepersistence.HistoricalReplayScopeStorage

func ReadHistoricalReplayScopeStorageForTest(ctx context.Context, selected any, eventID string) (HistoricalReplayScopeStorage, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return HistoricalReplayScopeStorage{}, err
	}
	var out HistoricalReplayScopeStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadHistoricalReplayScopeStorageTx(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return HistoricalReplayScopeStorage{}, err
	}
	return out, nil
}

func CountCopiedSourceReplayScopeStorageForTest(ctx context.Context, selected any, runID, sourceEventID string) (int, error) {
	for _, id := range []string{runID, sourceEventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return 0, err
		}
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = pipelinepersistence.CountCopiedSourceReplayScopeStorageTx(ctx, tx, runID, sourceEventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, err
}

func CountForkLocalReplayScopeStorageForTest(ctx context.Context, selected any, runID, sourceEventID string) (int, error) {
	for _, id := range []string{runID, sourceEventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return 0, err
		}
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = pipelinepersistence.CountForkLocalReplayScopeStorageTx(ctx, tx, runID, sourceEventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, err
}

func DeleteCommittedReplayScopeForTest(ctx context.Context, selected any, eventID string) error {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return pipelinepersistence.DeleteCommittedReplayScopeFixtureTx(ctx, tx, postgres, eventID)
	})
}

func ReadExactPipelineReceiptOutcomeReasonForTest(ctx context.Context, selected any, eventID string) (ExactPipelineReceiptOutcomeReason, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return ExactPipelineReceiptOutcomeReason{}, err
	}
	_, postgres := selected.(*PostgresStore)
	var out ExactPipelineReceiptOutcomeReason
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadExactPipelineReceiptOutcomeReasonTx(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil {
		return ExactPipelineReceiptOutcomeReason{}, err
	}
	return out, nil
}
