package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/counterprojection"
)

type RunEventCounterStorageConsistency = counterprojection.StorageConsistency
type FanOutSemanticRejectionStorage = pipelinepersistence.FanOutSemanticRejectionStorage
type FanOutObligationHistoryStorage = runforkrevision.FanOutObligationHistoryStorage

type RunRevisionFactStorage = runforkrevision.RunRevisionFactStorage
type FanOutRunBlockedClaimStorage = pipelinepersistence.FanOutRunBlockedClaimStorage

func ReadLatestFanOutObligationFactStorageForTest(ctx context.Context, selected any, runID string) (FanOutObligationHistoryStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (FanOutObligationHistoryStorage, error) {
		return runforkrevision.ReadLatestFanOutObligationFactStorageTx(ctx, tx, runID)
	})
}

func ReadRunRevisionFactStorageForTest(ctx context.Context, selected any, runID string, revision int64) (RunRevisionFactStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (RunRevisionFactStorage, error) {
		return runforkrevision.ReadRunRevisionFactStorageTx(ctx, tx, runID, revision)
	})
}

func ReadFanOutRunBlockedClaimStorageForTest(ctx context.Context, selected any, runID string) (FanOutRunBlockedClaimStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (FanOutRunBlockedClaimStorage, error) {
		return pipelinepersistence.ReadFanOutRunBlockedClaimStorageTx(ctx, tx, runID)
	})
}

func ReadRunEventCounterStorageConsistencyForTest(ctx context.Context, selected any, runID string) (RunEventCounterStorageConsistency, error) {
	var empty RunEventCounterStorageConsistency
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return empty, err
	}
	var out RunEventCounterStorageConsistency
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = counterprojection.ReadStorageConsistencyForTestTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return empty, err
	}
	return out, nil
}

func ReadFanOutSemanticRejectionStorageForTest(ctx context.Context, selected any, runID string) ([]FanOutSemanticRejectionStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return nil, err
	}
	var out []FanOutSemanticRejectionStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.ReadFanOutSemanticRejectionStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadFanOutObligationHistoryStorageForTest(ctx context.Context, selected any, runID string) ([]FanOutObligationHistoryStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return nil, err
	}
	var out []FanOutObligationHistoryStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkrevision.ReadFanOutObligationHistoryStorageForTestTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
