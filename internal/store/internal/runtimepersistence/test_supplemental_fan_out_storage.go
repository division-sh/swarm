package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type SupplementalOutcomeCardinalityStorage = pipelinepersistence.SupplementalOutcomeCardinalityStorage
type SupplementalIntentCardinalityStorage = pipelinepersistence.SupplementalIntentCardinalityStorage
type SupplementalClaimCursorStorage = pipelinepersistence.SupplementalClaimCursorStorage
type ServingCapacityStorage = pipelinepersistence.ServingCapacityStorage
type SupplementalProgressStorage = pipelinepersistence.SupplementalProgressStorage
type SupplementalPressureStorage = pipelinepersistence.SupplementalPressureStorage
type SupplementalExpiredClaimStorage = pipelinepersistence.SupplementalExpiredClaimStorage

type ServingLifetimeMetadataStorage = pipelinepersistence.ServingLifetimeMetadataStorage

func ReadServingLifetimeMetadataStorageForTest(ctx context.Context, selected any, runID string) (ServingLifetimeMetadataStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (ServingLifetimeMetadataStorage, error) {
		return pipelinepersistence.ReadServingLifetimeMetadataStorageTx(ctx, tx, runID)
	})
}

func ReadServingLifetimeClaimOwnerStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadServingLifetimeClaimOwnerStorageTx(ctx, tx, runID)
	})
}

func CountServingLifetimeHistoryStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return runforkrevision.CountServingLifetimeHistoryStorageTx(ctx, tx, runID)
	})
}

func ReadSupplementalCommittedOutcomeCardinalityForTest(ctx context.Context, selected any, run string) (SupplementalOutcomeCardinalityStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) (SupplementalOutcomeCardinalityStorage, error) {
		return pipelinepersistence.ReadSupplementalCommittedOutcomeCardinalityTx(ctx, tx, run)
	})
}

func CountSupplementalGlobalOutcomesForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, pipelinepersistence.CountSupplementalGlobalOutcomesTx)
}

func CountServingGlobalIntentsForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, pipelinepersistence.CountServingGlobalIntentsTx)
}

func ReadServingGlobalCapacityForTest(ctx context.Context, selected any) (ServingCapacityStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, pipelinepersistence.ReadServingGlobalCapacityTx)
}

func ReadSupplementalGlobalIntentCardinalityForTest(ctx context.Context, selected any) (SupplementalIntentCardinalityStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, pipelinepersistence.ReadSupplementalGlobalIntentCardinalityTx)
}

func ReadSupplementalGlobalClaimCursorForTest(ctx context.Context, selected any) (SupplementalClaimCursorStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, pipelinepersistence.ReadSupplementalGlobalClaimCursorTx)
}

func ReadSupplementalGlobalProgressForTest(ctx context.Context, selected any) (SupplementalProgressStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, pipelinepersistence.ReadSupplementalGlobalProgressTx)
}

func ReadSupplementalGlobalPressureForTest(ctx context.Context, selected any) (SupplementalPressureStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, pipelinepersistence.ReadSupplementalGlobalPressureTx)
}

func ReadSupplementalExpiredClaimForTest(ctx context.Context, selected any, key fanoutobligation.IntentKey) (SupplementalExpiredClaimStorage, error) {
	if err := key.Validate(); err != nil {
		return SupplementalExpiredClaimStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, []string{key.RunID, key.TriggeringDeliveryID}, func(ctx context.Context, tx *sql.Tx) (SupplementalExpiredClaimStorage, error) {
		return pipelinepersistence.ReadSupplementalExpiredClaimTx(ctx, tx, key)
	})
}

func ReadSupplementalFanOutObligationHistoryForTest(ctx context.Context, selected any, run string) ([]FanOutObligationHistoryStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{run}, func(ctx context.Context, tx *sql.Tx) ([]FanOutObligationHistoryStorage, error) {
		return runforkrevision.ReadSupplementalFanOutObligationHistoryTx(ctx, tx, run)
	})
}
