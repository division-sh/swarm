package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SupplementalOutcomeCardinalityStorage = private.SupplementalOutcomeCardinalityStorage
type SupplementalIntentCardinalityStorage = private.SupplementalIntentCardinalityStorage
type SupplementalClaimCursorStorage = private.SupplementalClaimCursorStorage
type ServingCapacityStorage = private.ServingCapacityStorage
type SupplementalProgressStorage = private.SupplementalProgressStorage
type SupplementalPressureStorage = private.SupplementalPressureStorage
type SupplementalExpiredClaimStorage = private.SupplementalExpiredClaimStorage

type ServingLifetimeMetadataStorage = private.ServingLifetimeMetadataStorage

func ReadServingLifetimeMetadataStorage(ctx context.Context, selected any, runID string) (ServingLifetimeMetadataStorage, error) {
	return private.ReadServingLifetimeMetadataStorageForTest(ctx, selected, runID)
}

func ReadServingLifetimeClaimOwnerStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadServingLifetimeClaimOwnerStorageForTest(ctx, selected, runID)
}

func CountServingLifetimeHistoryStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountServingLifetimeHistoryStorageForTest(ctx, selected, runID)
}

func ReadSupplementalCommittedOutcomeCardinality(ctx context.Context, selected any, run string) (SupplementalOutcomeCardinalityStorage, error) {
	return private.ReadSupplementalCommittedOutcomeCardinalityForTest(ctx, selected, run)
}
func CountSupplementalGlobalOutcomes(ctx context.Context, selected any) (int, error) {
	return private.CountSupplementalGlobalOutcomesForTest(ctx, selected)
}
func CountServingGlobalIntents(ctx context.Context, selected any) (int, error) {
	return private.CountServingGlobalIntentsForTest(ctx, selected)
}
func ReadServingGlobalCapacity(ctx context.Context, selected any) (ServingCapacityStorage, error) {
	return private.ReadServingGlobalCapacityForTest(ctx, selected)
}
func ReadSupplementalGlobalIntentCardinality(ctx context.Context, selected any) (SupplementalIntentCardinalityStorage, error) {
	return private.ReadSupplementalGlobalIntentCardinalityForTest(ctx, selected)
}
func ReadSupplementalGlobalClaimCursor(ctx context.Context, selected any) (SupplementalClaimCursorStorage, error) {
	return private.ReadSupplementalGlobalClaimCursorForTest(ctx, selected)
}
func ReadSupplementalGlobalProgress(ctx context.Context, selected any) (SupplementalProgressStorage, error) {
	return private.ReadSupplementalGlobalProgressForTest(ctx, selected)
}
func ReadSupplementalGlobalPressure(ctx context.Context, selected any) (SupplementalPressureStorage, error) {
	return private.ReadSupplementalGlobalPressureForTest(ctx, selected)
}
func ReadSupplementalExpiredClaim(ctx context.Context, selected any, key fanoutobligation.IntentKey) (SupplementalExpiredClaimStorage, error) {
	return private.ReadSupplementalExpiredClaimForTest(ctx, selected, key)
}
func ReadSupplementalFanOutObligationHistory(ctx context.Context, selected any, run string) ([]FanOutObligationHistoryStorage, error) {
	return private.ReadSupplementalFanOutObligationHistoryForTest(ctx, selected, run)
}
