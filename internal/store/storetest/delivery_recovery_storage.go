package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type DeliveryRollbackStorage = private.DeliveryRollbackStorage
type ExactDeliveryDeadLetterStorage = private.ExactDeliveryDeadLetterStorage
type DeliveryConformanceAttemptStorage = private.DeliveryConformanceAttemptStorage

func ReadDeliveryRollbackStorage(ctx context.Context, selected any, id string) (DeliveryRollbackStorage, error) {
	return private.ReadDeliveryRollbackStorageForTest(ctx, selected, id)
}
func ReadExactDeliveryDeadLetterStorage(ctx context.Context, selected any, id string, version int64) (ExactDeliveryDeadLetterStorage, error) {
	return private.ReadExactDeliveryDeadLetterStorageForTest(ctx, selected, id, version)
}
func ReadDeliveryAttemptFailureStorage(ctx context.Context, selected any, id string, version int64) ([]byte, error) {
	return private.ReadDeliveryAttemptFailureStorageForTest(ctx, selected, id, version)
}
func ReadLatestDeliveryActivityFailureStorage(ctx context.Context, selected any, id string) ([]byte, error) {
	return private.ReadLatestDeliveryActivityFailureStorageForTest(ctx, selected, id)
}
func ReadDeliveryConformanceAttemptsStorage(ctx context.Context, selected any, id string) ([]DeliveryConformanceAttemptStorage, error) {
	return private.ReadDeliveryConformanceAttemptsStorageForTest(ctx, selected, id)
}
func CountMatchingOpenDeliveryAttemptStorage(ctx context.Context, selected any, id string, version int64) (int, error) {
	return private.CountMatchingOpenDeliveryAttemptStorageForTest(ctx, selected, id, version)
}
func ReadDeliveryConformanceSelectionStorage(ctx context.Context, selected any, id string) (PublicationRuleSelectionStorage, error) {
	return private.ReadDeliveryConformanceSelectionStorageForTest(ctx, selected, id)
}
func CountDeliveryHandlerSelectionsStorage(ctx context.Context, selected any, id string) (int, error) {
	return private.CountDeliveryHandlerSelectionsStorageForTest(ctx, selected, id)
}
func MakeDeliveryRecoveryRetryEligible(ctx context.Context, selected any, id string) error {
	return private.MakeDeliveryRecoveryRetryEligibleForTest(ctx, selected, id)
}
func CorruptDeliveryConformanceRoute(ctx context.Context, selected any, id string) error {
	return private.CorruptDeliveryConformanceRouteForTest(ctx, selected, id)
}
