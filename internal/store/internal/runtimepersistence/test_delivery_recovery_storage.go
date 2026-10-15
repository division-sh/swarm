package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	authoractivityadapter "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity/readadapter"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type DeliveryRollbackStorage = delivery.DeliveryRollbackStorage
type ExactDeliveryDeadLetterStorage = delivery.ExactDeliveryDeadLetterStorage
type DeliveryConformanceAttemptStorage = delivery.DeliveryConformanceAttemptStorage

func ReadDeliveryRollbackStorageForTest(ctx context.Context, selected any, id string) (DeliveryRollbackStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, id, delivery.ReadDeliveryRollbackStorageTx)
}
func ReadExactDeliveryDeadLetterStorageForTest(ctx context.Context, selected any, id string, version int64) (ExactDeliveryDeadLetterStorage, error) {
	if version <= 0 {
		return ExactDeliveryDeadLetterStorage{}, fmt.Errorf("exact delivery diagnostic requires a positive claim version")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{id}, func(ctx context.Context, tx *sql.Tx) (ExactDeliveryDeadLetterStorage, error) {
		return delivery.ReadExactDeliveryDeadLetterStorageTx(ctx, tx, id, version)
	})
}
func ReadDeliveryAttemptFailureStorageForTest(ctx context.Context, selected any, id string, version int64) ([]byte, error) {
	if version <= 0 {
		return nil, fmt.Errorf("exact delivery failure requires a positive claim version")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{id}, func(ctx context.Context, tx *sql.Tx) ([]byte, error) {
		return delivery.ReadDeliveryAttemptFailureStorageTx(ctx, tx, id, version)
	})
}
func ReadLatestDeliveryActivityFailureStorageForTest(ctx context.Context, selected any, id string) ([]byte, error) {
	return readDeploymentStorageForTest(ctx, selected, id, authoractivityadapter.ReadLatestDeliveryFailureStorageTx)
}
func ReadDeliveryConformanceAttemptsStorageForTest(ctx context.Context, selected any, id string) ([]DeliveryConformanceAttemptStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, id, delivery.ReadDeliveryConformanceAttemptsStorageTx)
}
func CountMatchingOpenDeliveryAttemptStorageForTest(ctx context.Context, selected any, id string, version int64) (int, error) {
	if version <= 0 {
		return 0, fmt.Errorf("open delivery attempt requires a positive claim version")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{id}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountMatchingOpenDeliveryAttemptStorageTx(ctx, tx, id, version)
	})
}
func ReadDeliveryConformanceSelectionStorageForTest(ctx context.Context, selected any, id string) (PublicationRuleSelectionStorage, error) {
	return readDeploymentStorageForTest(ctx, selected, id, delivery.ReadDeliveryConformanceSelectionStorageTx)
}
func CountDeliveryHandlerSelectionsStorageForTest(ctx context.Context, selected any, id string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, id, delivery.CountDeliveryHandlerSelectionsStorageTx)
}
func MakeDeliveryRecoveryRetryEligibleForTest(ctx context.Context, selected any, id string) error {
	if err := validateStandaloneStorageEvent(selected, id); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return delivery.MakeDeliveryRecoveryRetryEligibleTx(ctx, tx, id)
	})
}
func CorruptDeliveryConformanceRouteForTest(ctx context.Context, selected any, id string) error {
	if err := validateStandaloneStorageEvent(selected, id); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return delivery.CorruptDeliveryConformanceRouteTx(ctx, tx, postgres, id)
	})
}
