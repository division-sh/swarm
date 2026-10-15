package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/adminpersistence"
	"github.com/division-sh/swarm/internal/store/internal/apiidempotency"
)

func ReadResetCompletionStorageRecordForTest(ctx context.Context, selected any) ([]byte, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, adminpersistence.ReadResetCompletionStorageRecordTx)
}

func CountResetCompletionStorageOperationsForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, adminpersistence.CountResetCompletionStorageOperationsTx)
}

func InstallPostgresResetAPICompletionLossForTest(ctx context.Context, selected any) error {
	if _, ok := selected.(*PostgresStore); !ok {
		return fmt.Errorf("reset API-completion loss requires the original postgres owner")
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, apiidempotency.InstallPostgresResetAPICompletionLossTx)
}

func RemovePostgresResetAPICompletionLossForTest(ctx context.Context, selected any) error {
	if _, ok := selected.(*PostgresStore); !ok {
		return fmt.Errorf("reset API-completion loss requires the original postgres owner")
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, apiidempotency.RemovePostgresResetAPICompletionLossTx)
}
