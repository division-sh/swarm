package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type PostgresRunCompletionIdentityStorage = runlifecycle.PostgresRunCompletionIdentityStorage
type PostgresRunStatusClockStorage = runlifecycle.PostgresRunStatusClockStorage
type PostgresRunStatusCountsStorage = runlifecycle.PostgresRunStatusCountsStorage
type PostgresRunStatusSettlementStorage = delivery.PostgresRunStatusSettlementStorage
type PostgresRunStatusFailureDiagnosticStorage = delivery.PostgresRunStatusFailureDiagnosticStorage

func ReadPostgresRunCompletionIdentityStorageForTest(ctx context.Context, selected any, eventID string) (PostgresRunCompletionIdentityStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, eventID); err != nil {
		return PostgresRunCompletionIdentityStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresRunCompletionIdentityStorage, error) {
		return runlifecycle.ReadPostgresRunCompletionIdentityStorageTx(ctx, tx, eventID)
	})
}

func ReadPostgresRunStatusClockStorageForTest(ctx context.Context, selected any, runID string) (PostgresRunStatusClockStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return PostgresRunStatusClockStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresRunStatusClockStorage, error) {
		return runlifecycle.ReadPostgresRunStatusClockStorageTx(ctx, tx, runID)
	})
}

func ReadPostgresRunStatusCountsStorageForTest(ctx context.Context, selected any, runID string) (PostgresRunStatusCountsStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return PostgresRunStatusCountsStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresRunStatusCountsStorage, error) {
		return runlifecycle.ReadPostgresRunStatusCountsStorageTx(ctx, tx, runID)
	})
}

func CountPostgresExtraRunningStatusRowsForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return runlifecycle.CountPostgresExtraRunningStatusRowsTx(ctx, tx, runID)
	})
}

func ReadPostgresRunStatusSettlementStorageForTest(ctx context.Context, selected any, runID string) (PostgresRunStatusSettlementStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return PostgresRunStatusSettlementStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresRunStatusSettlementStorage, error) {
		return delivery.ReadPostgresRunStatusSettlementStorageTx(ctx, tx, runID)
	})
}

func CountPostgresRunStatusActiveDeliveriesForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountPostgresRunStatusActiveDeliveriesTx(ctx, tx, runID)
	})
}

func ReadPostgresRunStatusFailureDiagnosticsStorageForTest(ctx context.Context, selected any, runID string) ([]PostgresRunStatusFailureDiagnosticStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return nil, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) ([]PostgresRunStatusFailureDiagnosticStorage, error) {
		return delivery.ReadPostgresRunStatusFailureDiagnosticsStorageTx(ctx, tx, runID)
	})
}
