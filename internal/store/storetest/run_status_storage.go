package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type PostgresRunCompletionIdentityStorage = private.PostgresRunCompletionIdentityStorage
type PostgresRunStatusClockStorage = private.PostgresRunStatusClockStorage
type PostgresRunStatusCountsStorage = private.PostgresRunStatusCountsStorage
type PostgresRunStatusSettlementStorage = private.PostgresRunStatusSettlementStorage
type PostgresRunStatusFailureDiagnosticStorage = private.PostgresRunStatusFailureDiagnosticStorage

func ReadPostgresRunCompletionIdentityStorage(ctx context.Context, selected any, eventID string) (PostgresRunCompletionIdentityStorage, error) {
	return private.ReadPostgresRunCompletionIdentityStorageForTest(ctx, selected, eventID)
}

func ReadPostgresRunStatusClockStorage(ctx context.Context, selected any, runID string) (PostgresRunStatusClockStorage, error) {
	return private.ReadPostgresRunStatusClockStorageForTest(ctx, selected, runID)
}

func ReadPostgresRunStatusCountsStorage(ctx context.Context, selected any, runID string) (PostgresRunStatusCountsStorage, error) {
	return private.ReadPostgresRunStatusCountsStorageForTest(ctx, selected, runID)
}

func CountPostgresExtraRunningStatusRows(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountPostgresExtraRunningStatusRowsForTest(ctx, selected, runID)
}

func ReadPostgresRunStatusSettlementStorage(ctx context.Context, selected any, runID string) (PostgresRunStatusSettlementStorage, error) {
	return private.ReadPostgresRunStatusSettlementStorageForTest(ctx, selected, runID)
}

func CountPostgresRunStatusActiveDeliveries(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountPostgresRunStatusActiveDeliveriesForTest(ctx, selected, runID)
}

func ReadPostgresRunStatusFailureDiagnosticsStorage(ctx context.Context, selected any, runID string) ([]PostgresRunStatusFailureDiagnosticStorage, error) {
	return private.ReadPostgresRunStatusFailureDiagnosticsStorageForTest(ctx, selected, runID)
}

func CountPostgresRunStatusSemanticEvents(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountPostgresRunStatusSemanticEventsForTest(ctx, selected, runID)
}
