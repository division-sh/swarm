package storetest

import (
	"context"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountSelectedForkExactExecutionStorage(ctx context.Context, selected any, fork, source, event string) (int, error) {
	return private.CountSelectedForkExactExecutionStorageForTest(ctx, selected, fork, source, event)
}

func CountSelectedForkDeliveryReplayStorage(ctx context.Context, selected any, fork string) (int, error) {
	return private.CountSelectedForkDeliveryReplayStorageForTest(ctx, selected, fork)
}

func CountRunDeliveryStorage(ctx context.Context, selected any, run string) (int, error) {
	return private.CountRunDeliveryStorageForTest(ctx, selected, run)
}

func ReadSelectedForkBindingModeStorage(ctx context.Context, selected any, fork, source, event string) (string, error) {
	return private.ReadSelectedForkBindingModeStorageForTest(ctx, selected, fork, source, event)
}

func ReadCLIMaterializedEntityState(ctx context.Context, selected any, run, entity string) (string, error) {
	return private.ReadCLIMaterializedEntityStateForTest(ctx, selected, run, entity)
}

func CountPostgresForkTypedRuntimeDiagnostic(ctx context.Context, selected any, run, event, owner string) (int, error) {
	return private.CountPostgresForkTypedRuntimeDiagnosticForTest(ctx, selected, run, event, owner)
}

func ReadPostgresGlobalRuntimeDiagnosticStorage(ctx context.Context, selected any) (string, error) {
	return private.ReadPostgresGlobalRuntimeDiagnosticStorageForTest(ctx, selected)
}

func SeedPostgresCLIMaterializationHistory(ctx context.Context, selected any, run, entity, event string, at time.Time) error {
	return private.SeedPostgresCLIMaterializationHistoryForTest(ctx, selected, run, entity, event, at)
}
