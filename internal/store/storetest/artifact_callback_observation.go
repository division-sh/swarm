package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ArtifactCallbackEventContextStorage = runtimepersistence.ArtifactCallbackEventContextStorage

func ReadPostgresArtifactCallbackEventContext(ctx context.Context, selected any, event string) (ArtifactCallbackEventContextStorage, error) {
	return runtimepersistence.ReadPostgresArtifactCallbackEventContextForTest(ctx, selected, event)
}

func ReadPostgresArtifactCallbackSourceRoute(ctx context.Context, selected any, event string) (string, error) {
	return runtimepersistence.ReadPostgresArtifactCallbackSourceRouteForTest(ctx, selected, event)
}

func CountPostgresArtifactCallbackSettledTarget(ctx context.Context, selected any, event, target, node string) (int, error) {
	return runtimepersistence.CountPostgresArtifactCallbackSettledTargetForTest(ctx, selected, event, target, node)
}

func ReadPostgresArtifactCallbackTargetDiagnostics(ctx context.Context, selected any, event string) (string, error) {
	return runtimepersistence.ReadPostgresArtifactCallbackTargetDiagnosticsForTest(ctx, selected, event)
}

func ReadPostgresArtifactCallbackDeliveries(ctx context.Context, selected any, event string) (string, error) {
	return runtimepersistence.ReadPostgresArtifactCallbackDeliveriesForTest(ctx, selected, event)
}

func ReadPostgresArtifactCallbackReceipts(ctx context.Context, selected any, event string) (string, error) {
	return runtimepersistence.ReadPostgresArtifactCallbackReceiptsForTest(ctx, selected, event)
}

func ReadPostgresArtifactCallbackSettlementDiagnostics(ctx context.Context, selected any, event string) (string, error) {
	return runtimepersistence.ReadPostgresArtifactCallbackSettlementDiagnosticsForTest(ctx, selected, event)
}
