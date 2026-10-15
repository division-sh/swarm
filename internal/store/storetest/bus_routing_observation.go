package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type BusAncestorDeliveryDiagnostic = private.BusAncestorDeliveryDiagnostic

type BusRoutingEventDiagnostic = private.BusRoutingEventDiagnostic
type BusRoutingDeliveryDiagnostic = private.BusRoutingDeliveryDiagnostic

func CountBusValidationDeadLetterNode(ctx context.Context, selected any, node string) (int, error) {
	return private.CountBusValidationDeadLetterNodeForTest(ctx, selected, node)
}

func CountUndeclaredBusChildEvents(ctx context.Context, selected any) (int, error) {
	return private.CountUndeclaredBusChildEventsForTest(ctx, selected)
}

func CountUndeclaredBusChildEntities(ctx context.Context, selected any) (int, error) {
	return private.CountUndeclaredBusChildEntitiesForTest(ctx, selected)
}

func ReadBusRoutingDeliveryDiagnostics(ctx context.Context, selected any) ([]BusRoutingDeliveryDiagnostic, error) {
	return private.ReadBusRoutingDeliveryDiagnosticsForTest(ctx, selected)
}

func ReadBusRoutingEventDiagnostics(ctx context.Context, selected any) ([]BusRoutingEventDiagnostic, error) {
	return private.ReadBusRoutingEventDiagnosticsForTest(ctx, selected)
}

func ReadBusRoutingEventNames(ctx context.Context, selected any) ([]string, error) {
	return private.ReadBusRoutingEventNamesForTest(ctx, selected)
}

func ReadBusAncestorDeliveryDiagnostics(ctx context.Context, selected any) ([]BusAncestorDeliveryDiagnostic, error) {
	return private.ReadBusAncestorDeliveryDiagnosticsForTest(ctx, selected)
}
