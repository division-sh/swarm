package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type BusAncestorDeliveryDiagnostic = private.BusAncestorDeliveryDiagnostic

func ReadBusRoutingEventNames(ctx context.Context, selected any) ([]string, error) {
	return private.ReadBusRoutingEventNamesForTest(ctx, selected)
}

func ReadBusAncestorDeliveryDiagnostics(ctx context.Context, selected any) ([]BusAncestorDeliveryDiagnostic, error) {
	return private.ReadBusAncestorDeliveryDiagnosticsForTest(ctx, selected)
}
