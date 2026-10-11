package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type FanInDiagnosticEventStorage = private.FanInDiagnosticEventStorage
type FanInDiagnosticDeliveryStorage = private.FanInDiagnosticDeliveryStorage

func ReadFanInPeriodReportedEventIDs(ctx context.Context, selected any) ([]string, error) {
	return private.ReadFanInPeriodReportedEventIDsForTest(ctx, selected)
}

func ReadFanInOperatingReportedEventIDs(ctx context.Context, selected any) ([]string, error) {
	return private.ReadFanInOperatingReportedEventIDsForTest(ctx, selected)
}

func ReadFanInEventRouteSettlement(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadFanInEventRouteSettlementForTest(ctx, selected, eventID)
}

func ReadFanInDiagnosticEvents(ctx context.Context, selected any) ([]FanInDiagnosticEventStorage, error) {
	return private.ReadFanInDiagnosticEventsForTest(ctx, selected)
}

func ReadFanInDiagnosticDeliveries(ctx context.Context, selected any) ([]FanInDiagnosticDeliveryStorage, error) {
	return private.ReadFanInDiagnosticDeliveriesForTest(ctx, selected)
}

func DeleteFanInSelectedPeriodOwner(ctx context.Context, selected any, runID, entityID string) (int64, error) {
	return private.DeleteFanInSelectedPeriodOwnerForTest(ctx, selected, runID, entityID)
}
