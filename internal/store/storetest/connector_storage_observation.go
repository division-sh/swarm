package storetest

import (
	"context"
	"github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadConnectorNormalizedProviderEventID(ctx context.Context, selected any, runID, entityID, eventName, providerEventID string) (string, error) {
	return runtimepersistence.ReadConnectorNormalizedProviderEventIDForTest(ctx, selected, runID, entityID, eventName, providerEventID)
}

func CountConnectorNamedSourceEvent(ctx context.Context, selected any, runID, eventName, sourceEventID string) (int, error) {
	return runtimepersistence.CountConnectorNamedSourceEventForTest(ctx, selected, runID, eventName, sourceEventID)
}

func CountConnectorNamedEventIdentity(ctx context.Context, selected any, eventID, eventName string) (int, error) {
	return runtimepersistence.CountConnectorNamedEventIdentityForTest(ctx, selected, eventID, eventName)
}

func ReadConnectorEventChronology(ctx context.Context, selected any, runID string) (string, error) {
	return runtimepersistence.ReadConnectorEventChronologyForTest(ctx, selected, runID)
}

func CountConnectorRunNodeDeliveries(ctx context.Context, selected any, runID, nodeID string) (int, error) {
	return runtimepersistence.CountConnectorRunNodeDeliveriesForTest(ctx, selected, runID, nodeID)
}

func ReadConnectorLatestNodeDeliveryDiagnostic(ctx context.Context, selected any, runID, nodeID string) (string, error) {
	return runtimepersistence.ReadConnectorLatestNodeDeliveryDiagnosticForTest(ctx, selected, runID, nodeID)
}

func CountConnectorEntityDeadLetters(ctx context.Context, selected any, entityID string) (int, error) {
	return runtimepersistence.CountConnectorEntityDeadLettersForTest(ctx, selected, entityID)
}

func ReadConnectorLatestEntityDeadLetterDiagnostic(ctx context.Context, selected any, entityID string) (string, error) {
	return runtimepersistence.ReadConnectorLatestEntityDeadLetterDiagnosticForTest(ctx, selected, entityID)
}

func ReadConnectorLatestActivityReceiptDiagnostic(ctx context.Context, selected any, runID string) (string, error) {
	return runtimepersistence.ReadConnectorLatestActivityReceiptDiagnosticForTest(ctx, selected, runID)
}

func CountConnectorRunActivityAttempts(ctx context.Context, selected any, runID string) (int, error) {
	return runtimepersistence.CountConnectorRunActivityAttemptsForTest(ctx, selected, runID)
}

func CountChannelSucceededToolAttempts(ctx context.Context, selected any, runID, tool string) (int, error) {
	return runtimepersistence.CountChannelSucceededToolAttemptsForTest(ctx, selected, runID, tool)
}

func ReadChannelFirstActivitySourceRoute(ctx context.Context, selected any, runID string) ([]byte, error) {
	return runtimepersistence.ReadChannelFirstActivitySourceRouteForTest(ctx, selected, runID)
}
