package storetest

import (
	"context"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type APIEventPublicationRefusalStorage = private.APIEventPublicationRefusalStorage
type APIEventReplayStorage = private.APIEventReplayStorage

func ReadLatestNamedEventIdentityStorage(ctx context.Context, selected any, eventName, excludedEventID string) (string, error) {
	return private.ReadLatestNamedEventIdentityStorageForTest(ctx, selected, eventName, excludedEventID)
}

func ReadPipelineReceiptOutcomeStorage(ctx context.Context, selected any, eventID string) (string, *runtimefailures.Envelope, error) {
	return private.ReadPipelineReceiptOutcomeStorageForTest(ctx, selected, eventID)
}

func CountPhysicalEventDeliveries(ctx context.Context, selected any) (int, error) {
	return private.CountPhysicalEventDeliveriesForTest(ctx, selected)
}

func CountAgentEventDeliveryStorage(ctx context.Context, selected any, eventID string) (int, error) {
	return private.CountAgentEventDeliveryStorageForTest(ctx, selected, eventID)
}
func CountPipelineEventReceiptStorage(ctx context.Context, selected any, eventID string) (int, error) {
	return private.CountPipelineEventReceiptStorageForTest(ctx, selected, eventID)
}

func ReadDecisionRouteStatusStorage(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadDecisionRouteStatusStorageForTest(ctx, selected, eventID)
}

func CountPhysicalRunIdentity(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountPhysicalRunIdentityForTest(ctx, selected, runID)
}
func CountPhysicalRunEvents(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountPhysicalRunEventsForTest(ctx, selected, runID)
}

func CountPhysicalEvents(ctx context.Context, selected any) (int, error) {
	return private.CountPhysicalEventsForTest(ctx, selected)
}

func CountPhysicalRuns(ctx context.Context, selected any) (int, error) {
	return private.CountPhysicalRunsForTest(ctx, selected)
}
func CountAPICommandReceipts(ctx context.Context, selected any) (int, error) {
	return private.CountAPICommandReceiptsForTest(ctx, selected)
}

type APIPublicationCardinalityStorage = private.APIPublicationCardinalityStorage

func ReadAPIFlowPublicationRefusalStorage(ctx context.Context, selected any) (APIPublicationCardinalityStorage, error) {
	return private.ReadAPIFlowPublicationRefusalStorageForTest(ctx, selected)
}
func ReadAPIRunStartRefusalStorage(ctx context.Context, selected any, runID string) (APIPublicationCardinalityStorage, error) {
	return private.ReadAPIRunStartRefusalStorageForTest(ctx, selected, runID)
}

func CountEventNameStorage(ctx context.Context, selected any, eventName string) (int, error) {
	return private.CountEventNameStorageForTest(ctx, selected, eventName)
}
func ReadAPIEventPublicationRefusalStorage(ctx context.Context, selected any, eventName string) (APIEventPublicationRefusalStorage, error) {
	return private.ReadAPIEventPublicationRefusalStorageForTest(ctx, selected, eventName)
}
func ReadAPIEventReplayStorage(ctx context.Context, selected any, originalEventID, replayEventID, auditEventID string) (APIEventReplayStorage, error) {
	return private.ReadAPIEventReplayStorageForTest(ctx, selected, originalEventID, replayEventID, auditEventID)
}
