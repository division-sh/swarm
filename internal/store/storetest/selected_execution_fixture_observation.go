package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountSelectedExecutionRootFlowHeadersStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountSelectedExecutionRootFlowHeadersStorageForTest(ctx, selected, runID)
}

func CountSelectedExecutionRootEntityRowsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountSelectedExecutionRootEntityRowsStorageForTest(ctx, selected, runID)
}

func CountEventReceiptStorage(ctx context.Context, selected any, eventID string) (int, error) {
	return private.CountEventReceiptStorageForTest(ctx, selected, eventID)
}

func CountSelectedExecutionEventMappingStorage(ctx context.Context, selected any, forkRunID, sourceRunID, sourceEventID, forkEventID string) (int, error) {
	return private.CountSelectedExecutionEventMappingStorageForTest(ctx, selected, forkRunID, sourceRunID, sourceEventID, forkEventID)
}

type SelectedExecutionEventStorage = private.SelectedExecutionEventStorage
type SelectedExecutionFollowUpCounts = private.SelectedExecutionFollowUpCounts

func ReadSelectedExecutionEventStorage(ctx context.Context, selected any, eventID string) (SelectedExecutionEventStorage, error) {
	return private.ReadSelectedExecutionEventStorageForTest(ctx, selected, eventID)
}

func ReadSelectedExecutionFollowUpCounts(ctx context.Context, selected any, runID, sourceEventID string) (SelectedExecutionFollowUpCounts, error) {
	return private.ReadSelectedExecutionFollowUpCountsForTest(ctx, selected, runID, sourceEventID)
}

func ReadSelectedExecutionPayloadsStorage(ctx context.Context, selected any, runID string) ([][]byte, error) {
	return private.ReadSelectedExecutionPayloadsStorageForTest(ctx, selected, runID)
}

func CountSelectedAgentFollowUpStorage(ctx context.Context, selected any, runID, sourceEventID string) (int, error) {
	return private.CountSelectedAgentFollowUpStorageForTest(ctx, selected, runID, sourceEventID)
}

type SelectedNodeDeliveryFailureStorage = private.SelectedNodeDeliveryFailureStorage

func ReadSelectedNodeDeliveryFailureStorage(ctx context.Context, selected any, runID, eventID string) (SelectedNodeDeliveryFailureStorage, error) {
	return private.ReadSelectedNodeDeliveryFailureStorageForTest(ctx, selected, runID, eventID)
}

func CountRunEventSubscriberDeliveriesStorage(ctx context.Context, selected any, runID, eventID, subscriberID string) (int, error) {
	return private.CountRunEventSubscriberDeliveriesStorageForTest(ctx, selected, runID, eventID, subscriberID)
}

func CountForkSourceSubscriberActiveDeliveriesStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountForkSourceSubscriberActiveDeliveriesStorageForTest(ctx, selected, runID)
}

func CountRunNamedAgentStorage(ctx context.Context, selected any, runID, agentID string) (int, error) {
	return private.CountRunNamedAgentStorageForTest(ctx, selected, runID, agentID)
}

func CountPostgresSelectedForkCancelledBranchEvidenceStorage(ctx context.Context, selected any, forkRunID, sourceRunID, forkEventID, policy string) (int, error) {
	return private.CountPostgresSelectedForkCancelledBranchEvidenceStorageForTest(ctx, selected, forkRunID, sourceRunID, forkEventID, policy)
}

func CountRunEventAgentDeliveriesStorage(ctx context.Context, selected any, runID, eventID, agentID string) (int, error) {
	return private.CountRunEventAgentDeliveriesStorageForTest(ctx, selected, runID, eventID, agentID)
}

func CountDeliveredAgentAttemptsStorage(ctx context.Context, selected any, eventID, agentID string) (int, error) {
	return private.CountDeliveredAgentAttemptsStorageForTest(ctx, selected, eventID, agentID)
}

func ReadLatestDeadLetterFailureStorage(ctx context.Context, selected any) (string, error) {
	return private.ReadLatestDeadLetterFailureStorageForTest(ctx, selected)
}

func ReadLatestReceiptFailureStorage(ctx context.Context, selected any) (string, error) {
	return private.ReadLatestReceiptFailureStorageForTest(ctx, selected)
}

func ReadPostgresSelectedAgentDeliveryFailureStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadPostgresSelectedAgentDeliveryFailureStorageForTest(ctx, selected, runID)
}

func ReadPostgresRunDeliveryAttemptArrayStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadPostgresRunDeliveryAttemptArrayStorageForTest(ctx, selected, runID)
}

func ReadPostgresSelectedForkDeliveryTimelineStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadPostgresSelectedForkDeliveryTimelineStorageForTest(ctx, selected, runID)
}

func ReadPostgresSelectedForkRouteTimelineStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadPostgresSelectedForkRouteTimelineStorageForTest(ctx, selected, runID)
}

func ReadPostgresSelectedForkEventTimelineStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadPostgresSelectedForkEventTimelineStorageForTest(ctx, selected, runID)
}

func ReadPostgresRunRuntimeLogArrayStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadPostgresRunRuntimeLogArrayStorageForTest(ctx, selected, runID)
}

func CountPostgresSelectedForkLocalRuntimeDiagnosticsStorage(ctx context.Context, selected any, runID, sourceEventID, lineageOwner string) (int, error) {
	return private.CountPostgresSelectedForkLocalRuntimeDiagnosticsStorageForTest(ctx, selected, runID, sourceEventID, lineageOwner)
}
