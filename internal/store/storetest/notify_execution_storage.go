package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type NotifyCompletedTurnsStorage = private.NotifyCompletedTurnsStorage
type NotifyFanOutCursorStorage = private.NotifyFanOutCursorStorage
type NotifyFanOutWorkStorage = private.NotifyFanOutWorkStorage

func ReadNotifyCompletedTurns(ctx context.Context, selected any, runID, agentID string) (NotifyCompletedTurnsStorage, error) {
	return private.ReadNotifyCompletedTurnsForTest(ctx, selected, runID, agentID)
}
func ReadNotifyLifecycleTransitionCount(ctx context.Context, selected any, agentID, previous, next string) (int, error) {
	return private.ReadNotifyLifecycleTransitionCountForTest(ctx, selected, agentID, previous, next)
}
func ReadNotifyLatestEventFailure(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadNotifyLatestEventFailureForTest(ctx, selected, eventID)
}
func ReadNotifyFlowInstanceCount(ctx context.Context, selected any, flow string) (int, error) {
	return private.ReadNotifyFlowInstanceCountForTest(ctx, selected, flow)
}
func ReadNotifyRunPresence(ctx context.Context, selected any, runID string) (int, error) {
	return private.ReadNotifyRunPresenceForTest(ctx, selected, runID)
}
func ReadNotifyFanOutCursor(ctx context.Context, selected any, runID string) (NotifyFanOutCursorStorage, error) {
	return private.ReadNotifyFanOutCursorForTest(ctx, selected, runID)
}
func ReadNotifyFirstRunFailure(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadNotifyFirstRunFailureForTest(ctx, selected, runID)
}
func ReadNotifyFanOutWork(ctx context.Context, selected any, runID string) (NotifyFanOutWorkStorage, error) {
	return private.ReadNotifyFanOutWorkForTest(ctx, selected, runID)
}

func ReadNotifyAgentDeliveryStatus(ctx context.Context, selected any, runID, agentID, instance string) (string, error) {
	return private.ReadNotifyAgentDeliveryStatusForTest(ctx, selected, runID, agentID, instance)
}
