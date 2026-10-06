package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadServedIncompletePipelineHandoffCount(ctx context.Context, selected any, runID string) (int, error) {
	return private.ReadServedIncompletePipelineHandoffCountForTest(ctx, selected, runID)
}

func ReadServedDeliveryStatusCount(ctx context.Context, selected any, eventID, subscriberType, subscriberID string, statuses ...string) (int, error) {
	return private.ReadServedDeliveryStatusCountForTest(ctx, selected, eventID, subscriberType, subscriberID, statuses...)
}

func ReadServedRunDebugSummary(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadServedRunDebugSummaryForTest(ctx, selected, runID)
}
