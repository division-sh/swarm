package pipeline

import (
	"context"
	"time"

	"empireai/internal/events"
)

func (pc *FactoryPipelineCoordinator) handleScanRequested(ctx context.Context, evt events.Event) {
	pc.scanCoordinator.handleScanRequested(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleDiscoveryReport(ctx context.Context, evt events.Event) {
	pc.scanCoordinator.handleDiscoveryReport(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleScanCompletion(ctx context.Context, evt events.Event) {
	pc.scanCoordinator.handleScanCompletion(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleDedupResolved(ctx context.Context, evt events.Event) {
	pc.scanCoordinator.handleDedupResolved(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) pendingDedupCountForScan(scanID string) int {
	return pc.scanCoordinator.pendingDedupCountForScan(scanID)
}

func (pc *FactoryPipelineCoordinator) checkScanTimeouts(ctx context.Context, now time.Time) {
	pc.scanCoordinator.checkTimeouts(ctx, now)
}
