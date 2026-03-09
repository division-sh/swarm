package pipeline

import (
	"context"
	"strings"
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

func (pc *FactoryPipelineCoordinator) handleSynthesisResolved(ctx context.Context, evt events.Event) {
	pc.scanCoordinator.handleSynthesisResolved(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) pendingDedupCountForScan(scanID string) int {
	scanID = strings.TrimSpace(scanID)
	if scanID == "" {
		return 0
	}
	pc.mu.Lock()
	pending := pc.scanCoordinator.pendingDedup
	pc.mu.Unlock()
	count := 0
	for _, cand := range pending {
		if strings.TrimSpace(cand.ScanID) == scanID {
			count++
		}
	}
	if count > 0 {
		return count
	}
	if _, restoredPending, ok := pc.loadWorkflowScanProjection(context.Background(), scanID); ok {
		return len(restoredPending)
	}
	return 0
}

func (pc *FactoryPipelineCoordinator) checkScanTimeouts(ctx context.Context, now time.Time) {
	pc.scanCoordinator.checkTimeouts(ctx, now)
}

func (pc *FactoryPipelineCoordinator) handleScanTimeoutTimer(ctx context.Context, _ events.Event) {
	pc.scanCoordinator.handleScanTimeout(ctx, events.Event{})
}

func (pc *FactoryPipelineCoordinator) handleCampaignDeadlineTimer(ctx context.Context, evt events.Event) {
	pc.scanCoordinator.handleCampaignDeadline(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleMarginalKillTimer(ctx context.Context, evt events.Event) {
	pc.applyMarginalKillTimer(ctx, evt)
}
