package pipeline

import (
	"context"
	"strings"
	"time"

	"empireai/internal/events"
)

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
