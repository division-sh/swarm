package pipeline

import (
	"context"
	"strings"
	"time"

	"empireai/internal/events"
)

func (pc *FactoryPipelineCoordinator) handleValidationStarted(ctx context.Context, evt events.Event) {
	(&ValidationOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleValidationGate(ctx context.Context, evt events.Event, gate string) {
	_ = gate
	(&ValidationOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleSpecValidationPassed(ctx context.Context, evt events.Event) {
	(&ValidationOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleSpecValidationFailed(ctx context.Context, evt events.Event) {
	(&ValidationOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleCTORevisionNeeded(ctx context.Context, evt events.Event) {
	(&ValidationOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleValidationRejected(ctx context.Context, evt events.Event) {
	(&ValidationOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleValidationPackaged(ctx context.Context, evt events.Event) {
	(&ValidationOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleValidationMoreData(ctx context.Context, evt events.Event) {
	(&ValidationOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleBrandRevision(ctx context.Context, evt events.Event) {
	(&ValidationOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleVerticalResumed(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleCTOApproved(ctx context.Context, evt events.Event) {
	(&ValidationOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleVerticalApproved(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleVerticalKilled(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleOpCoCEOReady(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleInnerSpecRevision(ctx context.Context, evt events.Event) bool {
	return pc.validationGate.handleInnerSpecRevision(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleSpecRevisionRequested(evt events.Event) {
	pc.validationGate.handleSpecRevisionRequested(evt)
}

func (pc *FactoryPipelineCoordinator) checkPackagingTimeouts(ctx context.Context, now time.Time) {
	pc.validationGate.checkPackagingTimeouts(ctx, now)
}

func (pc *FactoryPipelineCoordinator) handleRuntimeReset(ctx context.Context, _ events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, events.Event{Type: events.EventType("runtime.reset")})
}

func (pc *FactoryPipelineCoordinator) handleMarginalReviewTimer(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleLifecyclePortfolioDigestTimer(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) countVerticalsInStage(ctx context.Context, stage string) int {
	if pc == nil || pc.db == nil || strings.TrimSpace(stage) == "" {
		return 0
	}
	var count int
	_ = dbQueryRowContext(ctx, pc.db, `
		SELECT COUNT(*)
		FROM verticals
		WHERE stage = $1
	`, strings.TrimSpace(stage)).Scan(&count)
	return count
}

func (pc *FactoryPipelineCoordinator) handleBudgetThresholdCrossed(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleMailboxItemDecided(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleQAValidationPassed(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleReviewDeployFeedback(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleSystemDirective(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleBuildComplete(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleLaunchReady(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleOpcoSteadyStateReached(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleOpcoGrowthTriggered(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleOpcoGrowthStabilized(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleOpcoTeardownRequested(ctx context.Context, evt events.Event) {
	(&LifecycleOrchestrator{coordinator: pc}).Handle(ctx, evt)
}
