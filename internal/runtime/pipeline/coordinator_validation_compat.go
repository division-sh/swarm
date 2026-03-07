package pipeline

import (
	"context"
	"time"

	"empireai/internal/events"
)

func (pc *FactoryPipelineCoordinator) handleValidationStarted(ctx context.Context, evt events.Event) {
	pc.validationGate.handleValidationStarted(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleValidationGate(ctx context.Context, evt events.Event, gate string) {
	pc.validationGate.handleValidationGate(ctx, evt, gate)
}

func (pc *FactoryPipelineCoordinator) handleSpecValidationPassed(ctx context.Context, evt events.Event) {
	pc.validationGate.handleSpecValidationPassed(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleSpecValidationFailed(ctx context.Context, evt events.Event) {
	pc.validationGate.handleSpecValidationFailed(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleCTORevisionNeeded(ctx context.Context, evt events.Event) {
	pc.validationGate.handleCTORevisionNeeded(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleValidationRejected(ctx context.Context, evt events.Event) {
	pc.validationGate.handleValidationRejected(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleValidationPackaged(ctx context.Context, evt events.Event) {
	pc.validationGate.handleValidationPackaged(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleValidationMoreData(ctx context.Context, evt events.Event) {
	pc.validationGate.handleValidationMoreData(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleBrandRevision(ctx context.Context, evt events.Event) {
	pc.validationGate.handleBrandRevision(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleVerticalResumed(ctx context.Context, evt events.Event) {
	pc.validationGate.handleVerticalResumed(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleCTOApproved(ctx context.Context, evt events.Event) {
	pc.validationGate.handleCTOApproved(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleVerticalApproved(ctx context.Context, evt events.Event) {
	pc.validationGate.handleVerticalApproved(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleVerticalKilled(ctx context.Context, evt events.Event) {
	pc.validationGate.handleVerticalKilled(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleOpCoCEOReady(ctx context.Context, evt events.Event) {
	pc.validationGate.handleOpCoCEOReady(ctx, evt)
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
