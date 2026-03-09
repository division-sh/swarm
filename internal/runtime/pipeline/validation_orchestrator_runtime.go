package pipeline

import (
	"context"

	"empireai/internal/events"
)

func (n *ValidationOrchestrator) handleValidationStarted(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleValidationStarted(ctx, evt)
}

func (n *ValidationOrchestrator) handleValidationGate(ctx context.Context, evt events.Event, gate string) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleValidationGate(ctx, evt, gate)
}

func (n *ValidationOrchestrator) handleCTOApproved(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleCTOApproved(ctx, evt)
}

func (n *ValidationOrchestrator) handleSpecValidationPassed(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleSpecValidationPassed(ctx, evt)
}

func (n *ValidationOrchestrator) handleSpecValidationFailed(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleSpecValidationFailed(ctx, evt)
}

func (n *ValidationOrchestrator) handleCTORevisionNeeded(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleCTORevisionNeeded(ctx, evt)
}

func (n *ValidationOrchestrator) handleValidationRejected(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleValidationRejected(ctx, evt)
}

func (n *ValidationOrchestrator) handleValidationPackaged(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleValidationPackaged(ctx, evt)
}

func (n *ValidationOrchestrator) handleValidationMoreData(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleValidationMoreData(ctx, evt)
}

func (n *ValidationOrchestrator) handleBrandRevision(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleBrandRevision(ctx, evt)
}

func (n *ValidationOrchestrator) handleSpecRevisionRequested(evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleSpecRevisionRequested(evt)
}

func (n *ValidationOrchestrator) handleInnerSpecRevision(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return false
	}
	return n.coordinator.validationGate.handleInnerSpecRevision(ctx, evt)
}
