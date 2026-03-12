package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type ValidationOrchestrator struct {
	coordinator *FactoryPipelineCoordinator
}

func (n *ValidationOrchestrator) NodeID() string { return "validation-orchestrator" }

func (n *ValidationOrchestrator) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(n.NodeID())
}

func (n *ValidationOrchestrator) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(string(evt.Type))
	}
	policy, ok := workflowNodeEventPolicy(n.NodeID(), eventType)
	if !ok {
		return false, false
	}
	if policy.RequireVertical && workflowEventEntityID(evt) == "" {
		return false, false
	}
	return policy.Consume, true
}

func (n *ValidationOrchestrator) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return false
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "vertical.shortlisted":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleValidationStarted(ctx, evt)
	case "research.completed":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleValidationGate(ctx, evt, "g1")
	case "spec.approved":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleValidationGate(ctx, evt, "g2")
	case "spec_review.passed":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleSpecReviewPassed(ctx, evt)
	case "spec_review.issues_found":
		n.handleSpecReviewIssuesFound(ctx, evt)
	case "cto.spec_approved":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleCTOApproved(ctx, evt)
	case "brand.candidates_ready":
		n.handleValidationGate(ctx, evt, "g4")
	case "spec.validation_passed":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleSpecValidationPassed(ctx, evt)
	case "spec.validation_failed":
		n.handleSpecValidationFailed(ctx, evt)
	case "cto.spec_revision_needed":
		n.handleCTORevisionNeeded(ctx, evt)
	case "research.vertical_rejected", "cto.spec_vetoed":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleValidationRejected(ctx, evt)
	case "vertical.ready_for_review":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleValidationPackaged(ctx, evt)
	case "vertical.needs_more_data":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleValidationMoreData(ctx, evt)
	case "brand.revision_needed":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleBrandRevision(ctx, evt)
	case "spec.revision_requested":
		if n.executeDeclarativeValidationHandler(ctx, evt) {
			return true
		}
		n.handleSpecRevisionRequested(evt)
	case "spec.revision_needed":
		_ = n.handleInnerSpecRevision(ctx, evt)
	default:
		return false
	}
	return true
}
