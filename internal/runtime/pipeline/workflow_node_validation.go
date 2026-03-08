package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type validationWorkflowRuntime interface {
	updateVerticalStage(context.Context, string, string, string)
	publish(context.Context, string, string, map[string]any)
	parkVerticalWithMailbox(context.Context, string, string, map[string]any)
	specVersionMatches(string, map[string]any) bool
}

func (n *ValidationGate) NodeID() string { return "pipeline-coordinator" }

func (n *ValidationGate) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(n.NodeID())
}

func (n *ValidationGate) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(string(evt.Type))
	}
	policy, ok := workflowNodeEventPolicy(n.NodeID(), eventType)
	if !ok {
		return false, false
	}
	if policy.RequireVertical && strings.TrimSpace(evt.VerticalID) == "" {
		return false, false
	}
	return policy.Consume, true
}

func (n *ValidationGate) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.runtime == nil {
		return false
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "vertical.shortlisted":
		n.handleValidationStarted(ctx, evt)
	case "research.completed":
		n.handleValidationGate(ctx, evt, "g1")
	case "spec.revision_requested":
		n.handleSpecRevisionRequested(evt)
	case "spec.revision_needed":
		_ = n.handleInnerSpecRevision(ctx, evt)
	case "spec.approved":
		n.handleValidationGate(ctx, evt, "g2")
	case "cto.spec_approved":
		n.handleCTOApproved(ctx, evt)
	case "brand.candidates_ready":
		n.handleValidationGate(ctx, evt, "g4")
	case "spec.validation_passed":
		n.handleSpecValidationPassed(ctx, evt)
	case "spec.validation_failed":
		n.handleSpecValidationFailed(ctx, evt)
	case "vertical.approved":
		n.handleVerticalApproved(ctx, evt)
	case "vertical.killed":
		n.handleVerticalKilled(ctx, evt)
	case "opco.ceo_ready":
		n.handleOpCoCEOReady(ctx, evt)
	case "cto.spec_revision_needed":
		n.handleCTORevisionNeeded(ctx, evt)
	case "research.vertical_rejected", "cto.spec_vetoed":
		n.handleValidationRejected(ctx, evt)
	case "vertical.ready_for_review":
		n.handleValidationPackaged(ctx, evt)
	case "vertical.needs_more_data":
		n.handleValidationMoreData(ctx, evt)
	case "brand.revision_needed":
		n.handleBrandRevision(ctx, evt)
	case "vertical.resumed":
		n.handleVerticalResumed(ctx, evt)
	default:
		return false
	}
	return true
}
