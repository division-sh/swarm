package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type LifecycleOrchestrator struct {
	coordinator *FactoryPipelineCoordinator
}

func (n *LifecycleOrchestrator) NodeID() string { return "lifecycle-orchestrator" }

func (n *LifecycleOrchestrator) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(n.NodeID())
}

func (n *LifecycleOrchestrator) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(string(evt.Type))
	}
	payload := parsePayloadMap(evt.Payload)
	switch eventType {
	case "timer.portfolio_digest":
		if boolFromAny(payload["scoring_rejections_injected"]) {
			return false, false
		}
	case "timer.marginal_review":
		if boolFromAny(payload["review_request_injected"]) {
			return false, false
		}
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

func (n *LifecycleOrchestrator) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return false
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "vertical.approved":
		n.handleVerticalApproved(ctx, evt)
	case "vertical.killed":
		n.handleVerticalKilled(ctx, evt)
	case "vertical.resumed":
		n.handleVerticalResumed(ctx, evt)
	case "opco.ceo_ready":
		n.handleOpCoCEOReady(ctx, evt)
	case "timer.marginal_kill":
		n.coordinator.applyMarginalKillTimer(ctx, evt)
	case "timer.portfolio_digest":
		n.handlePortfolioDigestTimer(ctx, evt)
	case "runtime.reset":
		n.handleRuntimeReset(ctx)
	case "budget.threshold_crossed":
		n.handleBudgetThresholdCrossed(ctx, evt)
	case "mailbox.item_decided":
		n.handleMailboxItemDecided(ctx, evt)
	case "qa.validation_passed":
		n.handleQAValidationPassed(ctx, evt)
	case "review.deploy_feedback":
		n.handleReviewDeployFeedback(ctx, evt)
	case "system.directive":
		n.handleSystemDirective(ctx, evt)
	case "build_complete":
		n.handleBuildComplete(ctx, evt)
	case "launch_ready":
		n.handleLaunchReady(ctx, evt)
	case "opco.steady_state_reached":
		n.handleOpcoSteadyStateReached(ctx, evt)
	case "opco.growth_triggered":
		n.handleOpcoGrowthTriggered(ctx, evt)
	case "opco.growth_stabilized":
		n.handleOpcoGrowthStabilized(ctx, evt)
	case "opco.teardown_requested":
		n.handleOpcoTeardownRequested(ctx, evt)
	case "timer.marginal_review":
		n.handleMarginalReviewTimer(ctx, evt)
		return true
	default:
		return false
	}
	return true
}
