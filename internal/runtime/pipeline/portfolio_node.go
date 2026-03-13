package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type PortfolioNode struct {
	coordinator *FactoryPipelineCoordinator
}

func (n *PortfolioNode) NodeID() string { return "portfolio-node" }

func (n *PortfolioNode) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(n.NodeID())
}

func (n *PortfolioNode) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
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
	}
	policy, ok := workflowNodeEventPolicy(n.NodeID(), eventType)
	if !ok {
		return false, false
	}
	return policy.Consume, true
}

func (n *PortfolioNode) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil {
		return false
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "timer.portfolio_digest":
		n.coordinator.forwardPortfolioDigestTimer(ctx, evt)
	case "runtime.reset":
		n.coordinator.resetWorkflowRuntimeState(ctx)
	case "budget.threshold_crossed":
		_, _ = n.coordinator.applyWorkflowEventTransition(ctx, evt)
	case "system.directive":
		n.coordinator.handlePortfolioSystemDirective(ctx, evt)
	case "opco.spinup_requested":
		return n.coordinator.executeNodeHandlerPlan(ctx, n.NodeID(), evt)
	default:
		return false
	}
	return true
}
