package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type declarativeWorkflowNode struct {
	nodeID      string
	coordinator *FactoryPipelineCoordinator
}

func (n *declarativeWorkflowNode) NodeID() string {
	if n == nil {
		return ""
	}
	return strings.TrimSpace(n.nodeID)
}

func (n *declarativeWorkflowNode) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(n.NodeID())
}

func (n *declarativeWorkflowNode) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	if n == nil {
		return false, false
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(string(evt.Type))
	}
	policy, ok := workflowNodeEventPolicy(n.NodeID(), eventType)
	if !ok {
		return false, false
	}
	if policy.RequireEntity && workflowEventEntityID(evt) == "" {
		return false, false
	}
	return policy.Consume, true
}

func (n *declarativeWorkflowNode) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil {
		return false
	}
	return n.coordinator.executeNodeHandlerPlan(ctx, n.NodeID(), evt)
}
