package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type DiscoveryAggregator struct {
	coordinator *FactoryPipelineCoordinator
}

func (n *DiscoveryAggregator) NodeID() string { return "discovery-aggregator" }

func (n *DiscoveryAggregator) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(n.NodeID())
}

func (n *DiscoveryAggregator) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(string(evt.Type))
	}
	policy, ok := workflowNodeEventPolicy(n.NodeID(), eventType)
	if !ok {
		return false, false
	}
	return policy.Consume, true
}

func (n *DiscoveryAggregator) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil || n.coordinator.scanCoordinator == nil {
		return false
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "category.assessed", "trend.identified", "source.scraped":
		n.handleDiscoveryReport(ctx, evt)
	case "dedup.resolved":
		n.handleDedupResolved(ctx, evt)
	case "synthesis.resolved":
		n.handleSynthesisResolved(ctx, evt)
	default:
		return false
	}
	return true
}
