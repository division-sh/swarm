package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type ScanOrchestrator struct {
	coordinator *ScanCoordinator
}

func (n *ScanOrchestrator) NodeID() string { return "scan-orchestrator" }

func (n *ScanOrchestrator) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(n.NodeID())
}

func (n *ScanOrchestrator) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
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

func (n *ScanOrchestrator) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil {
		return false
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "scan.requested":
		n.handleScanRequested(ctx, evt)
	case "market_research.scan_complete", "trend_research.scan_complete",
		"scanner.google_maps.scan_complete", "scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete", "scanner.directories.scan_complete",
		"scanner.yelp.scan_complete":
		n.handleScanCompletion(ctx, evt)
	case "timer.scan_timeout":
		n.coordinator.handleScanTimeout(ctx, evt)
	case "timer.campaign_deadline":
		n.coordinator.handleCampaignDeadline(ctx, evt)
	default:
		return false
	}
	return true
}
