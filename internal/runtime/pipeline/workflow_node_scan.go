package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type scanWorkflowRuntime interface {
	handlePortfolioDigestTimer(context.Context, events.Event)
	planAndPersistShards(context.Context, events.Event, string, string, map[string]any) int
	publish(context.Context, string, string, map[string]any)
	logPrefilterSkip(context.Context, events.Event, string, string, string, string, map[string]any, float64, float64)
	markShardCompletedByAgent(context.Context, string) string
	shardTerminalProgress(context.Context, string) (int, int, int, bool)
	loadVerticalsByGeography(context.Context, string) ([]verticalCandidate, error)
	ensureVerticalDiscovered(context.Context, string, string, string, map[string]any) (string, error)
	loadWorkflowScanProjection(context.Context, string) (*scanAccumulator, map[string]pendingCandidate, bool)
}

func (n *ScanCoordinator) NodeID() string { return "scan-orchestrator" }

func (n *ScanCoordinator) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(n.NodeID())
}

func (n *ScanCoordinator) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(string(evt.Type))
	}
	if eventType == "timer.portfolio_digest" {
		payload := parsePayloadMap(evt.Payload)
		if boolFromAny(payload["scoring_rejections_injected"]) {
			return false, false
		}
		return true, true
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

func (n *ScanCoordinator) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.runtime == nil {
		return false
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "timer.portfolio_digest":
		n.runtime.handlePortfolioDigestTimer(ctx, evt)
	case "timer.scan_timeout":
		n.handleScanTimeout(ctx, evt)
	case "timer.campaign_deadline":
		n.handleCampaignDeadline(ctx, evt)
	case "scan.requested":
		n.handleScanRequested(ctx, evt)
	case "category.assessed", "trend.identified", "source.scraped":
		n.handleDiscoveryReport(ctx, evt)
	case "market_research.scan_complete", "trend_research.scan_complete",
		"scanner.google_maps.scan_complete", "scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete", "scanner.directories.scan_complete",
		"scanner.yelp.scan_complete":
		n.handleScanCompletion(ctx, evt)
	case "dedup.resolved":
		n.handleDedupResolved(ctx, evt)
	case "synthesis.resolved":
		n.handleSynthesisResolved(ctx, evt)
	default:
		return false
	}
	return true
}
