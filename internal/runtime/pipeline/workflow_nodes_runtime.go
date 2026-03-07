package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type workflowNodeExecutor = WorkflowNodeExecutor

type scanWorkflowRuntime interface {
	handlePortfolioDigestTimer(context.Context, events.Event)
	planAndPersistShards(context.Context, events.Event, string, string, map[string]any) int
	publish(context.Context, string, string, map[string]any)
	logPrefilterSkip(context.Context, events.Event, string, string, string, string, map[string]any, float64, float64)
	markShardCompletedByAgent(context.Context, string) string
	shardTerminalProgress(context.Context, string) (int, int, int, bool)
	loadVerticalsByGeography(context.Context, string) ([]verticalCandidate, error)
	ensureVerticalDiscovered(context.Context, string, string, string, map[string]any) (string, error)
}

type scoringWorkflowRuntime interface {
	handleVerticalDerived(context.Context, events.Event)
	loadScoringSeed(context.Context, string) (string, string, string)
	publish(context.Context, string, string, map[string]any)
	updateScoredVerticalState(context.Context, string, string, map[string]any, string)
	appendScoringDigestBuffer(context.Context, VerticalScoredPayload)
}

type validationWorkflowRuntime interface {
	updateVerticalStage(context.Context, string, string, string)
	publish(context.Context, string, string, map[string]any)
	parkVerticalWithMailbox(context.Context, string, string, map[string]any)
	specVersionMatches(string, map[string]any) bool
}

func (n *ScanCoordinator) NodeID() string { return "pipeline-coordinator" }

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
	if policy.RequireVertical && strings.TrimSpace(evt.VerticalID) == "" {
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
		// synthesis is a pure judgment refinement; discovery accumulation
		// already consumed raw reports and does not need additional state here.
	default:
		return false
	}
	return true
}

func (n *ScoringState) NodeID() string { return "scoring-node" }

func (n *ScoringState) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(n.NodeID())
}

func (n *ScoringState) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = strings.TrimSpace(string(evt.Type))
	}
	if eventType == "vertical.scored" {
		payload := parsePayloadMap(evt.Payload)
		result := strings.ToLower(strings.TrimSpace(asString(payload["result"])))
		switch result {
		case "marginal", "rejected":
			return true, true
		default:
			return false, true
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

func (n *ScoringState) Handle(ctx context.Context, evt events.Event) bool {
	if n == nil || n.runtime == nil {
		return false
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "vertical.derived":
		n.runtime.handleVerticalDerived(ctx, evt)
	case "vertical.scored":
		// Delivery filtering for this event type is handled in InterceptPolicy.
	default:
		return false
	}
	return true
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

func (pc *FactoryPipelineCoordinator) workflowNodeExecutors() []workflowNodeExecutor {
	out := make([]workflowNodeExecutor, 0, 3)
	if pc != nil && pc.scanCoordinator != nil {
		out = append(out, pc.scanCoordinator)
	}
	if pc != nil && pc.scoringState != nil {
		out = append(out, pc.scoringState)
	}
	if pc != nil && pc.validationGate != nil {
		out = append(out, pc.validationGate)
	}
	return out
}

func (pc *FactoryPipelineCoordinator) workflowNodeInterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	for _, executor := range pc.workflowNodeExecutors() {
		if consume, handled := executor.InterceptPolicy(eventType, evt); handled {
			return consume, true
		}
	}
	return false, false
}

func (pc *FactoryPipelineCoordinator) dispatchWorkflowNodeEvent(ctx context.Context, evt events.Event) bool {
	eventType := strings.TrimSpace(string(evt.Type))
	if eventType == "" {
		return false
	}
	for _, executor := range pc.workflowNodeExecutors() {
		if handled := executor.Handle(ctx, evt); handled {
			return true
		}
	}
	return false
}
