package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type scoringWorkflowRuntime interface {
	handleVerticalDerived(context.Context, events.Event)
	loadScoringSeed(context.Context, string) (string, string, string)
	publish(context.Context, string, string, map[string]any)
	applyWorkflowEventTransition(context.Context, events.Event) (workflowTransitionOutcome, bool)
	updateScoredVerticalState(context.Context, string, string, map[string]any, string)
	appendScoringDigestBuffer(context.Context, VerticalScoredPayload)
	persistWorkflowScoringAccumulator(context.Context, *scoringAccumulator)
	clearWorkflowScoringAccumulator(context.Context, string)
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
