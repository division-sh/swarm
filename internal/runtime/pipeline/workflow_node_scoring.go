package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type scoringWorkflowRuntime interface {
	handleScoringRequested(context.Context, events.Event)
	handleVerticalDerived(context.Context, events.Event)
	handleScoreDimensionComplete(context.Context, events.Event)
	handleScoringContestResolved(context.Context, events.Event)
	loadScoringSeed(context.Context, string) (string, string, string)
	loadWorkflowScoringAccumulator(context.Context, string) (*scoringAccumulator, bool)
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

func (n *ScoringState) BackgroundWorkflowExecutor() WorkflowNodeExecutor {
	if n == nil || n.runtime == nil {
		return nil
	}
	return newScoringBackgroundExecutor(n.runtime)
}

type scoringBackgroundExecutor struct {
	runtime scoringWorkflowRuntime
}

func newScoringBackgroundExecutor(runtime scoringWorkflowRuntime) WorkflowNodeExecutor {
	if runtime == nil {
		return nil
	}
	return &scoringBackgroundExecutor{runtime: runtime}
}

func (e *scoringBackgroundExecutor) NodeID() string { return ScoringNodeID }

func (e *scoringBackgroundExecutor) Subscriptions() []events.EventType {
	return workflowNodeSubscriptions(ScoringNodeID)
}

func (e *scoringBackgroundExecutor) InterceptPolicy(string, events.Event) (bool, bool) {
	return false, false
}

func (e *scoringBackgroundExecutor) Handle(ctx context.Context, evt events.Event) bool {
	if e == nil || e.runtime == nil {
		return false
	}
	ctx = withPipelineSourceAgent(ctx, ScoringNodeID)
	switch strings.TrimSpace(string(evt.Type)) {
	case "vertical.discovered":
		e.runtime.handleScoringRequested(ctx, evt)
	case "vertical.derived":
		e.runtime.handleVerticalDerived(ctx, evt)
	case "score.dimension_complete":
		e.runtime.handleScoreDimensionComplete(ctx, evt)
	case "scoring.contest_resolved":
		e.runtime.handleScoringContestResolved(ctx, evt)
	default:
		return false
	}
	return true
}
