package pipeline

import (
	"context"
	"database/sql"
	"strings"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
)

const ScoringNodeID = "scoring-node"

func scoringTransitionSubscriptions() []events.EventType {
	return []events.EventType{
		events.EventType("vertical.scored"),
	}
}

func scoringBackgroundSubscriptions() []events.EventType {
	return []events.EventType{
		events.EventType("vertical.discovered"),
		events.EventType("vertical.derived"),
		events.EventType("score.dimension_complete"),
		events.EventType("scoring.contest_resolved"),
	}
}

type scoringBackgroundRuntime interface {
	handleScoringRequested(context.Context, events.Event)
	handleVerticalDerived(context.Context, events.Event)
	handleScoreDimensionComplete(context.Context, events.Event)
	handleScoringContestResolved(context.Context, events.Event)
}

type scoringStateRuntime interface {
	scoringBackgroundRuntime
	loadScoringSeed(context.Context, string) (string, string, string)
	loadWorkflowScoringAccumulator(context.Context, string) (*scoringAccumulator, bool)
	publish(context.Context, string, string, map[string]any)
	applyWorkflowEventTransition(context.Context, events.Event) (workflowTransitionOutcome, bool)
	ContractBundle() *runtimecontracts.WorkflowContractBundle
	currentWorkflowState(context.Context, string) WorkflowState
	matchWorkflowRulesWithVars(workflowTriggerContext, map[string]any, map[string]any) (workflowRuleMatch, bool)
	updateScoredVerticalState(context.Context, string, string, map[string]any, string)
	appendScoringDigestBuffer(context.Context, VerticalScoredPayload)
	persistWorkflowScoringAccumulator(context.Context, *scoringAccumulator)
	clearWorkflowScoringAccumulator(context.Context, string)
}

func NewScoringNode(bus systemNodeBus, runtime scoringBackgroundRuntime, db *sql.DB) *backgroundWorkflowNode {
	if bus == nil || runtime == nil {
		return nil
	}
	executor := newScoringBackgroundExecutor(runtime)
	return newBackgroundWorkflowNode(executor, bus, db)
}

type scoringTransitionExecutor struct {
	runtime scoringBackgroundRuntime
}

type scoringBackgroundExecutor struct {
	runtime scoringBackgroundRuntime
}

func newScoringBackgroundExecutor(runtime scoringBackgroundRuntime) WorkflowNodeExecutor {
	if runtime == nil {
		return nil
	}
	return &scoringBackgroundExecutor{runtime: runtime}
}

func newScoringTransitionExecutor(runtime scoringBackgroundRuntime) WorkflowNodeExecutor {
	if runtime == nil {
		return nil
	}
	return &scoringTransitionExecutor{runtime: runtime}
}

func (e *scoringTransitionExecutor) NodeID() string { return ScoringNodeID }

func (e *scoringTransitionExecutor) Subscriptions() []events.EventType {
	return scoringTransitionSubscriptions()
}

func (e *scoringTransitionExecutor) InterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	if e == nil {
		return false, false
	}
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
	policy, ok := workflowNodeEventPolicy(ScoringNodeID, eventType)
	if !ok {
		return false, false
	}
	if policy.RequireVertical && strings.TrimSpace(evt.VerticalID) == "" {
		return false, false
	}
	return policy.Consume, true
}

func (e *scoringTransitionExecutor) Handle(ctx context.Context, evt events.Event) bool {
	if e == nil || e.runtime == nil {
		return false
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "vertical.scored":
		// Delivery filtering for this event type is handled in InterceptPolicy.
	default:
		return false
	}
	return true
}

func (e *scoringTransitionExecutor) BackgroundWorkflowExecutor() WorkflowNodeExecutor {
	if e == nil || e.runtime == nil {
		return nil
	}
	return newScoringBackgroundExecutor(e.runtime)
}

func (e *scoringBackgroundExecutor) NodeID() string { return ScoringNodeID }

func (e *scoringBackgroundExecutor) Subscriptions() []events.EventType {
	return scoringBackgroundSubscriptions()
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
