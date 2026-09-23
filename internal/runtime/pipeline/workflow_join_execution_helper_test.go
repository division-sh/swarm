package pipeline

import (
	"context"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Resolve the compiled join occurrence, then execute through the retained handler.
func executeResolvedJoinForTest(pc *PipelineCoordinator, ctx context.Context, evt events.Event, trigger workflowTriggerContext) (contractHandlerExecutionResult, error) {
	if pc == nil || pc.SemanticSource() == nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, nil
	}
	source := pc.SemanticSource()
	resolution, found, err := resolveWorkflowJoinOccurrence(source, evt)
	if err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
	}
	if !found {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, nil
	}
	if strings.TrimSpace(trigger.HandlerEventKey) == "" {
		trigger.HandlerEventKey = resolution.Ref.HandlerEvent()
	}
	flowID := resolution.Ref.FlowPath()
	if flowID == "" {
		flowID = semanticview.RootExecutionFlowID(source)
	}
	return pc.executeNodeContractHandler(withPipelineFlowScope(ctx, flowID), resolution.Ref.Node(), resolution.Handler, trigger, false, true)
}
