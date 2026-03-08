package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type workflowNodeExecutor = WorkflowNodeExecutor

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
