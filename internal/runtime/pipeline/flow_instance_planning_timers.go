package pipeline

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

// FlowInstancePlanningInitialTimerReader composes only the constructor's
// initial-entry read. Its owner and lifetime are the canonical index cut.
type FlowInstancePlanningInitialTimerReader interface {
	ListFlowInstancePlanningInitialTimers(context.Context, flowidentity.RunScopedFlowInstance, string) ([]WorkflowTimerActivation, error)
}

type flowInstancePlanningInitialTimerReaderKey struct{}

func WithFlowInstancePlanningInitialTimerReader(ctx context.Context, reader FlowInstancePlanningInitialTimerReader) (context.Context, error) {
	if reader == nil {
		return ctx, fmt.Errorf("initial-entry planning requires its finite timer reader")
	}
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, flowInstancePlanningInitialTimerReaderKey{}, reader), nil
}

func (pc *PipelineCoordinator) readWorkflowPlanningTimers(ctx context.Context, runID string, route flowidentity.Route, entityID string, cause workflowTimerCause) ([]WorkflowTimerActivation, error) {
	if cause.Kind == workflowTimerCauseInitial {
		if reader, ok := ctx.Value(flowInstancePlanningInitialTimerReaderKey{}).(FlowInstancePlanningInitialTimerReader); ok {
			owner, err := flowidentity.NewRunScopedFlowInstance(runID, route)
			if err != nil {
				return nil, err
			}
			return reader.ListFlowInstancePlanningInitialTimers(ctx, owner, entityID)
		}
	}
	return pc.workflowStore.listPersistedWorkflowTimerActivations(ctx, runID, entityID, true)
}
