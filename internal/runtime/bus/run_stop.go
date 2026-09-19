package bus

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func (eb *EventBus) BeginRunStop(ctx context.Context, runID string) (pipelineobligation.ParentTransition, error) {
	if eb == nil {
		return nil, errors.New("run stop requires a pipeline obligation owner")
	}
	owner, ok := eb.pipelineObligations.(pipelineobligation.ParentTransitionOwner)
	if !ok {
		return nil, errors.New("run stop requires selected-store pipeline parent transition admission")
	}
	return owner.BeginParentTransition(ctx, runID)
}
