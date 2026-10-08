package runtime

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// Boot wires EventBus before Manager exists. This forwarding owner preserves
// creation/finalization capability when the Manager reference is published.
type runtimeFlowConstructionOwner struct {
	current func() *manager.AgentManager
}

func (o runtimeFlowConstructionOwner) manager() (*manager.AgentManager, error) {
	if o.current == nil {
		return nil, fmt.Errorf("flow construction manager is required")
	}
	owner := o.current()
	if owner == nil {
		return nil, fmt.Errorf("flow construction manager is required")
	}
	return owner, nil
}

func (o runtimeFlowConstructionOwner) PrepareFlowInstanceActivation(ctx context.Context, request pipeline.FlowInstanceActivationRequest) (pipeline.FlowInstanceActivationPlan, error) {
	owner, err := o.manager()
	if err != nil {
		return pipeline.FlowInstanceActivationPlan{}, err
	}
	return owner.PrepareFlowInstanceActivation(ctx, request)
}

func (o runtimeFlowConstructionOwner) FinalizeCommittedFlowInstanceActivation(ctx context.Context, committed pipeline.CommittedFlowInstanceActivation) error {
	owner, err := o.manager()
	if err != nil {
		return err
	}
	return owner.FinalizeCommittedFlowInstanceActivation(ctx, committed)
}

var _ pipeline.FlowInstanceActivationPlanner = runtimeFlowConstructionOwner{}
var _ pipeline.CommittedFlowInstanceActivationFinalizer = runtimeFlowConstructionOwner{}
