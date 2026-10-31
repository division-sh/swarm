package engine

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func (e *Executor) appendEntityMutation(frame *executionFrame, op entityruntime.Mutation) error {
	if frame.entityMutations == nil {
		contract, ok := entityruntime.ResolveForFlow(e.deps.Source, frame.req.ExecutionFlowID.String())
		if !ok {
			return fmt.Errorf("flow %s has no declared entity contract", frame.req.ExecutionFlowID.String())
		}
		plan, err := entityruntime.NewMutationPlan(contract, frame.state.State.StateCarrier.Fields)
		if err != nil {
			return err
		}
		frame.entityMutations = plan
	}
	if err := frame.entityMutations.Append(op); err != nil {
		return err
	}
	frame.state.State.StateCarrier.Fields = frame.entityMutations.Draft()
	return nil
}

func (e *Executor) validateEntityMutationList(frame *executionFrame) error {
	if frame.entityMutations == nil {
		return nil
	}
	fields, err := frame.entityMutations.Validate()
	if err != nil {
		return err
	}
	frame.state.State.StateCarrier.Fields = fields
	frame.result.StateMutation.StateCarrier.Fields = cloneStringAnyMap(fields)
	return nil
}
