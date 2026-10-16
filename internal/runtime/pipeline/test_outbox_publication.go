package pipeline

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	identitypkg "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/engine"
)

// CommitConstructedOutboxFixture exercises the native batch boundary for bus
// component proofs. It cannot create workflow state or export storage authority.
func (pc *PipelineCoordinator) CommitConstructedOutboxFixture(ctx context.Context, identity flowidentity.RunScopedFlowInstance, intents []engine.EmitIntent) (engine.CommittedEngineMutation, error) {
	if pc == nil || pc.workflowStore == nil || pc.workflowStore.engineMutations == nil {
		return engine.CommittedEngineMutation{}, fmt.Errorf("outbox fixture requires selected workflow persistence")
	}
	instance, found, err := pc.workflowStore.Load(ctx, identity)
	if err != nil {
		return engine.CommittedEngineMutation{}, err
	}
	if !found {
		return engine.CommittedEngineMutation{}, fmt.Errorf("outbox fixture requires a constructed workflow instance")
	}
	address := engine.StateAddress{
		FlowInstance: identity, FlowID: identitypkg.NormalizeFlowID(instance.WorkflowName), EntityID: identitypkg.NormalizeEntityID(instance.EntityID),
	}
	dependencies := coordinatorEngineDependencies(pc)
	state, found, err := dependencies.StateRepo.LoadState(ctx, address)
	if err != nil || !found {
		return engine.CommittedEngineMutation{}, fmt.Errorf("load evaluated outbox fixture state: found=%v: %w", found, err)
	}
	return dependencies.MutationOwner.CommitEngineMutation(ctx, engine.EngineMutation{
		Address: address, EvaluatedState: state,
		State: engine.StateMutation{StateCarrier: state.StateCarrier}, EmitIntents: intents,
	})
}
