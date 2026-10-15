package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
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
	state, err := workflowEngineStateRecord(identity, instance, instance.CurrentState, instance.Revision, WorkflowEngineStateTransitionUpdateStateAndCompanion, time.Now().UTC())
	if err != nil {
		return engine.CommittedEngineMutation{}, err
	}
	planner, ok := pc.bus.(EnginePublicationPlanner)
	if !ok {
		return engine.CommittedEngineMutation{}, fmt.Errorf("outbox fixture requires the native publication planner")
	}
	publications, err := planner.PrepareEnginePublications(ctx, intents)
	if err != nil {
		return engine.CommittedEngineMutation{}, err
	}
	owner := pipelineEngineMutationOwner{
		store: pc.workflowStore, state: pipelineEngineStateRepo{coordinator: pc}, publication: planner,
	}
	return owner.commitPreparedEngineMutation(ctx, engine.EngineMutation{EmitIntents: intents}, WorkflowEngineMutationCommand{State: state, Publications: publications}, publications)
}
