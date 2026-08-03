package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
)

func (pc *PipelineCoordinator) prepareWorkflowTermination(
	ctx context.Context,
	instance *WorkflowInstance,
	reason string,
	terminatedAt time.Time,
) (PreparedWorkflowLifecycleMutation, error) {
	var prepared PreparedWorkflowLifecycleMutation
	if pc == nil || instance == nil {
		return prepared, fmt.Errorf("workflow termination requires coordinator and instance")
	}
	if terminatedAt.IsZero() {
		return prepared, fmt.Errorf("workflow termination requires exact occurrence time")
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		return prepared, err
	}
	activations, err := gateruntime.List(carrier.StateBuckets)
	if err != nil {
		return prepared, err
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	identity := StoredFlowInstance(pc.SemanticSource(), *instance)
	if runID == "" || strings.TrimSpace(identity.EntityID) == "" {
		return prepared, fmt.Errorf("workflow termination requires exact run and persisted entity identity")
	}
	for _, activation := range activations {
		if activation.Status == gateruntime.StatusDecisionCommitted {
			return PreparedWorkflowLifecycleMutation{}, fmt.Errorf("flow cannot terminate while decision card %s has a committed verdict awaiting its frozen route", activation.CardID)
		}
		if !activation.Supersede(reason, terminatedAt.UTC()) {
			continue
		}
		if pc.decisionCards == nil {
			return PreparedWorkflowLifecycleMutation{}, fmt.Errorf("workflow decision-card lifecycle owner is required")
		}
		card, err := pc.decisionCards.GetDecisionCard(ctx, activation.CardID)
		if err != nil {
			return PreparedWorkflowLifecycleMutation{}, fmt.Errorf("load terminated flow decision card: %w", err)
		}
		if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
			return PreparedWorkflowLifecycleMutation{}, err
		}
		evt, err := workflowGateSupersededEvent(card, activation, *instance, terminatedAt)
		if err != nil {
			return PreparedWorkflowLifecycleMutation{}, err
		}
		prepared.Commit.GateCards = append(prepared.Commit.GateCards, WorkflowGateCardMutation{
			Kind:         WorkflowGateCardMutationSupersede,
			Card:         card,
			EntityID:     identity.EntityID,
			ActivationID: activation.ActivationID,
			Reason:       activation.SupersededReason,
			OccurredAt:   terminatedAt.UTC(),
		})
		prepared.Commit.RequestCompletionCandidate = true
		prepared.Emissions = append(prepared.Emissions, runtimeengine.EmitIntent{Event: evt})
	}
	instance.StateBuckets = carrier.PersistedStateBuckets()
	return prepared, nil
}

func (pc *PipelineCoordinator) commitWorkflowTermination(
	ctx context.Context,
	route runtimeflowidentity.Route,
	terminatedAt time.Time,
) (WorkflowInstance, error) {
	if pc == nil || pc.workflowStore == nil || pc.workflowStore.engineMutations == nil {
		return WorkflowInstance{}, fmt.Errorf("workflow termination requires the selected workflow engine mutation owner")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() || terminatedAt.IsZero() {
		return WorkflowInstance{}, fmt.Errorf("workflow termination requires exact route and occurrence time")
	}
	instance, found, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return WorkflowInstance{}, err
	}
	if !found {
		return WorkflowInstance{}, &WorkflowInstanceLookupMiss{RequestedKey: route.InstancePath}
	}
	if strings.TrimSpace(instance.Status) == "terminated" {
		if instance.TerminatedAt.IsZero() {
			return WorkflowInstance{}, fmt.Errorf("terminal workflow instance %s has no termination time", route.InstancePath)
		}
		return instance, nil
	}
	expectedState := strings.TrimSpace(instance.CurrentState)
	expectedRevision := instance.Revision
	prepared, err := pc.prepareWorkflowTermination(ctx, &instance, "flow_terminated", terminatedAt.UTC())
	if err != nil {
		return WorkflowInstance{}, err
	}
	instance.Status = "terminated"
	instance.TerminatedAt = terminatedAt.UTC()
	updatedAt := terminatedAt.UTC()
	if updatedAt.Before(instance.CreatedAt) {
		return WorkflowInstance{}, fmt.Errorf("workflow termination time cannot precede creation time")
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	state, err := workflowEngineStateRecord(runID, route, instance, expectedState, expectedRevision, false, updatedAt)
	if err != nil {
		return WorkflowInstance{}, err
	}
	var publications []runtimeengine.DurablePublicationPlan
	if len(prepared.Emissions) > 0 {
		planner, ok := pc.bus.(EnginePublicationPlanner)
		if !ok {
			return WorkflowInstance{}, fmt.Errorf("workflow termination requires the publication planner")
		}
		publications, err = planner.PrepareEnginePublications(ctx, prepared.Emissions)
		if err != nil {
			return WorkflowInstance{}, err
		}
		if len(publications) != len(prepared.Emissions) {
			releaseErr := planner.ReleaseEnginePublications(context.WithoutCancel(ctx), publications)
			return WorkflowInstance{}, errors.Join(fmt.Errorf("workflow termination planner returned %d plans for %d emissions", len(publications), len(prepared.Emissions)), releaseErr)
		}
	}
	committed, err := pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{
		State: state, Lifecycle: prepared.Commit, Publications: publications,
	})
	if err != nil {
		if planner, ok := pc.bus.(EnginePublicationPlanner); ok {
			err = errors.Join(err, planner.ReleaseEnginePublications(context.WithoutCancel(ctx), publications))
		}
		return WorkflowInstance{}, err
	}
	if planner, ok := pc.bus.(EnginePublicationPlanner); ok {
		if err := planner.FinalizeEnginePublications(ctx, committed.Publications); err != nil {
			return WorkflowInstance{}, err
		}
	}
	if err := pc.finalizeWorkflowLifecycleMutation(ctx, committed.Lifecycle); err != nil {
		return WorkflowInstance{}, err
	}
	if len(prepared.Emissions) > 0 {
		dispatcher := pc.bus.EngineDispatcher()
		if dispatcher == nil {
			return WorkflowInstance{}, fmt.Errorf("workflow termination requires the post-commit dispatcher")
		}
		if err := dispatcher.DispatchPostCommit(context.WithoutCancel(ctx), prepared.Emissions); err != nil {
			return WorkflowInstance{}, err
		}
	}
	persisted, found, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return WorkflowInstance{}, err
	}
	if !found || strings.TrimSpace(persisted.Status) != "terminated" || persisted.TerminatedAt.IsZero() {
		return WorkflowInstance{}, fmt.Errorf("canonical terminal flow instance %s was not persisted", route.InstancePath)
	}
	return persisted, nil
}
