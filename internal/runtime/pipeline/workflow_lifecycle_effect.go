package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

type pipelineWorkflowLifecycleOwner struct {
	coordinator *PipelineCoordinator
}

func (o pipelineWorkflowLifecycleOwner) PrepareWorkflowLifecycleMutation(ctx context.Context, instance *WorkflowInstance, effects []runtimeworkflowlifecycle.Effect, reconcileGenerations bool) (PreparedWorkflowLifecycleMutation, error) {
	if o.coordinator == nil {
		return PreparedWorkflowLifecycleMutation{}, fmt.Errorf("workflow lifecycle owner is unavailable")
	}
	return o.coordinator.prepareWorkflowLifecycleMutation(ctx, instance, effects, reconcileGenerations)
}

func (o pipelineWorkflowLifecycleOwner) FinalizeWorkflowLifecycleMutation(ctx context.Context, committed CommittedWorkflowLifecycleMutation) error {
	if o.coordinator == nil {
		return fmt.Errorf("workflow lifecycle owner is unavailable")
	}
	return o.coordinator.finalizeWorkflowLifecycleMutation(ctx, committed)
}

func (o pipelineWorkflowLifecycleOwner) AcceptedEventEffect(route runtimeflowidentity.Route, entityID identity.EntityID, event events.Event, fromState, toState string) (runtimeworkflowlifecycle.Effect, error) {
	pc := o.coordinator
	if pc == nil {
		return runtimeworkflowlifecycle.Effect{}, fmt.Errorf("workflow lifecycle owner is unavailable")
	}
	entityID = identity.NormalizeEntityID(entityID.String())
	if entityID.IsZero() {
		return runtimeworkflowlifecycle.Effect{}, fmt.Errorf("accepted workflow event requires entity identity")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return runtimeworkflowlifecycle.Effect{}, fmt.Errorf("accepted workflow event requires exact instance route")
	}
	fromState = strings.TrimSpace(fromState)
	toState = strings.TrimSpace(toState)
	var transition *runtimeworkflowlifecycle.Transition
	if toState != "" && toState != fromState {
		if fromState == "" {
			return runtimeworkflowlifecycle.Effect{}, fmt.Errorf("accepted workflow transition requires the persisted source state")
		}
		value, err := runtimeworkflowlifecycle.NewTransition(
			fromState,
			toState,
			workflowTransitionIdentity(pc.WorkflowDefinition(), fromState, toState, string(event.Type())),
		)
		if err != nil {
			return runtimeworkflowlifecycle.Effect{}, err
		}
		transition = &value
	}
	return runtimeworkflowlifecycle.NewAcceptedEvent(
		route,
		entityID,
		event.ID(),
		string(event.Type()),
		event.CreatedAt(),
		transition,
	)
}

func (o pipelineWorkflowLifecycleOwner) ApplyWorkflowLifecycleEffects(ctx context.Context, effects []runtimeworkflowlifecycle.Effect) error {
	pc := o.coordinator
	if pc == nil || len(effects) == 0 {
		return nil
	}
	if pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return nil
	}
	return fmt.Errorf("durable workflow lifecycle effects must be committed by the selected workflow engine mutation owner")
}

func (o pipelineWorkflowLifecycleOwner) ArmInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error {
	pc := o.coordinator
	if pc == nil || pc.workflowTimers == nil {
		return fmt.Errorf("workflow timer lifecycle owner is unavailable")
	}
	return pc.workflowTimers.ArmInitialEntryTimers(ctx, route)
}

func (o pipelineWorkflowLifecycleOwner) ReconcileInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error {
	pc := o.coordinator
	if pc == nil || pc.workflowTimers == nil {
		return fmt.Errorf("workflow timer lifecycle owner is unavailable")
	}
	return pc.workflowTimers.reconcileInitialEntryDeclarations(ctx, route)
}

func (o pipelineWorkflowLifecycleOwner) RetireInitialEntryTimerWakeups(ctx context.Context, route runtimeflowidentity.Route) error {
	pc := o.coordinator
	if pc == nil || pc.workflowTimers == nil {
		return fmt.Errorf("workflow timer lifecycle owner is unavailable")
	}
	return pc.workflowTimers.RetireInitialEntryTimerWakeups(ctx, route)
}
