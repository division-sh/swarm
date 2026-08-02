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
		entityID.String(),
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
	if _, ok := PipelineSQLTxFromContext(ctx); !ok {
		return fmt.Errorf("workflow lifecycle effects require the selected workflow mutation")
	}
	for _, effect := range effects {
		if err := pc.applyWorkflowLifecycleEffect(ctx, effect); err != nil {
			return err
		}
	}
	return nil
}

func (pc *PipelineCoordinator) applyWorkflowLifecycleEffect(ctx context.Context, effect runtimeworkflowlifecycle.Effect) error {
	entityID := strings.TrimSpace(effect.InstanceID())
	route := effect.Route()
	if entityID == "" || !route.Valid() || effect.OccurredAt().IsZero() {
		return fmt.Errorf("workflow lifecycle effect is incomplete")
	}
	fromState := ""
	toState := ""
	cause := workflowTimerCause{OccurredAt: effect.OccurredAt()}
	switch effect.Kind() {
	case runtimeworkflowlifecycle.KindInitialEntry:
		toState = strings.TrimSpace(effect.InitialStage())
		cause.Kind = workflowTimerCauseInitial
		cause.EventType = "state:" + toState
		cause.ToState = toState
	case runtimeworkflowlifecycle.KindAcceptedEvent:
		cause.Kind = workflowTimerCauseEvent
		cause.EventID = effect.EventID()
		cause.EventType = effect.EventType()
		if transition, ok := effect.Transition(); ok {
			fromState = transition.From()
			toState = transition.To()
			cause.Kind = workflowTimerCauseTransition
			cause.TransitionID = transition.ID()
			cause.FromState = fromState
			cause.ToState = toState
		}
	default:
		return fmt.Errorf("workflow lifecycle effect kind is unsupported")
	}
	if effect.Kind() == runtimeworkflowlifecycle.KindInitialEntry {
		if err := pc.workflowTimers.reconcileInitialEntry(ctx, route, entityID, toState, cause); err != nil {
			return err
		}
	} else if err := pc.workflowTimers.Reconcile(ctx, route, entityID, fromState, toState, cause); err != nil {
		return err
	}
	if err := pc.applyWorkflowJoinIntents(ctx, route, entityID, fromState, toState, effect.OccurredAt()); err != nil {
		return err
	}
	return pc.applyWorkflowGateIntents(ctx, route, entityID, fromState, toState, cause.EventType, effect.OccurredAt())
}

func (pc *PipelineCoordinator) applyAcceptedWorkflowEvent(ctx context.Context, route runtimeflowidentity.Route, entityID string, event events.Event, fromState, toState string) error {
	owner := pipelineWorkflowLifecycleOwner{coordinator: pc}
	effect, err := owner.AcceptedEventEffect(route, identity.NormalizeEntityID(entityID), event, fromState, toState)
	if err != nil {
		return err
	}
	return owner.ApplyWorkflowLifecycleEffects(ctx, []runtimeworkflowlifecycle.Effect{effect})
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
