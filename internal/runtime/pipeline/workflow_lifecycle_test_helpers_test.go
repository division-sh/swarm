package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func withLiveWorkflowInitialEntry(ctx context.Context) context.Context {
	return runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
}

func (pc *PipelineCoordinator) persistWorkflowStateForTest(ctx context.Context, route runtimeflowidentity.Route, entityID, nextState, sourceEvent string) error {
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok {
		return fmt.Errorf("test transition requires an inbound event")
	}
	instance, found, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return err
	}
	if !found {
		return &WorkflowInstanceLookupMiss{RequestedKey: route.InstancePath}
	}
	currentState := strings.TrimSpace(instance.CurrentState)
	instance.CurrentState = strings.TrimSpace(nextState)
	instance.EnteredStageAt = inbound.CreatedAt()
	instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(
		pc.WorkflowDefinition(), currentState, nextState, inbound.ID(), sourceEvent, inbound.CreatedAt(),
	))
	effect, err := (pipelineWorkflowLifecycleOwner{coordinator: pc}).AcceptedEventEffect(route, identity.NormalizeEntityID(entityID), inbound, currentState, nextState)
	if err != nil {
		return err
	}
	return commitTestWorkflowLifecycleMutation(ctx, pc, route, instance, currentState, []runtimeworkflowlifecycle.Effect{effect})
}

func (pc *PipelineCoordinator) applyWorkflowGateForTest(ctx context.Context, route runtimeflowidentity.Route, _ string, setGate string, clearAll bool) error {
	return pc.workflowStore.mutate(ctx, route, func(instance *WorkflowInstance) {
		gates := cloneWorkflowGates(instance.Gates)
		if clearAll {
			clear(gates)
		}
		if setGate = strings.TrimSpace(setGate); setGate != "" {
			gates[setGate] = true
		}
		instance.Gates = gates
	})
}

func fireWorkflowTimerTestWakeup(ctx context.Context, pc *PipelineCoordinator, activation WorkflowTimerActivation) (WorkflowTimerFireOutcome, error) {
	wakeup, err := newWorkflowTimerWakeup(activation)
	if err != nil {
		return "", err
	}
	return fireTypedWorkflowTimerTestWakeup(ctx, pc, wakeup)
}

func fireTypedWorkflowTimerTestWakeup(ctx context.Context, pc *PipelineCoordinator, wakeup WorkflowTimerWakeup) (WorkflowTimerFireOutcome, error) {
	outcome, _, err := pc.workflowTimers.fireWakeup(ctx, wakeup)
	return outcome, err
}

func applyTestInitialEntryEffect(ctx context.Context, pc *PipelineCoordinator, route runtimeflowidentity.Route, entityID string) error {
	instance, found, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("test workflow instance %s is missing", entityID)
	}
	mode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok {
		mode = runtimeeffects.ExecutionModeLive
	}
	effect, err := runtimeworkflowlifecycle.NewInitialEntry(testWorkflowInstanceRoute(instance.StorageRef), identity.NormalizeEntityID(entityID), instance.CurrentState, mode, instance.EnteredStageAt)
	if err != nil {
		return err
	}
	return commitTestWorkflowLifecycleMutation(ctx, pc, route, instance, instance.CurrentState, []runtimeworkflowlifecycle.Effect{effect})
}

func (pc *PipelineCoordinator) applyWorkflowGateIntents(ctx context.Context, route runtimeflowidentity.Route, entityID, currentStage, nextStage, sourceEvent string, occurredAt time.Time) error {
	return applyTestAcceptedLifecycleEffect(ctx, pc, route, entityID, currentStage, nextStage, sourceEvent, occurredAt)
}

func (pc *PipelineCoordinator) reconcileClosedJoinSchedules(ctx context.Context, route runtimeflowidentity.Route, entityID string, _ runtimeengine.StateCarrier) error {
	instance, found, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return err
	}
	if !found {
		return &WorkflowInstanceLookupMiss{RequestedKey: route.InstancePath}
	}
	mode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok {
		mode = runtimeeffects.ExecutionModeLive
	}
	effect, err := runtimeworkflowlifecycle.NewAcceptedEvent(route, identity.NormalizeEntityID(entityID), uuid.NewString(), "test.join_reconcile", mode, time.Now().UTC(), nil)
	if err != nil {
		return err
	}
	return commitTestWorkflowLifecycleMutation(ctx, pc, route, instance, instance.CurrentState, []runtimeworkflowlifecycle.Effect{effect})
}

func applyTestAcceptedLifecycleEffect(ctx context.Context, pc *PipelineCoordinator, route runtimeflowidentity.Route, entityID, currentStage, nextStage, eventType string, occurredAt time.Time) error {
	instance, found, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return err
	}
	if !found {
		return &WorkflowInstanceLookupMiss{RequestedKey: route.InstancePath}
	}
	var effect runtimeworkflowlifecycle.Effect
	mode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok {
		mode = runtimeeffects.ExecutionModeLive
	}
	if strings.TrimSpace(currentStage) == "" {
		effect, err = runtimeworkflowlifecycle.NewInitialEntry(route, identity.NormalizeEntityID(entityID), nextStage, mode, occurredAt)
	} else {
		var transition runtimeworkflowlifecycle.Transition
		transition, err = runtimeworkflowlifecycle.NewTransition(currentStage, nextStage, "test_"+uuid.NewString())
		if err == nil {
			effect, err = runtimeworkflowlifecycle.NewAcceptedEvent(route, identity.NormalizeEntityID(entityID), uuid.NewString(), eventType, mode, occurredAt, &transition)
		}
	}
	if err != nil {
		return err
	}
	expectedState := instance.CurrentState
	instance.CurrentState = nextStage
	instance.EnteredStageAt = occurredAt.UTC()
	return commitTestWorkflowLifecycleMutation(ctx, pc, route, instance, expectedState, []runtimeworkflowlifecycle.Effect{effect})
}

func reconcileWorkflowTimerForTest(ctx context.Context, pc *PipelineCoordinator, route runtimeflowidentity.Route, entityID, currentStage, nextStage string, cause workflowTimerCause) error {
	instance, found, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return err
	}
	if !found {
		return &WorkflowInstanceLookupMiss{RequestedKey: route.InstancePath}
	}
	cause = cause.normalized()
	mode := cause.ExecutionMode
	if !mode.Valid() {
		mode = runtimeeffects.ExecutionModeLive
	}
	var effect runtimeworkflowlifecycle.Effect
	if cause.Kind == workflowTimerCauseInitial {
		effect, err = runtimeworkflowlifecycle.NewInitialEntry(route, identity.NormalizeEntityID(entityID), nextStage, mode, cause.OccurredAt)
	} else {
		var transition *runtimeworkflowlifecycle.Transition
		if strings.TrimSpace(currentStage) != "" && strings.TrimSpace(currentStage) != strings.TrimSpace(nextStage) {
			transitionID := firstNonEmptyString(cause.TransitionID, "test_"+uuid.NewString())
			value, transitionErr := runtimeworkflowlifecycle.NewTransition(currentStage, nextStage, transitionID)
			if transitionErr != nil {
				return transitionErr
			}
			transition = &value
		}
		effect, err = runtimeworkflowlifecycle.NewAcceptedEvent(
			route,
			identity.NormalizeEntityID(entityID),
			firstNonEmptyString(cause.EventID, uuid.NewString()),
			firstNonEmptyString(cause.EventType, "test.timer_event"),
			mode,
			cause.OccurredAt,
			transition,
		)
	}
	if err != nil {
		return err
	}
	expectedState := instance.CurrentState
	if strings.TrimSpace(nextStage) != "" {
		instance.CurrentState = strings.TrimSpace(nextStage)
	}
	return commitTestWorkflowLifecycleMutation(ctx, pc, route, instance, expectedState, []runtimeworkflowlifecycle.Effect{effect})
}

func commitTestWorkflowLifecycleMutation(
	ctx context.Context,
	pc *PipelineCoordinator,
	route runtimeflowidentity.Route,
	instance WorkflowInstance,
	expectedState string,
	effects []runtimeworkflowlifecycle.Effect,
) error {
	if pc == nil || pc.workflowStore == nil || pc.workflowStore.engineMutations == nil {
		return fmt.Errorf("test workflow lifecycle requires the selected workflow engine mutation owner")
	}
	prepared, err := pc.prepareWorkflowLifecycleMutation(ctx, &instance, effects, len(effects) > 0)
	if err != nil {
		return err
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	updatedAt := time.Now().UTC()
	if updatedAt.Before(instance.CreatedAt) {
		updatedAt = instance.CreatedAt
	}
	state, err := workflowEngineStateRecord(runID, route, instance, expectedState, instance.Revision, WorkflowEngineStateTransitionUpdateStateAndCompanion, updatedAt)
	if err != nil {
		return err
	}
	var publications []runtimeengine.DurablePublicationPlan
	if len(prepared.Emissions) > 0 {
		planner, ok := pc.bus.(EnginePublicationPlanner)
		if !ok {
			return fmt.Errorf("test workflow lifecycle requires publication planner")
		}
		publications, err = planner.PrepareEnginePublications(ctx, prepared.Emissions)
		if err != nil {
			return err
		}
	}
	committed, err := pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{
		State: state, Lifecycle: prepared.Commit, Publications: publications,
	})
	if err != nil {
		if planner, ok := pc.bus.(EnginePublicationPlanner); ok {
			err = errors.Join(err, planner.ReleaseEnginePublications(context.WithoutCancel(ctx), publications))
		}
		return err
	}
	if planner, ok := pc.bus.(EnginePublicationPlanner); ok {
		if err := planner.FinalizeEnginePublications(ctx, committed.Publications); err != nil {
			return err
		}
	}
	if err := pc.finalizeWorkflowLifecycleMutation(ctx, committed.Lifecycle); err != nil {
		return err
	}
	if len(prepared.Emissions) > 0 {
		dispatcher := pc.bus.EngineDispatcher()
		if dispatcher == nil {
			return fmt.Errorf("test workflow lifecycle requires post-commit dispatcher")
		}
		return dispatcher.DispatchPostCommit(context.WithoutCancel(ctx), prepared.Emissions)
	}
	return nil
}
