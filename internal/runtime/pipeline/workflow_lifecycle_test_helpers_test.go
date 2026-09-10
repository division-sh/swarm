package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func withLiveWorkflowInitialEntry(ctx context.Context) context.Context {
	return runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
}

func workflowLifecycleEventForTest(t *testing.T, store *workflowInstanceStore, ctx context.Context, flowID, instanceID, entityID, eventType string, at time.Time) events.Event {
	t.Helper()
	mode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok {
		mode = runtimeeffects.ExecutionModeLive
	}
	inbound := eventtest.RunCreatingRootIngressWithMode(uuid.NewString(), events.EventType(eventType), "operator", "", []byte(`{}`), 0,
		runtimecorrelation.RunIDFromContext(ctx), "", handlerTestWorkflowEnvelope(flowID, instanceID, entityID), at, mode)
	dialect := authoractivityfixture.DialectPostgres
	if store.isSQLite() {
		dialect = authoractivityfixture.DialectSQLite
	}
	seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, inbound)
	return inbound
}

func lifecycleStateFixtureForTest(t *testing.T, flowID, from, to string, eventTypes ...string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	prefix := ""
	files := map[string]string{"schema.yaml": "name: lifecycle-state-fixture\n", "entities.yaml": "test_entity: {}\n"}
	if flowID != "." {
		prefix = flowID + "/"
	}
	files[prefix+"schema.yaml"] = fmt.Sprintf("name: lifecycle-state-fixture\nstages:\n  %s: {initial: true}\n  %s: {}\n", from, to)
	files[prefix+"entities.yaml"] = "test_entity: {}\n"
	nodes := "lifecycle-owner:\n  id: lifecycle-owner\n  execution_type: system_node\n  event_handlers:\n"
	for _, eventType := range eventTypes {
		files[prefix+"events.yaml"] += eventType + ": {}\n"
		nodes += fmt.Sprintf("    %s: {advances_to: %s}\n", eventType, to)
	}
	files[prefix+"nodes.yaml"] = nodes
	return loadWorkflowTempBundle(t, files)
}

func lifecycleTransitionRecordFixtureForTest(t *testing.T, flowID, from, to, eventID string, at time.Time) WorkflowTransitionRecord {
	t.Helper()
	bundle := lifecycleStateFixtureForTest(t, flowID, from, to, "lifecycle.transitioned")
	pc := &PipelineCoordinator{module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}}
	transition, err := compiledLifecycleTransitionForTest(pc, flowID, from, to, "lifecycle.transitioned")
	if err != nil {
		t.Fatal(err)
	}
	return WorkflowTransitionRecord{Evidence: *transition, TransitionID: transition.ID(), From: from, To: to, TriggerEventID: eventID, FiredAt: at}
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
	transition, err := compiledLifecycleTransitionForTest(pc, instance.WorkflowName, currentState, nextState, sourceEvent)
	if err != nil {
		return err
	}
	instance.CurrentState = strings.TrimSpace(nextState)
	instance.EnteredStageAt = inbound.CreatedAt()
	if transition != nil {
		instance.TransitionHistory = append(instance.TransitionHistory, WorkflowTransitionRecord{
			Evidence: *transition, TransitionID: transition.ID(), From: transition.From(), To: transition.To(),
			TriggerEventID: inbound.ID(), FiredAt: inbound.CreatedAt(), GuardsEvaluated: transition.GuardsEvaluated(),
		})
	}
	effect, err := (pipelineWorkflowLifecycleOwner{coordinator: pc}).AcceptedEventEffect(route, identity.NormalizeEntityID(entityID), inbound, currentState, nextState, transition)
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
		inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
		if !ok {
			return fmt.Errorf("test accepted lifecycle effect requires a caller inbound event")
		}
		if string(inbound.Type()) != eventType || !inbound.CreatedAt().Equal(occurredAt) {
			return fmt.Errorf("test lifecycle event type/time contradicts caller inbound event")
		}
		var transition *runtimeworkflowlifecycle.Transition
		transition, err = compiledLifecycleTransitionForTest(pc, instance.WorkflowName, currentStage, nextStage, eventType)
		if err == nil {
			effect, err = runtimeworkflowlifecycle.NewAcceptedEvent(route, identity.NormalizeEntityID(entityID), inbound.ID(), string(inbound.Type()), mode, inbound.CreatedAt(), transition)
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
		inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
		if !ok {
			return fmt.Errorf("test timer reconciliation requires a caller inbound event")
		}
		if cause.EventID != inbound.ID() || cause.EventType != string(inbound.Type()) || !cause.OccurredAt.Equal(inbound.CreatedAt()) {
			return fmt.Errorf("test timer cause contradicts caller inbound event")
		}
		var transition *runtimeworkflowlifecycle.Transition
		if strings.TrimSpace(currentStage) != "" && strings.TrimSpace(currentStage) != strings.TrimSpace(nextStage) {
			value, transitionErr := compiledLifecycleTransitionForTest(pc, instance.WorkflowName, currentStage, nextStage, cause.EventType)
			if transitionErr != nil {
				return transitionErr
			}
			transition = value
		}
		effect, err = runtimeworkflowlifecycle.NewAcceptedEvent(
			route,
			identity.NormalizeEntityID(entityID),
			inbound.ID(),
			string(inbound.Type()),
			mode,
			inbound.CreatedAt(),
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

// Direct lifecycle tests must name a unique authored carrier in the selected flow.
// Missing fixture declarations are errors, never permission to invent an edge.
func compiledLifecycleTransitionForTest(pc *PipelineCoordinator, flowID, from, to, eventType string) (*runtimeworkflowlifecycle.Transition, error) {
	if from == to {
		return nil, nil
	}
	graph, ok := semanticview.WorkflowStageTopology(pc.SemanticSource(), flowID)
	if !ok {
		return nil, fmt.Errorf("test lifecycle requires compiled topology for flow %q", flowID)
	}
	var selected *runtimecontracts.WorkflowStageTopologyEdge
	for _, edge := range graph.Edges {
		if edge.From != from || edge.To != to || edge.EventType != eventType {
			continue
		}
		if selected != nil {
			return nil, fmt.Errorf("test lifecycle event %q has ambiguous compiled carriers for %s -> %s", eventType, from, to)
		}
		value := edge
		selected = &value
	}
	if selected == nil {
		return nil, fmt.Errorf("test lifecycle has no compiled carrier for flow %s event %s: %s -> %s", graph.FlowID, eventType, from, to)
	}
	compiled, err := graph.AdmitTransition(selected.Site(), from, to)
	if err != nil {
		return nil, err
	}
	fact := handlerselection.NotApplicable()
	if selected.RuleRef.Valid() {
		contexts := map[runtimecontracts.HandlerAdvanceCarrierKind]handlerselection.Context{
			runtimecontracts.HandlerAdvanceCarrierRules:          handlerselection.ContextRules,
			runtimecontracts.HandlerAdvanceCarrierOnComplete:     handlerselection.ContextOnComplete,
			runtimecontracts.HandlerAdvanceCarrierJoinOnComplete: handlerselection.ContextJoinComplete,
			runtimecontracts.HandlerAdvanceCarrierJoinTimeout:    handlerselection.ContextJoinTimeout,
		}
		fact, err = handlerselection.Selected(contexts[selected.AdvanceCarrier], selected.RuleRef, "")
		if err != nil {
			return nil, err
		}
	}
	transition, err := runtimeworkflowlifecycle.NewCompiledTransition(compiled, fact, nil)
	return &transition, err
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
