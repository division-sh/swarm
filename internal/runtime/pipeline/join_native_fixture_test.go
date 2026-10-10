package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

// Only acknowledged results from the original selected mutation owner count.
// No transaction, schedule activation or acknowledgment is implemented here.
type nativePipelineLifecycleMutationObservationForTest struct {
	WorkflowEngineMutationOwner
	mu                         sync.Mutex
	activations, cancellations []genericschedule.Activation
	cancellationRequests       []WorkflowScheduleMutation
}

func (o *nativePipelineLifecycleMutationObservationForTest) CommitWorkflowEngineMutation(ctx context.Context, command WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	result, err := o.WorkflowEngineMutationOwner.CommitWorkflowEngineMutation(ctx, command)
	if result.Committed {
		o.mu.Lock()
		o.activations = append(o.activations, result.Lifecycle.GenericScheduleActivations...)
		o.cancellations = append(o.cancellations, result.Lifecycle.GenericScheduleCancellations...)
		for _, mutation := range command.Lifecycle.Schedules {
			if mutation.Kind == WorkflowScheduleMutationCancel {
				o.cancellationRequests = append(o.cancellationRequests, mutation)
			}
		}
		o.mu.Unlock()
	}
	return result, err
}

func nativeWorkflowJoinEventForTest(ctx context.Context, flow, path, entity, name string, payload []byte, at time.Time) events.Event {
	flow = pipelineDeclarationFlowPath(flow)
	mode, found := effects.ExecutionModeFromContext(ctx)
	if !found {
		mode = executionmode.Live
	}
	target := events.RouteIdentity{FlowID: flow, FlowInstance: path, EntityID: entity}
	return eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), events.EventType(name), "operator", "", payload, 0,
		correlation.RunIDFromContext(ctx), events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), testWorkflowRoutingSource(flow, path, entity), at, mode)
}

func dispatchNativeWorkflowJoinEventForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, event events.Event, nodeID string) {
	t.Helper()
	route := publishNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, event, nodeID)
	if handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), event); err != nil || !handled {
		t.Fatalf("native join event execution: handled=%t error=%v", handled, err)
	}
}

func publishNativeWorkflowJoinEventForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, event events.Event, nodeID string) events.DeliveryRoute {
	t.Helper()
	target := event.Envelope().Target.Normalized()
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineSourceNode(t, pc.SemanticSource(), target.FlowID, nodeID)), Target: events.MustExistingEntityTarget(target)}
	owner, err := WorkflowJoinAdmissionOwner(pc.SemanticSource(), event.RunID(), target)
	if err != nil {
		t.Fatal(err)
	}
	instance, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, owner)
	if err != nil || !found {
		t.Fatalf("native join target missing: %t/%v", found, err)
	}
	route.Context.Joins, _, err = PrepareWorkflowJoinAdmission(pc.SemanticSource(), event.RunID(), string(event.Type()), route, &instance)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.PublishNode(ctx, event, route); err != nil {
		t.Fatal(err)
	}
	return route
}

func (o *nativePipelineLifecycleMutationObservationForTest) schedules() ([]genericschedule.Activation, []genericschedule.Activation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]genericschedule.Activation(nil), o.activations...), append([]genericschedule.Activation(nil), o.cancellations...)
}

func (o *nativePipelineLifecycleMutationObservationForTest) requestedCancellations() []WorkflowScheduleMutation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]WorkflowScheduleMutation(nil), o.cancellationRequests...)
}

func nativeWorkflowJoinCoordinatorForTest(t *testing.T, backend string, bundle *runtimecontracts.WorkflowContractBundle, schedules GenericScheduleWakeupOwner, open pipelineDeliveryNativeOpenerForTest) (*PipelineDeliveryNativeFixtureForTest, *PipelineCoordinator, context.Context, *nativePipelineLifecycleMutationObservationForTest) {
	t.Helper()
	source := semanticview.Wrap(bundle)
	fixture := open(t, backend, source)
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatal(err)
	}
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules})
	ctx := correlation.WithRunID(fixture.Context, uuid.NewString())
	if err := fixture.RequireRun(ctx, correlation.RunIDFromContext(ctx)); err != nil {
		t.Fatal(err)
	}
	observer := &nativePipelineLifecycleMutationObservationForTest{WorkflowEngineMutationOwner: pc.workflowStore.engineMutations}
	pc.workflowStore.engineMutations = observer
	return fixture, pc, ctx, observer
}

func nativeWorkflowJoinSourceCoordinatorForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, source semanticview.Source, schedules GenericScheduleWakeupOwner, observer *nativePipelineLifecycleMutationObservationForTest) *PipelineCoordinator {
	t.Helper()
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: source, workflowNodes: nodes, guardRegistry: NewContractGuardRegistry(source)}, GenericSchedules: schedules})
	observer.WorkflowEngineMutationOwner = pc.workflowStore.engineMutations
	pc.workflowStore.engineMutations = observer
	return pc
}

func nativeWorkflowJoinPublicationContextForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, event events.Event, route events.DeliveryRoute, ordinary bool) (context.Context, error) {
	t.Helper()
	// Replays retain the exact committed route, including its original join binding.
	count, err := fixture.DeliveryCount(ctx, event.ID(), route.Recipient.ID())
	if err != nil {
		return ctx, err
	}
	if count == 0 {
		if ordinary {
			owner, err := WorkflowJoinAdmissionOwner(pc.SemanticSource(), event.RunID(), route.Target.Route())
			if err != nil {
				return ctx, err
			}
			instance, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, owner)
			if err != nil || !found {
				return ctx, fmt.Errorf("native join publication target: found=%t error=%v", found, err)
			}
			route.Context.Joins, _, err = PrepareWorkflowJoinAdmission(pc.SemanticSource(), event.RunID(), string(event.Type()), route, &instance)
			if err != nil {
				return ctx, err
			}
		}
		if err := fixture.PublishNode(ctx, event, route); err != nil {
			return ctx, err
		}
	}
	snapshot, err := fixture.NodeDeliverySnapshot(ctx, event.RunID(), event.ID(), route.Recipient.ID())
	if err != nil {
		return ctx, err
	}
	if snapshot.Route.Target != route.Target || snapshot.Route.Recipient != route.Recipient {
		return ctx, fmt.Errorf("native join replay contradicts its committed recipient/target")
	}
	if count != 0 {
		// A durable route is a receipt, not a live successor continuation. Replay
		// the original publication owner to transfer its actual retained handoff.
		if err := fixture.PublishNode(ctx, event, snapshot.Route); err != nil {
			return ctx, err
		}
	}
	return withWorkflowNodeDeliveryRoute(ctx, snapshot.Route), nil
}

func executeNativeResolvedJoinForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, event events.Event, trigger workflowTriggerContext) (contractHandlerExecutionResult, error) {
	t.Helper()
	resolution, found, err := resolveWorkflowJoinOccurrence(pc.SemanticSource(), event)
	if err != nil || !found {
		return contractHandlerExecutionResult{}, err
	}
	if strings.TrimSpace(trigger.HandlerEventKey) == "" {
		trigger.HandlerEventKey = resolution.Ref.HandlerEvent()
	}
	recipient, target, _, _, err := ResolveWorkflowJoinOccurrenceDeliveryTarget(pc.SemanticSource(), event)
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	ctx, err = nativeWorkflowJoinPublicationContextForTest(t, fixture, pc, ctx, event, events.DeliveryRoute{Recipient: recipient, Target: events.MustExistingEntityTarget(target)}, false)
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	return executeNativeClaimedPipelineHandlerForTest(t, pc, withPipelineFlowScope(ctx, resolution.Ref.FlowPath()), resolution.Ref.Node(), resolution.Handler, trigger)
}

func executeNativePublishedWorkflowJoinForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, mutations *nativePipelineLifecycleMutationObservationForTest, pc *PipelineCoordinator, ctx context.Context, node identity.ExecutableNode, handler runtimecontracts.SystemNodeEventHandler, trigger workflowTriggerContext) (contractHandlerExecutionResult, error) {
	t.Helper()
	flow, path := node.FlowPath(), trigger.State.Control.FlowPath
	if flow == semanticview.RootExecutionFlowID(pc.SemanticSource()) {
		path = trigger.Event.RunID()
	}
	if path == "" {
		path = trigger.Event.FlowInstance()
	}
	target := events.RouteIdentity{FlowID: flow, FlowInstance: path, EntityID: trigger.State.EntityID}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)}
	ctx, err := nativeWorkflowJoinPublicationContextForTest(t, fixture, pc, ctx, trigger.Event, route, !isJoinLifecycleEvent(trigger.Event.Type()))
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, ctx, node, handler, trigger)
	if err != nil || handler.Join == nil || isJoinLifecycleEvent(trigger.Event.Type()) {
		return result, err
	}
	return driveNativePublishedJoinCompletionForTest(t, fixture, mutations, pc, ctx, node, handler, result)
}

func driveNativePublishedJoinCompletionForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, mutations *nativePipelineLifecycleMutationObservationForTest, pc *PipelineCoordinator, ctx context.Context, node identity.ExecutableNode, handler runtimecontracts.SystemNodeEventHandler, result contractHandlerExecutionResult) (contractHandlerExecutionResult, error) {
	t.Helper()
	route, found := workflowNodeDeliveryRoute(ctx)
	if !found {
		return result, fmt.Errorf("native completion requires its original published route")
	}
	target := route.Target.Route()

	declaration, err := timeridentity.NewJoinRef(node, "item.completed", handler.Join.Stage, handler.Join.EffectiveID())
	if err != nil {
		return result, err
	}
	receipt, found := events.DeliveryContextFromContext(ctx).JoinAdmission(declaration)
	if !found || receipt.Disposition != events.JoinAdmissionBound {
		return result, fmt.Errorf("native join completion requires the original publication binding")
	}
	owner, err := WorkflowJoinAdmissionOwner(pc.SemanticSource(), receipt.Ref.StageEntry().RunID, target)
	if err != nil {
		return result, err
	}
	instance, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, owner)
	if err != nil || !found {
		return result, fmt.Errorf("native join completion owner: found=%t error=%v", found, err)
	}
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return result, err
	}
	activation, found, err := joinruntime.Load(carrier.StateBuckets, node, joinruntime.ActivationKey(receipt.Ref))
	if err != nil || !found {
		return result, fmt.Errorf("native join completion arm: found=%t error=%v", found, err)
	}
	if !activation.OutcomePending || activation.OutcomeFired || activation.Status != joinruntime.StatusClosed {
		return result, nil
	}
	upserts, _ := mutations.schedules()
	for _, schedule := range upserts {
		if schedule.Command.TaskID != activation.TimerTaskID() {
			continue
		}
		envelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, target)
		control := workflowJoinScheduleEventForTest(t, schedule.Command.TaskID+":fixture-completion", schedule, owner.RunID, envelope, schedule.InitialDueAt)
		return executeNativeResolvedJoinForTest(t, fixture, pc, ctx, control, workflowTriggerContext{Event: control, State: mustCurrentWorkflowState(t, pc, ctx, owner.Route, target.EntityID), HandlerEventKey: receipt.Ref.HandlerEvent()})
	}
	return result, fmt.Errorf("closed native join lacks its committed completion schedule")

}
