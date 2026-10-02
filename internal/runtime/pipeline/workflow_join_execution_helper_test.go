package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func installedWorkflowJoinDeliveryOwnerForTest(t *testing.T, pc *PipelineCoordinator) *pipelineTestDeliveryOwner {
	t.Helper()
	owner, ok := pc.deliveryStore.(*pipelineTestDeliveryOwner)
	if !ok || owner == nil {
		t.Fatal("join test requires its delivery owner installed before execution")
	}
	return owner
}

func withClaimedWorkflowNodePublicationForTest(t *testing.T, pc *PipelineCoordinator, ctx context.Context, evt events.Event, route events.DeliveryRoute) context.Context {
	t.Helper()
	configurePipelineTestDeliveryOwner(t, pc)
	ctx, err := persistWorkflowJoinPublicationForTest(t, pc, ctx, evt, route, true)
	if err != nil {
		t.Fatal(err)
	}
	owner := installedWorkflowJoinDeliveryOwnerForTest(t, pc)
	id, err := runtimedelivery.DeliveryID(evt.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := owner.Snapshot(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := owner.ClaimDelivery(ctx, snapshot.Authority, evt, snapshot.Route)
	if err != nil {
		t.Fatal(err)
	}
	acquired, ok := claimed.Acquired()
	if !ok {
		t.Fatalf("component execution claim = %s, want acquired", claimed.Disposition)
	}
	if pc.workOwner == nil {
		pc.workOwner = pipelineTestWorkOwner(t)
	}
	ctx = runtimedelivery.WithClaim(ctx, acquired.Claim)
	heartbeat, err := runtimedelivery.StartClaimHeartbeatFromClaim(ctx, pc.workOwner, owner, acquired.Claim, claimed.Renewal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := heartbeat.Stop(); err != nil {
			t.Errorf("component claim heartbeat cleanup: %v", err)
		}
	})
	return heartbeat.Context()
}

// Resolve the compiled join occurrence, then execute through the retained handler.
func executeResolvedJoinForTest(t *testing.T, pc *PipelineCoordinator, ctx context.Context, evt events.Event, trigger workflowTriggerContext) (contractHandlerExecutionResult, error) {
	t.Helper()
	if pc == nil || pc.SemanticSource() == nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, nil
	}
	source := pc.SemanticSource()
	resolution, found, err := resolveWorkflowJoinOccurrence(source, evt)
	if err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
	}
	if !found {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, nil
	}
	if strings.TrimSpace(trigger.HandlerEventKey) == "" {
		trigger.HandlerEventKey = resolution.Ref.HandlerEvent()
	}
	flowID := resolution.Ref.FlowPath()
	if flowID == "" {
		flowID = semanticview.RootExecutionFlowID(source)
	}
	recipient, target, _, _, err := ResolveWorkflowJoinOccurrenceDeliveryTarget(source, evt)
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	ctx, err = persistWorkflowJoinPublicationForTest(t, pc, ctx, evt, events.DeliveryRoute{Recipient: recipient, Target: events.MustExistingEntityTarget(target)}, false)
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	return executeClaimedWorkflowJoinForTest(t, pc, withPipelineFlowScope(ctx, flowID), resolution.Ref.Node(), resolution.Handler, trigger)
}

// Ordinary direct executions explicitly publish first. Retries use the retained
// route read back from the delivery owner, never a newly selected activation.
func persistWorkflowJoinPublicationForTest(t *testing.T, pc *PipelineCoordinator, ctx context.Context, evt events.Event, route events.DeliveryRoute, ordinary bool) (context.Context, error) {
	t.Helper()
	owner := installedWorkflowJoinDeliveryOwnerForTest(t, pc)
	id, err := runtimedelivery.DeliveryID(evt.ID(), route)
	if err != nil {
		return ctx, err
	}
	var exists bool
	var retainedID string
	err = pc.workflowStore.testDB().QueryRowContext(ctx, `SELECT delivery_id FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' AND subscriber_id=$2`, evt.ID(), route.Recipient.ID()).Scan(&retainedID)
	if err == nil {
		id, exists = retainedID, true
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ctx, err
	}
	if !exists {
		if ordinary {
			target := route.Target.Route()
			instanceOwner, err := WorkflowJoinAdmissionOwner(pc.SemanticSource(), evt.RunID(), target)
			if err != nil {
				return ctx, err
			}
			unlock := pc.lockWorkflowEntity(target.EntityID)
			instance, found, err := pc.workflowStore.Load(ctx, instanceOwner)
			if err == nil {
				var observed *WorkflowInstance
				if found {
					observed = &instance
				}
				route.Context.Joins, _, err = PrepareWorkflowJoinAdmission(pc.SemanticSource(), evt.RunID(), string(evt.Type()), route, observed)
			}
			unlock()
			if err != nil {
				return ctx, err
			}
		}
		if err := pc.workflowStore.testDB().QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE event_id=$1)`, evt.ID()).Scan(&exists); err != nil {
			return ctx, err
		}
		if !exists {
			persistExactJoinEvent(t, pc.workflowStore, ctx, evt)
		}
		if err := owner.commitInitial(ctx, evt, route); err != nil {
			return ctx, err
		}
		id, err = runtimedelivery.DeliveryID(evt.ID(), route)
		if err != nil {
			return ctx, err
		}
	}
	snapshot, err := owner.Snapshot(ctx, id)
	if err != nil {
		return ctx, err
	}
	if snapshot.Route.Target != route.Target || snapshot.Route.Recipient != route.Recipient {
		return ctx, fmt.Errorf("retained test publication contradicts its exact recipient/target")
	}
	return withWorkflowNodeDeliveryRoute(ctx, snapshot.Route), nil
}

func executePublishedWorkflowJoinForTest(t *testing.T, pc *PipelineCoordinator, ctx context.Context, node identity.ExecutableNode, handler runtimecontracts.SystemNodeEventHandler, trigger workflowTriggerContext) (contractHandlerExecutionResult, error) {
	t.Helper()
	flowID := node.FlowPath()
	path := trigger.State.Control.FlowPath
	if flowID == semanticview.RootExecutionFlowID(pc.SemanticSource()) {
		path = trigger.Event.RunID()
	}
	if path == "" {
		path = trigger.Event.FlowInstance()
	}
	target := events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: trigger.State.EntityID}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)}
	ctx, err := persistWorkflowJoinPublicationForTest(t, pc, ctx, trigger.Event, route, !isJoinLifecycleEvent(trigger.Event.Type()))
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	result, err := executeClaimedWorkflowJoinForTest(t, pc, ctx, node, handler, trigger)
	if err != nil || handler.Join == nil || isJoinLifecycleEvent(trigger.Event.Type()) {
		return result, err
	}
	return drivePublishedJoinCompletionForTest(t, pc, ctx, node, handler, result)
}

func drivePublishedJoinCompletionForTest(t *testing.T, pc *PipelineCoordinator, ctx context.Context, node identity.ExecutableNode, handler runtimecontracts.SystemNodeEventHandler, result contractHandlerExecutionResult) (contractHandlerExecutionResult, error) {
	t.Helper()
	declaration, err := timeridentity.NewJoinRef(node, "item.completed", handler.Join.Stage, handler.Join.EffectiveID())
	if err != nil {
		return result, err
	}
	receipt, found := events.DeliveryContextFromContext(ctx).JoinAdmission(declaration)
	if !found || receipt.Disposition != events.JoinAdmissionBound {
		return result, fmt.Errorf("completion fixture requires its original bound publication receipt")
	}
	route, found := workflowNodeDeliveryRoute(ctx)
	if !found {
		return result, fmt.Errorf("completion fixture requires its retained route")
	}
	target := route.Target.Route()
	owner, err := WorkflowJoinAdmissionOwner(pc.SemanticSource(), receipt.Ref.StageEntry().RunID, target)
	if err != nil {
		return result, err
	}
	instance, found, err := pc.workflowStore.Load(ctx, owner)
	if err != nil || !found {
		return result, fmt.Errorf("completion fixture owner: found=%v err=%v", found, err)
	}
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return result, err
	}
	activation, found, err := joinruntime.Load(carrier.StateBuckets, node, joinruntime.ActivationKey(receipt.Ref))
	if err != nil || !found {
		return result, fmt.Errorf("completion fixture arm: found=%v err=%v", found, err)
	}
	if !activation.OutcomePending || activation.OutcomeFired || activation.Status != joinruntime.StatusClosed {
		return result, nil
	}
	upserts, _ := committedWorkflowSchedulesForTest(t, pc.workflowStore)
	for _, schedule := range upserts {
		if schedule.Command.TaskID != activation.TimerTaskID() {
			continue
		}
		envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, target.EntityID)
		if node.FlowPath() != semanticview.RootExecutionFlowID(pc.SemanticSource()) {
			envelope = handlerTestWorkflowEnvelope(target.FlowID, target.FlowInstance, target.EntityID)
		}
		control := workflowJoinScheduleEventForTest(t, schedule.Command.TaskID+":fixture-completion", schedule, owner.RunID, envelope, schedule.InitialDueAt)
		return executeResolvedJoinForTest(t, pc, ctx, control, workflowTriggerContext{Event: control, State: mustCurrentWorkflowState(t, pc, ctx, owner.Route, target.EntityID), HandlerEventKey: receipt.Ref.HandlerEvent()})
	}
	return result, fmt.Errorf("closed arm lacks its exact committed completion schedule")
}

func executeClaimedWorkflowJoinForTest(t *testing.T, pc *PipelineCoordinator, ctx context.Context, node identity.ExecutableNode, handler runtimecontracts.SystemNodeEventHandler, trigger workflowTriggerContext) (result contractHandlerExecutionResult, resultErr error) {
	t.Helper()
	route, found := workflowNodeDeliveryRoute(ctx)
	if !found {
		return result, fmt.Errorf("join test requires its explicit persisted publication")
	}
	owner := installedWorkflowJoinDeliveryOwnerForTest(t, pc)
	id, err := runtimedelivery.DeliveryID(trigger.Event.ID(), route)
	if err != nil {
		return result, err
	}
	snapshot, err := owner.Snapshot(ctx, id)
	if err != nil {
		return result, err
	}
	claimed, err := owner.ClaimDelivery(ctx, snapshot.Authority, trigger.Event, snapshot.Route)
	if err != nil {
		return result, err
	}
	acquired, ok := claimed.Acquired()
	if !ok {
		if claimed.Disposition == runtimedelivery.ClaimTerminal {
			return contractHandlerExecutionResult{Handled: true, Outcome: &handlerExecutionOutcome{Status: HandlerOutcomeDiscarded}}, nil
		}
		return result, fmt.Errorf("join test delivery not acquired: %s", claimed.Disposition)
	}
	ctx = runtimedelivery.WithClaim(ctx, acquired.Claim)
	heartbeat, err := runtimedelivery.StartClaimHeartbeatFromClaim(ctx, pc.workOwner, owner, acquired.Claim, claimed.Renewal)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, heartbeat.Stop()) }()
	return pc.executeNodeContractHandler(heartbeat.Context(), node, handler, trigger, false, true)
}

func initialWorkflowJoinRefForTest(t *testing.T, ctx context.Context, node identity.ExecutableNode, path, entityID, stage, joinID string) timeridentity.JoinRef {
	t.Helper()
	owner := testRunScopedWorkflowInstanceFromContext(ctx, path)
	route := owner.Route
	effect, err := workflowlifecycle.NewInitialEntry(route, identity.NormalizeEntityID(entityID), stage, executionmode.Live, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := effect.StageEntry(owner)
	if err != nil || !found {
		t.Fatalf("initial lifecycle entry: found=%v err=%v", found, err)
	}
	ref, err := timeridentity.NewJoinRef(node, "item.completed", stage, joinID)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindStageEntry(entry, timeridentity.JoinRef{}.Generation())
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func workflowJoinActivationKey(t *testing.T, buckets map[string]map[string]any, node identity.ExecutableNode) string {
	t.Helper()
	activations, err := joinruntime.List(buckets)
	if err != nil {
		t.Fatal(err)
	}
	var key string
	for _, activation := range activations {
		if activation.JoinRef().Node().Equal(node) && activation.Stage() == "awaiting" && activation.JoinID() == "awaiting" {
			if key != "" {
				t.Fatal("fixture requires one exact lifecycle-entry arm; multiple arms must use their retained JoinRef")
			}
			key = joinruntime.ActivationKey(activation.JoinRef())
		}
	}
	if key == "" {
		t.Fatal("fixture is missing its bound lifecycle-entry arm")
	}
	return key
}

func mustJoinRefForTest(t *testing.T, handle timeridentity.TimerHandle) timeridentity.JoinRef {
	t.Helper()
	ref, found := handle.JoinRef()
	if !found {
		t.Fatal("fixture requires a bound join timer handle")
	}
	return ref
}

func assertJoinCompletionCancellationsForTest(t *testing.T, cancellations []runtimegenericschedule.Activation, ref timeridentity.JoinRef) {
	t.Helper()
	deadline, err := timeridentity.JoinTimeoutHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	complete, err := timeridentity.JoinCompleteHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{deadline.TaskID(): true, complete.TaskID(): true}
	if len(cancellations) != len(want) {
		t.Fatalf("completion cancellations=%d, want exact deadline and completion obligations", len(cancellations))
	}
	for _, cancellation := range cancellations {
		_, actual, found := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, cancellation)))
		if !found || !actual.Equal(ref) || !want[cancellation.Command.TaskID] {
			t.Fatalf("completion cancelled a duplicate or unrelated arm: %s", cancellation.Command.TaskID)
		}
		delete(want, cancellation.Command.TaskID)
	}
}

func admitJoinTransitionOccurrenceForTest(t *testing.T, pc *PipelineCoordinator, ctx context.Context, evt events.Event, transition *workflowlifecycle.Transition) string {
	t.Helper()
	node, _, found := transition.HandlerOrigin()
	if !found {
		t.Fatal("direct transition fixture requires its compiled handler owner")
	}
	path := evt.FlowInstance()
	if node.FlowPath() == semanticview.RootExecutionFlowID(pc.SemanticSource()) {
		path = evt.RunID()
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: node.FlowPath(), FlowInstance: path, EntityID: evt.EntityID()})}
	ctx, err := persistWorkflowJoinPublicationForTest(t, pc, ctx, evt, route, false)
	if err != nil {
		t.Fatal(err)
	}
	owner := installedWorkflowJoinDeliveryOwnerForTest(t, pc)
	id, err := runtimedelivery.DeliveryID(evt.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := owner.Snapshot(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := owner.ClaimDelivery(ctx, snapshot.Authority, evt, snapshot.Route)
	if err != nil {
		t.Fatal(err)
	}
	acquired, ok := claimed.Acquired()
	if !ok {
		t.Fatalf("direct transition occurrence not admitted: %s", claimed.Disposition)
	}
	return acquired.Claim.DeliveryID()
}

func persistAdmittedJoinTransitionForTest(t *testing.T, pc *PipelineCoordinator, ctx context.Context, route runtimeflowidentity.Route, entityID, nextState, eventType string) error {
	t.Helper()
	inbound, found := runtimecorrelation.InboundEventFromContext(ctx)
	if !found {
		return fmt.Errorf("join transition fixture requires its persisted causal event")
	}
	instance, found, err := pc.workflowStore.Load(ctx, testRunScopedWorkflowRoute(ctx, route))
	if err != nil || !found {
		return fmt.Errorf("join transition owner: found=%v err=%v", found, err)
	}
	current := instance.CurrentState
	transition, err := compiledLifecycleTransitionForTest(pc, instance.WorkflowName, current, nextState, eventType)
	if err != nil {
		return err
	}
	effect, err := workflowlifecycle.NewAcceptedEvent(route, identity.NormalizeEntityID(entityID), inbound.ID(), string(inbound.Type()), inbound.ExecutionMode(), inbound.CreatedAt(), transition)
	if err != nil {
		return err
	}
	if transition != nil {
		effect, err = effect.WithExecutionOccurrence("delivery", admitJoinTransitionOccurrenceForTest(t, pc, ctx, inbound, transition))
		if err != nil {
			return err
		}
		instance.TransitionHistory = append(instance.TransitionHistory, WorkflowTransitionRecord{
			Evidence: *transition, TransitionID: transition.ID(), From: transition.From(), To: transition.To(), TriggerEventID: inbound.ID(), FiredAt: inbound.CreatedAt(), GuardsEvaluated: transition.GuardsEvaluated(),
		})
	}
	instance.CurrentState, instance.EnteredStageAt = nextState, inbound.CreatedAt()
	return commitTestWorkflowLifecycleMutation(ctx, pc, route, instance, current, []workflowlifecycle.Effect{effect})
}
