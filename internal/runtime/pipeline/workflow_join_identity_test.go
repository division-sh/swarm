package pipeline

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

type exactWorkflowJoinSource struct {
	semanticview.Source
	plans             []runtimecontracts.WorkflowJoinPlan
	nodeFlowID        string
	overrideNodeOwner bool
}

func (s exactWorkflowJoinSource) WorkflowJoins() []runtimecontracts.WorkflowJoinPlan {
	return append([]runtimecontracts.WorkflowJoinPlan(nil), s.plans...)
}

func (s exactWorkflowJoinSource) ExecutableNodeSource(node runtimeidentity.ExecutableNode) (runtimecontracts.ContractItemSource, bool) {
	if s.overrideNodeOwner && node.FlowPath() == pipelineDeclarationFlowPath(s.nodeFlowID) && (node.NodeID() == "join-node" || node.NodeID() == "dispatcher" || node.NodeID() == "observer") {
		return runtimecontracts.ContractItemSource{FlowPath: node.FlowPath(), Family: "nodes"}, true
	}
	return s.Source.ExecutableNodeSource(node)
}

func exactJoinRoutingSource(flowID, path, entityID string) events.RoutingSource {
	if flowID == "" {
		return eventtest.RootRoutingSource(path)
	}
	return eventtest.ConcreteTemplateRoutingSource(flowID, path, entityID)
}

func exactJoinSemanticStateEqual(left, right WorkflowInstance) bool {
	return left.CurrentState == right.CurrentState &&
		reflect.DeepEqual(left.TransitionHistory, right.TransitionHistory) &&
		reflect.DeepEqual(left.StateBuckets, right.StateBuckets) &&
		reflect.DeepEqual(left.Fields, right.Fields) &&
		reflect.DeepEqual(left.Bookkeeping, right.Bookkeeping) &&
		reflect.DeepEqual(left.Gates, right.Gates)
}

type exactJoinScope struct {
	declarationFlowID string
	executionFlowID   string
	path              string
	route             runtimeflowidentity.Route
	entityID          string
}

func exactRootAndFlowJoinSource(bundle *runtimecontracts.WorkflowContractBundle) exactWorkflowJoinSource {
	rootPlan := exactCompiledJoinPlanForTest(bundle, ".")
	flowPlan := exactCompiledJoinPlanForTest(bundle, "orders")
	return exactWorkflowJoinSource{
		Source: workflowJoinLifecycleRootAndFlowSource(bundle),
		plans:  []runtimecontracts.WorkflowJoinPlan{rootPlan, flowPlan},
	}
}

func exactCompiledJoinPlanForTest(bundle *runtimecontracts.WorkflowContractBundle, flowID string) runtimecontracts.WorkflowJoinPlan {
	for _, plan := range bundle.Semantics.Joins {
		if plan.Node.Equal(mustPipelineNode(flowID, "join-node")) && plan.HandlerEvent == "item.completed" {
			return plan
		}
	}
	panic("fixture has no exact compiled join plan")
}

func workflowJoinLifecycleRootAndFlowSource(bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	return semanticview.Wrap(bundle)
}

func seedExactJoinScope(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context, source semanticview.Source, declarationFlowID, path string) exactJoinScope {
	t.Helper()
	executionFlowID := declarationFlowID
	if executionFlowID == "" {
		executionFlowID = semanticview.RootExecutionFlowID(source)
	}
	route := testRunScopedWorkflowInstanceFromContext(ctx, path).Route
	entityID := FlowInstanceEntityID(path)
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, source, ctx, WorkflowInstance{
		InstanceID: route.InstanceID, StorageRef: path, WorkflowName: executionFlowID, WorkflowVersion: source.WorkflowVersion(),
		EntityID: entityID, CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(),
		Fields:     map[string]any{"expected": []any{"a", "b"}, "instance_key": strings.TrimPrefix(path, declarationFlowID+"/")},
		EntityType: "test_entity",
	})); err != nil {
		t.Fatal(err)
	}
	return exactJoinScope{declarationFlowID: declarationFlowID, executionFlowID: executionFlowID, path: path, route: route, entityID: entityID}
}

func exactJoinActivationForScope(t *testing.T, pc *PipelineCoordinator, store *workflowInstanceStore, ctx context.Context, scope exactJoinScope) joinruntime.Activation {
	t.Helper()
	instance, found, err := store.Load(ctx, testRunScopedWorkflowRoute(ctx, scope.route))
	if err != nil || !found {
		t.Fatalf("load exact join scope %s: found=%v err=%v", scope.path, found, err)
	}
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		t.Fatal(err)
	}
	activations, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	if len(activations) != 1 {
		t.Fatalf("join activations for %s = %#v", scope.path, activations)
	}
	return activations[0]
}

func deliverExactJoinMember(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, mutations *nativePipelineLifecycleMutationObservationForTest, pc *PipelineCoordinator, ctx context.Context, source semanticview.Source, scope exactJoinScope, member string) error {
	t.Helper()
	event := nativeWorkflowJoinEventForTest(ctx, scope.declarationFlowID, scope.path, scope.entityID, "item.completed", mustJSON(map[string]any{"member_id": member, "result": map[string]any{"value": member}}), time.Now().UTC())
	joinNode := pipelineNode(t, scope.declarationFlowID, "join-node")
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(joinNode), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: scope.executionFlowID, FlowInstance: scope.path, EntityID: scope.entityID})}
	deliveryCtx, err := nativeWorkflowJoinPublicationContextForTest(t, fixture, pc, ctx, event, route, true)
	if err != nil {
		return err
	}
	handler := source.ExecutableNodeEventHandlers(joinNode)["item.completed"]
	result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, deliveryCtx, joinNode, handler, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, pc, ctx, scope.route, scope.entityID), HandlerEventKey: "item.completed"})
	if err != nil {
		return err
	}
	_, err = driveNativePublishedJoinCompletionForTest(t, fixture, mutations, pc, deliveryCtx, joinNode, handler, result)
	return err
}

func TestJoinScheduleFactsAreDerivedOnlyFromTypedDeclarationHandle(t *testing.T) {
	source := workflowJoinLifecycleSource(workflowJoinLifecycleBundle(t))
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	for _, tc := range []struct {
		name             string
		flowID           string
		wantKind         events.RoutingSourceKind
		wantFlowInstance string
	}{
		{name: "explicit root", flowID: "", wantKind: events.RoutingSourceRoot},
		{name: "flow owned", flowID: "orders", wantKind: events.RoutingSourceFlowOwnedControl, wantFlowInstance: "orders/order-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.wantFlowInstance
			entityID := runtimeflowidentity.EntityID(path)
			if path == "" {
				path = runID
				entityID = runtimeflowidentity.EntityID(runID)
			}
			instanceRoute := testRunScopedWorkflowInstanceForRun(runID, path).Route
			handle := pipelineJoinHandle(t, tc.flowID, timeridentity.TimerHandleJoinTimeout, runID, path, entityID)
			ref, _ := handle.JoinRef()
			activation, err := joinruntime.NewActivation(ref, []string{"a"}, nil, now, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			instance := WorkflowInstance{
				WorkflowName: ref.FlowPath(), StorageRef: path, InstanceID: instanceRoute.InstanceID, EntityID: entityID,
			}
			if tc.flowID != "" {
				instance = materializedWorkflowInstanceForSource(t, source, runtimecorrelation.WithRunID(context.Background(), runID), instance)
			}
			command, err := joinSchedule(source, runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: instanceRoute}, instance, activation, executionmode.Live)
			if err != nil {
				t.Fatal(err)
			}
			parsed, ref, ok := timeridentity.ParseJoinHandle(parsePayloadMap(mustJSON(command.Payload.Interface())))
			if !ok || !ref.Equal(activation.JoinRef()) || parsed.TaskID() != command.TaskID || command.ScheduleKey != command.TaskID || command.EventType != parsed.EventType() {
				t.Fatalf("derived command disagrees with handle: command=%#v ref=%#v ok=%v", command, ref, ok)
			}
			if command.RoutingSource.Kind() != tc.wantKind || command.FlowInstance != tc.wantFlowInstance || command.RoutingSource.Route().FlowID != tc.flowID {
				t.Fatalf("derived route = kind:%q flow:%q instance:%q", command.RoutingSource.Kind(), command.RoutingSource.Route().FlowID, command.FlowInstance)
			}
		})
	}
}

func TestJoinLifecycleHandlerResolutionRequiresExactDeclarationRef(t *testing.T) {
	bundle := workflowJoinLifecycleBundle(t)
	base := workflowJoinLifecycleRootAndFlowSource(bundle)
	plan := bundle.Semantics.Joins[0]
	rootPlan := plan
	rootPlan.Node = mustPipelineNode("", "join-node")
	flowPlan := plan
	flowPlan.Node = mustPipelineNode("orders", "join-node")
	source := exactWorkflowJoinSource{Source: base, plans: []runtimecontracts.WorkflowJoinPlan{rootPlan, flowPlan}}
	entityID := uuid.NewString()
	flowInstance := "orders/order-1"
	runID := uuid.NewString()
	rootHandle := pipelineJoinHandle(t, "", timeridentity.TimerHandleJoinTimeout, runID, runID, entityID)
	flowHandle := pipelineJoinHandle(t, "orders", timeridentity.TimerHandleJoinTimeout, runID, flowInstance, entityID)
	otherFlowHandle := pipelineJoinHandle(t, "returns", timeridentity.TimerHandleJoinTimeout, runID, "returns/order-1", entityID)
	if rootHandle.TaskID() == flowHandle.TaskID() {
		t.Fatal("root and flow-owned same-leaf declarations share a task identity")
	}
	rootSource, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	flowSource, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "orders", FlowInstance: flowInstance, EntityID: entityID})
	if err != nil {
		t.Fatal(err)
	}
	otherFlowInstance := "returns/order-1"
	otherFlowSource, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "returns", FlowInstance: otherFlowInstance, EntityID: entityID})
	if err != nil {
		t.Fatal(err)
	}
	otherFlowEnvelope := events.EnvelopeForSourceRoute(
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), otherFlowInstance),
		events.RouteIdentity{FlowID: "returns", FlowInstance: otherFlowInstance, EntityID: entityID},
	)
	rootEvent := exactJoinOccurrenceEvent(t, "root", rootHandle, rootSource, events.EventEnvelope{EntityID: entityID})
	flowEvent := exactJoinOccurrenceEvent(t, "flow", flowHandle, flowSource, workflowJoinTestEnvelope(flowInstance, entityID))
	for _, tc := range []struct {
		name   string
		event  events.Event
		flowID string
	}{
		{name: "root", event: rootEvent, flowID: ""},
		{name: "flow", event: flowEvent, flowID: "orders"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantNode := pipelineNode(t, tc.flowID, "join-node")
			resolution, ok, err := resolveWorkflowJoinOccurrence(source, tc.event)
			if err != nil || !ok || resolution.Ref.FlowPath() != pipelineDeclarationFlowPath(tc.flowID) || !resolution.Plan.Node.Equal(wantNode) {
				t.Fatalf("resolution = %#v ok=%v err=%v", resolution, ok, err)
			}
			recipient, target, handler, ok, err := ResolveWorkflowJoinOccurrenceDeliveryTarget(source, tc.event)
			recipientNode, recipientIsNode := recipient.Node()
			if err != nil || !ok || !recipientIsNode || !recipientNode.Equal(wantNode) || !handler.Node().Equal(wantNode) || handler.Empty() {
				t.Fatalf("delivery target = recipient:%#v target:%#v handler:%#v ok=%v err=%v", recipient, target, handler, ok, err)
			}
			wantExecutionFlow := tc.flowID
			wantInstance := flowInstance
			if tc.flowID == "" {
				wantExecutionFlow = semanticview.RootExecutionFlowID(source)
				wantInstance = tc.event.RunID()
			}
			if target.FlowID != wantExecutionFlow || target.FlowInstance != wantInstance || target.EntityID != entityID || handler.ExecutionFlowID(source) != wantExecutionFlow {
				t.Fatalf("delivery target = %#v handler=%#v, want flow=%q instance=%q entity=%q", target, handler, wantExecutionFlow, wantInstance, entityID)
			}
			owner := events.MustExistingEntityTarget(target)
			if err := ValidateStampedDeliveryTargetOwnership(source, tc.event, recipient, handler, resolution.Handler, owner); err != nil {
				t.Fatalf("valid stamped join occurrence: %v", err)
			}
			wrongEntity := target
			wrongEntity.EntityID = eventtest.UUID("wrong-stamped-join-entity")
			wrongTask := exactJoinOccurrenceEventWithFacts(t, "wrong-stamped-task", string(tc.event.Type()), "runtime.generic_schedule", resolution.Handle.TaskID()+"-hostile", resolution.Handle, tc.event.RoutingSource(), tc.event.NormalizedEnvelope())
			for _, bad := range []struct {
				name    string
				event   events.Event
				handler DeliveryTargetHandler
				owner   events.DeliveryTargetOwnership
			}{
				{"wrong task", wrongTask, handler, owner},
				{"wrong entity", tc.event, handler, events.MustExistingEntityTarget(wrongEntity)},
				{"wrong handler event", tc.event, handler.ForEvent("unrelated.event"), owner},
			} {
				t.Run(bad.name, func(t *testing.T) {
					if err := ValidateStampedDeliveryTargetOwnership(source, bad.event, recipient, bad.handler, resolution.Handler, bad.owner); err == nil {
						t.Fatalf("stamped join accepted %s", bad.name)
					}
				})
			}
		})
	}

	for _, hostile := range []struct {
		name  string
		event events.Event
	}{
		{name: "root handle on flow route", event: exactJoinOccurrenceEvent(t, "root-on-flow", rootHandle, flowSource, workflowJoinTestEnvelope(flowInstance, entityID))},
		{name: "flow handle on root route", event: exactJoinOccurrenceEvent(t, "flow-on-root", flowHandle, rootSource, events.EventEnvelope{EntityID: entityID})},
		{name: "wrong producer", event: exactJoinOccurrenceEventWithFacts(t, "wrong-producer", joinTimeoutEvent, "runtime", rootHandle.TaskID(), rootHandle, rootSource, events.EventEnvelope{EntityID: entityID})},
		{name: "wrong task", event: exactJoinOccurrenceEventWithFacts(t, "wrong-task", joinTimeoutEvent, "runtime.generic_schedule", rootHandle.TaskID()+"-hostile", rootHandle, rootSource, events.EventEnvelope{EntityID: entityID})},
		{name: "wrong event kind", event: exactJoinOccurrenceEventWithFacts(t, "wrong-kind", joinCompleteEvent, "runtime.generic_schedule", rootHandle.TaskID(), rootHandle, rootSource, events.EventEnvelope{EntityID: entityID})},
		{name: "same leaf unrelated flow", event: exactJoinOccurrenceEvent(t, "other-flow", otherFlowHandle, otherFlowSource, otherFlowEnvelope)},
	} {
		t.Run(hostile.name, func(t *testing.T) {
			if resolution, ok, err := resolveWorkflowJoinOccurrence(source, hostile.event); err == nil || ok {
				t.Fatalf("hostile occurrence resolved: %#v ok=%v err=%v", resolution, ok, err)
			}
			if recipient, target, handler, ok, err := ResolveWorkflowJoinOccurrenceDeliveryTarget(source, hostile.event); err == nil || ok || !recipient.Empty() || !target.Empty() || !handler.Empty() {
				t.Fatalf("hostile delivery target resolved: recipient=%#v target=%#v handler=%#v ok=%v err=%v", recipient, target, handler, ok, err)
			}
		})
	}
}

func TestWorkflowJoinDeclarationRefUsesExactExecutionScope(t *testing.T) {
	bundle := workflowJoinLifecycleBundle(t)
	plan := bundle.Semantics.Joins[0]
	rootPlan := plan
	rootPlan.Node = mustPipelineNode("", "join-node")
	flowPlan := plan
	flowPlan.Node = mustPipelineNode("orders", "join-node")
	source := exactWorkflowJoinSource{
		Source: workflowJoinLifecycleRootAndFlowSource(bundle),
		plans:  []runtimecontracts.WorkflowJoinPlan{rootPlan, flowPlan},
	}
	handler := source.ExecutableNodeEventHandlers(mustPipelineNode(".", "join-node"))["item.completed"]

	rootRef, err := workflowJoinDeclarationRef(source, pipelineNode(t, "", "join-node"), "item.completed", handler)
	if err != nil || rootRef.FlowPath() != "." {
		t.Fatalf("root declaration = %#v err=%v", rootRef, err)
	}
	flowRef, err := workflowJoinDeclarationRef(source, pipelineNode(t, "orders", "join-node"), "item.completed", handler)
	if err != nil || flowRef.FlowPath() != "orders" {
		t.Fatalf("flow declaration = %#v err=%v", flowRef, err)
	}
	if rootRef.Equal(flowRef) {
		t.Fatal("same-leaf root and flow declarations collapsed to one identity")
	}
	if _, err := workflowJoinDeclarationRef(source, pipelineNode(t, "returns", "join-node"), "item.completed", handler); err == nil {
		t.Fatal("unrelated flow scope selected a same-leaf join declaration")
	}
}

func VerifyNativeRootAndFlowWorkflowJoinArrivalCompletionCancelsExactScheduleOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, scope := range []struct {
			name   string
			flowID string
		}{
			{name: "root", flowID: ""},
			{name: "flow", flowID: "orders"},
		} {
			t.Run(storeCase.name+"/"+scope.name, func(t *testing.T) {
				h := newNativeExactWorkflowJoinHarness(t, storeCase.name, scope.flowID, "dispatching", []any{"a", "b"}, workflowJoinLifecycleBundleStartingAt(t, "dispatching"), open)
				h.transition("awaiting", "dispatch.completed")
				store, ctx, pc, source := h.store, h.ctx, h.pc, h.source
				path, route, entityID := h.path, h.route, h.entityID
				upserts, _ := h.mutations.schedules()
				if len(upserts) != 1 {
					t.Fatalf("armed schedules = %#v", upserts)
				}
				_, armedRef, ok := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, upserts[0])))
				if !ok || armedRef.FlowPath() != pipelineDeclarationFlowPath(scope.flowID) || upserts[0].Command.RoutingSource.Route().FlowID != scope.flowID {
					t.Fatalf("armed declaration = ref:%#v command:%#v ok=%v", armedRef, upserts[0].Command, ok)
				}
				armedInstance, found, err := store.Load(ctx, testRunScopedWorkflowRoute(ctx, route))
				if err != nil || !found {
					t.Fatalf("load armed instance: found=%v err=%v", found, err)
				}
				armedCarrier, err := workflowInstanceStateCarrier(armedInstance)
				if err != nil {
					t.Fatal(err)
				}
				joinNode := pipelineNode(t, scope.flowID, "join-node")
				armedActivation, found, err := joinruntime.Load(armedCarrier.StateBuckets, joinNode, joinruntime.ActivationKey(armedRef))
				if err != nil || !found || !armedActivation.JoinRef().Equal(armedRef) {
					t.Fatalf("armed activation = found:%v activation:%#v ref:%#v err:%v", found, armedActivation, armedRef, err)
				}

				handler := source.ExecutableNodeEventHandlers(joinNode)["item.completed"]
				deliver := func(coordinator *PipelineCoordinator, id, member string) error {
					event := nativeWorkflowJoinEventForTest(ctx, scope.flowID, path, entityID, "item.completed", mustJSON(map[string]any{"member_id": member, "result": map[string]any{"value": member}}), time.Now().UTC())
					_, err := executeNativePublishedWorkflowJoinForTest(t, h.fixture, h.mutations, coordinator, ctx, joinNode, handler, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, coordinator, ctx, route, entityID), HandlerEventKey: "item.completed"})
					return err
				}
				if err := deliver(pc, "member-a", "a"); err != nil {
					t.Fatal(err)
				}
				h.restart()
				store, ctx, pc = h.store, h.ctx, h.pc
				if err := deliver(pc, "member-b", "b"); err != nil {
					t.Fatal(err)
				}
				instance, found, err := store.Load(ctx, testRunScopedWorkflowRoute(ctx, route))
				if err != nil || !found {
					t.Fatalf("load closed instance: found=%v err=%v", found, err)
				}
				carrier, err := workflowInstanceStateCarrier(instance)
				if err != nil {
					t.Fatal(err)
				}
				activation, found, err := joinruntime.Load(carrier.StateBuckets, joinNode, joinruntime.ActivationKey(armedRef))
				if err != nil || !found || activation.Status != joinruntime.StatusClosed || activation.FlowPath() != pipelineDeclarationFlowPath(scope.flowID) || activation.Completed() != 2 || !activation.TimerCancelled {
					t.Fatalf("closed activation = found:%v activation:%#v err:%v", found, activation, err)
				}
				_, cancellations := h.mutations.schedules()
				assertJoinCompletionCancellationsForTest(t, cancellations, armedRef)
				if err := deliver(pc, "member-b-duplicate", "b"); err == nil {
					t.Fatal("stale duplicate mutated a closed join")
				}
			})
		}
	}
}

func VerifyNativeSiblingFlowJoinDeclarationsStayIndependentAcrossRestartOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, order := range []struct {
			name    string
			reverse bool
		}{{name: "a_then_b"}, {name: "b_then_a", reverse: true}} {
			t.Run(storeCase.name+"/"+order.name, func(t *testing.T) {
				files := workflowJoinLifecycleFixtureFiles(false, "")
				for _, name := range []string{"schema.yaml", "entities.yaml", "events.yaml", "types.yaml", "nodes.yaml"} {
					for _, flowID := range []string{"a", "b"} {
						files[flowID+"/"+name] = strings.Replace(files["orders/"+name], "name: orders", "name: "+flowID, 1)
					}
					delete(files, "orders/"+name)
				}
				bundle := loadWorkflowTempBundle(t, files)
				plans := []runtimecontracts.WorkflowJoinPlan{exactCompiledJoinPlanForTest(bundle, "a"), exactCompiledJoinPlanForTest(bundle, "b")}
				if order.reverse {
					plans[0], plans[1] = plans[1], plans[0]
					children := bundle.FlowTree.Root.Children
					children[0], children[1] = children[1], children[0]
					for i := range children {
						bundle.FlowTree.ByID[children[i].Path] = &children[i]
					}
				}
				source := exactWorkflowJoinSource{Source: semanticview.Wrap(bundle), plans: plans}
				schedules := &recordingGenericScheduleWakeupOwner{}
				fixture := open(t, storeCase.name, source)
				ctx := runtimecorrelation.WithRunID(fixture.Context, uuid.NewString())
				if err := fixture.RequireRun(ctx, runtimecorrelation.RunIDFromContext(ctx)); err != nil {
					t.Fatal(err)
				}
				mutations := &nativePipelineLifecycleMutationObservationForTest{}
				pc := nativeWorkflowJoinSourceCoordinatorForTest(t, fixture, source, schedules, mutations)
				store := pc.workflowStore
				firstScope := seedExactJoinScope(t, fixture, ctx, source, "a", "a/"+uuid.NewString())
				secondScope := seedExactJoinScope(t, fixture, ctx, source, "b", "b/"+uuid.NewString())
				for _, scope := range []exactJoinScope{firstScope, secondScope} {
					if err := applyTestInitialEntryEffect(ctx, pc, scope.route, scope.entityID); err != nil {
						t.Fatalf("arm sibling flow join %s: %v", scope.declarationFlowID, err)
					}
				}
				upserts, _ := mutations.schedules()
				if len(upserts) != 2 || upserts[0].Command.TaskID == upserts[1].Command.TaskID {
					t.Fatalf("sibling flow join schedules = %#v, want two exact declarations", upserts)
				}
				if firstActivation, secondActivation := exactJoinActivationForScope(t, pc, store, ctx, firstScope), exactJoinActivationForScope(t, pc, store, ctx, secondScope); firstActivation.Completed() != 0 || secondActivation.Completed() != 0 {
					t.Fatalf("armed sibling flow activations = first:%#v second:%#v", firstActivation, secondActivation)
				}
				if err := deliverExactJoinMember(t, fixture, mutations, pc, ctx, source, firstScope, "a"); err != nil {
					t.Fatal(err)
				}
				if firstActivation, secondActivation := exactJoinActivationForScope(t, pc, store, ctx, firstScope), exactJoinActivationForScope(t, pc, store, ctx, secondScope); firstActivation.Completed() != 1 || secondActivation.Completed() != 0 {
					t.Fatalf("first sibling flow arrival crossed declarations: first:%#v second:%#v", firstActivation, secondActivation)
				}
				fixture = fixture.ReopenExecution()
				ctx = runtimecorrelation.WithRunID(fixture.Context, runtimecorrelation.RunIDFromContext(ctx))
				pc = nativeWorkflowJoinSourceCoordinatorForTest(t, fixture, source, schedules, mutations)
				store = pc.workflowStore
				if err := deliverExactJoinMember(t, fixture, mutations, pc, ctx, source, secondScope, "a"); err != nil {
					t.Fatal(err)
				}
				if firstActivation, secondActivation := exactJoinActivationForScope(t, pc, store, ctx, firstScope), exactJoinActivationForScope(t, pc, store, ctx, secondScope); firstActivation.Completed() != 1 || secondActivation.Completed() != 1 {
					t.Fatalf("restarted sibling flow arrivals = first:%#v second:%#v", firstActivation, secondActivation)
				}
			})
		}
	}
}

func VerifyNativeRootAndFlowWorkflowJoinStageExitCancelsExactScheduleOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, flowID := range []string{"", "orders"} {
			name := "root"
			if flowID != "" {
				name = "flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				h := newNativeExactWorkflowJoinHarness(t, storeCase.name, flowID, "awaiting", []any{"a", "b"}, nil, open)
				schedule := h.armInitial()
				h.transition("dispatching", "manual.abort")
				activation := h.activation()
				if activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonStageExit ||
					!activation.TimerCancelled || activation.FlowPath() != pipelineDeclarationFlowPath(flowID) {
					t.Fatalf("stage-exit activation = %#v", activation)
				}
				_, cancellations := h.mutations.schedules()
				if len(cancellations) != 1 || cancellations[0].Command.ScheduleKey != schedule.Command.ScheduleKey {
					t.Fatalf("stage-exit cancellations = %#v, want %q", cancellations, schedule.Command.ScheduleKey)
				}

				beforeLate := h.instance()
				h.restart()
				late := h.scheduleEvent(schedule, "late-after-stage-exit")
				if _, err := h.fire(late); err != nil {
					t.Fatalf("late cancelled occurrence: %v", err)
				}
				afterLate := h.instance()
				if !exactJoinSemanticStateEqual(beforeLate, afterLate) {
					t.Fatalf("late cancelled occurrence mutated workflow\nbefore=%#v\nafter=%#v", beforeLate, afterLate)
				}
				_, afterCancellations := h.mutations.schedules()
				if len(afterCancellations) != 1 {
					t.Fatalf("late occurrence emitted extra cancellations: %#v", afterCancellations)
				}
			})
		}
	}
}

func VerifyNativeRootAndFlowWorkflowJoinImmediateCompletionFiresExactHandleAfterRestartOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, flowID := range []string{"", "orders"} {
			name := "root"
			if flowID != "" {
				name = "flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				h := newNativeExactWorkflowJoinHarness(t, storeCase.name, flowID, "awaiting", nil, nil, open)
				schedule := h.armInitial()
				if schedule.Command.EventType != joinCompleteEvent {
					t.Fatalf("immediate schedule = %#v", schedule)
				}
				activation := h.activation()
				if activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonComplete ||
					!activation.OutcomePending || activation.FlowPath() != pipelineDeclarationFlowPath(flowID) {
					t.Fatalf("immediate activation = %#v", activation)
				}

				h.restart()
				event := h.scheduleEvent(schedule, "immediate-completion")
				result, err := h.fire(event)
				if err != nil || !result.Handled {
					t.Fatalf("immediate completion = handled:%v err:%v", result.Handled, err)
				}
				beforeDuplicate := h.instance()
				if _, err := h.fire(event); err != nil {
					t.Fatalf("duplicate completion: %v", err)
				}
				afterDuplicate := h.instance()
				if !exactJoinSemanticStateEqual(beforeDuplicate, afterDuplicate) {
					t.Fatalf("duplicate completion mutated workflow\nbefore=%#v\nafter=%#v", beforeDuplicate, afterDuplicate)
				}
				activation = h.activation()
				if !activation.OutcomeFired || activation.OutcomePending || !activation.TimerCancelled || activation.FlowPath() != pipelineDeclarationFlowPath(flowID) {
					t.Fatalf("fired immediate activation = %#v", activation)
				}
				if afterDuplicate.CurrentState != "ready" {
					t.Fatalf("immediate completion state = %q", afterDuplicate.CurrentState)
				}
				_, cancellations := h.mutations.schedules()
				if len(cancellations) != 1 || cancellations[0].Command.ScheduleKey != schedule.Command.ScheduleKey {
					t.Fatalf("immediate completion cancellations = %#v", cancellations)
				}
			})
		}
	}
}

func VerifyNativeRootAndFlowWorkflowJoinTimeoutFiresExactHandleAfterRestartOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, flowID := range []string{"", "orders"} {
			name := "root"
			if flowID != "" {
				name = "flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				h := newNativeExactWorkflowJoinHarness(t, storeCase.name, flowID, "awaiting", []any{"a", "b"}, nil, open)
				schedule := h.armInitial()
				if schedule.Command.EventType != joinTimeoutEvent {
					t.Fatalf("timeout schedule = %#v", schedule)
				}
				h.restart()
				event := h.scheduleEvent(schedule, "join-timeout")
				result, err := h.fire(event)
				if err != nil || !result.Handled {
					t.Fatalf("timeout fire = handled:%v err:%v", result.Handled, err)
				}
				beforeDuplicate := h.instance()
				if _, err := h.fire(event); err != nil {
					t.Fatalf("duplicate timeout: %v", err)
				}
				afterDuplicate := h.instance()
				if !exactJoinSemanticStateEqual(beforeDuplicate, afterDuplicate) {
					t.Fatalf("duplicate timeout mutated workflow\nbefore=%#v\nafter=%#v", beforeDuplicate, afterDuplicate)
				}
				activation := h.activation()
				if activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonDeadline ||
					!activation.OutcomeFired || !activation.TimerCancelled || activation.FlowPath() != pipelineDeclarationFlowPath(flowID) {
					t.Fatalf("timed-out activation = %#v", activation)
				}
				if afterDuplicate.CurrentState != "attention" {
					t.Fatalf("timeout state = %q", afterDuplicate.CurrentState)
				}
				_, cancellations := h.mutations.schedules()
				if len(cancellations) != 1 || cancellations[0].Command.ScheduleKey != schedule.Command.ScheduleKey {
					t.Fatalf("timeout cancellations = %#v", cancellations)
				}
			})
		}
	}
}

func VerifyNativeRootAndFlowWorkflowJoinLoopSupersessionCancelsExactGenerationOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	runRootAndFlowWorkflowJoinLoopSupersession(t, open, nil)
}

// This uses real arm, repeat, cancellation and persistence owners before
// folding their mutation records, rather than constructing retained join JSON.
func VerifyNativeWorkflowJoinRetainedGenerationMutationRoundTripBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	runRootAndFlowWorkflowJoinLoopSupersession(t, open, func(t *testing.T, h *nativeExactWorkflowJoinHarness) {
		records, err := h.fixture.AccumulatorHistory(h.ctx, runtimecorrelation.RunIDFromContext(h.ctx), h.entityID)
		if err != nil {
			t.Fatal(err)
		}

		if len(records) == 0 {
			t.Fatal("ordinary join lifecycle wrote no accumulator history")
		}
		projection, err := mutationlog.ReconstructEntityStateProjection(records)
		if err != nil {
			t.Fatal(err)
		}
		carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, projection.Accumulator)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := joinruntime.List(carrier.StateBuckets)
		if err != nil {
			t.Fatal(err)
		}
		live, err := workflowInstanceStateCarrier(h.instance())
		if err != nil {
			t.Fatal(err)
		}
		want, err := joinruntime.List(live.StateBuckets)
		if err != nil || len(want) == 0 {
			t.Fatalf("ordinary retained joins: %#v %v", want, err)
		}
		if len(want) != 1 || len(want[0].Outputs) != 1 {
			t.Fatalf("retained join must contain the real member output: %#v", want)
		}
		if !reflect.DeepEqual(want, actual) {
			t.Fatalf("mutation fold lost real retained join identity/state: want=%#v got=%#v projection=%#v", want, actual, projection.Accumulator)
		}
	})
}

func runRootAndFlowWorkflowJoinLoopSupersession(t *testing.T, open pipelineDeliveryNativeOpenerForTest, verify func(*testing.T, *nativeExactWorkflowJoinHarness)) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, flowID := range []string{"", "orders"} {
			name := "root"
			if flowID != "" {
				name = "flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				bundle := workflowJoinLifecycleBundleWithOptions(t, false, "repeat")
				h := newNativeExactWorkflowJoinHarness(t, storeCase.name, flowID, "dispatching", []any{"a", "b"}, bundle, open)
				declarationFlowID := pipelineDeclarationFlowPath(flowID)
				loop := h.startLoop()
				createdAt := loop.StartedAt

				schedule := h.armedSchedule()
				_, firstRef, ok := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, schedule)))
				if !ok || !firstRef.Generation().Equal(loop.Generation()) || firstRef.FlowPath() != pipelineDeclarationFlowPath(flowID) {
					t.Fatalf("first generation handle = %#v ok=%v, loop=%#v", firstRef, ok, loop.Generation())
				}
				if verify != nil {
					handler := h.source.ExecutableNodeEventHandlers(mustPipelineNode(h.flowID, "join-node"))["item.completed"]
					if handler.Join == nil || handler.Loop == nil {
						t.Fatal("retained-member fixture requires the admitted join and loop handler")
					}
					arrival := eventtest.ExistingRunRootIngressWithRoutingSource(
						uuid.NewString(), events.EventType("item.completed"), "operator", "",
						mustJSON(map[string]any{"member_id": "a", "result": map[string]any{"value": "retained"}, "revision_id": loop.RevisionID}), 0,
						runtimecorrelation.RunIDFromContext(h.ctx), h.envelope(), exactJoinRoutingSource(h.flowID, h.path, h.entityID), time.Now().UTC(),
					)
					if _, err := executeNativePublishedWorkflowJoinForTest(t, h.fixture, h.mutations, h.pc, h.ctx, pipelineNode(t, h.flowID, "join-node"), handler, workflowTriggerContext{
						Event: arrival, State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID), HandlerEventKey: "item.completed",
					}); err != nil {
						t.Fatalf("record retained member before repeat: %v", err)
					}
				}

				eventID := uuid.NewString()
				payload := mustJSON(map[string]any{"revision_id": loop.Generation().RevisionID})
				eventAt := createdAt.Add(time.Minute)
				event := eventtest.ExistingRunRootIngressWithRoutingSource(
					eventID, events.EventType("loop.repeat"), "operator", "", payload, 0,
					runtimecorrelation.RunIDFromContext(h.ctx), h.envelope(), exactJoinRoutingSource(h.flowID, h.path, h.entityID), eventAt,
				)
				repeat := runtimecontracts.SystemNodeEventHandler{
					Loop: &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "awaiting"}, AdvancesTo: "awaiting",
				}
				result, err := executeNativePublishedWorkflowJoinForTest(t, h.fixture, h.mutations, h.pc, h.ctx, pipelineNode(t, flowID, "observer"), repeat, workflowTriggerContext{
					Event: event, State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID),
				})
				if err != nil || !result.Handled {
					t.Fatalf("repeat loop = handled:%v err:%v", result.Handled, err)
				}

				instance := h.instance()
				carrier, err := workflowInstanceStateCarrier(instance)
				if err != nil {
					t.Fatal(err)
				}
				nextLoop, found, err := loopruntime.Load(carrier.StateBuckets, declarationFlowID, "revision")
				if err != nil || !found || nextLoop.Generation().Equal(loop.Generation()) {
					t.Fatalf("next loop = found:%v activation:%#v err:%v", found, nextLoop, err)
				}
				firstActivation, found, err := joinruntime.Load(
					carrier.StateBuckets, pipelineNode(t, flowID, "join-node"),
					joinruntime.ActivationKey(firstRef),
				)
				if err != nil || !found || firstActivation.Status != joinruntime.StatusClosed ||
					firstActivation.CloseReason != joinruntime.CloseReasonStageExit || !firstActivation.TimerCancelled ||
					!firstActivation.JoinRef().Equal(firstRef) {
					t.Fatalf("superseded join = found:%v activation:%#v err:%v", found, firstActivation, err)
				}
				_, cancellations := h.mutations.schedules()
				if len(cancellations) != 1 || cancellations[0].Command.ScheduleKey != schedule.Command.ScheduleKey {
					t.Fatalf("superseded cancellations = %#v, want %q", cancellations, schedule.Command.ScheduleKey)
				}

				beforeStale := h.instance()
				h.restart()
				if _, err := h.fire(h.scheduleEvent(schedule, "superseded-generation")); err == nil {
					t.Fatal("stale superseded occurrence was accepted")
				} else if envelope, ok := runtimefailures.EnvelopeFromError(err); !ok || envelope.Class != runtimefailures.ClassUnexpectedArrival {
					t.Fatalf("stale superseded occurrence = %v, envelope=%#v", err, envelope)
				}
				afterStale := h.instance()
				if !exactJoinSemanticStateEqual(beforeStale, afterStale) {
					t.Fatalf("stale generation mutated semantic state\nbefore=%#v\nafter=%#v", beforeStale, afterStale)
				}
				if verify != nil {
					verify(t, h)
				}
			})
		}
	}
}

func VerifyNativeNestedFanOutDiamondJoinsRetainIndependentDeclarationHandlesForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		t.Run(storeCase.name, func(t *testing.T) {
			bundle := workflowJoinLifecycleBundle(t)
			source := exactRootAndFlowJoinSource(bundle)
			schedules := &recordingGenericScheduleWakeupOwner{}
			fixture := open(t, storeCase.name, source)
			ctx := runtimecorrelation.WithRunID(fixture.Context, uuid.NewString())
			if err := fixture.RequireRun(ctx, runtimecorrelation.RunIDFromContext(ctx)); err != nil {
				t.Fatal(err)
			}
			mutations := &nativePipelineLifecycleMutationObservationForTest{}
			pc := nativeWorkflowJoinSourceCoordinatorForTest(t, fixture, source, schedules, mutations)
			store := pc.workflowStore
			runID := runtimecorrelation.RunIDFromContext(ctx)
			root := seedExactJoinScope(t, fixture, ctx, source, "", runID)
			left := seedExactJoinScope(t, fixture, ctx, source, "orders", "orders/"+uuid.NewString())
			right := seedExactJoinScope(t, fixture, ctx, source, "orders", "orders/"+uuid.NewString())
			for _, scope := range []exactJoinScope{root, left, right} {
				if err := applyTestInitialEntryEffect(ctx, pc, scope.route, scope.entityID); err != nil {
					t.Fatalf("arm %s diamond join: %v", scope.path, err)
				}
			}
			upserts, _ := mutations.schedules()
			if len(upserts) != 3 {
				t.Fatalf("diamond schedules = %#v", upserts)
			}
			_, leftRef, leftOK := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, upserts[1])))
			_, rightRef, rightOK := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, upserts[2])))
			if !leftOK || !rightOK || !leftRef.Declaration().Equal(rightRef.Declaration()) || upserts[1].Command.TaskID == upserts[2].Command.TaskID || upserts[1].Command.EntityID == upserts[2].Command.EntityID {
				t.Fatalf("concrete child schedules lost declaration/owner distinction: left=%#v right=%#v", upserts[1].Command, upserts[2].Command)
			}

			fixture = fixture.ReopenExecution()
			ctx = runtimecorrelation.WithRunID(fixture.Context, runtimecorrelation.RunIDFromContext(ctx))
			pc = nativeWorkflowJoinSourceCoordinatorForTest(t, fixture, source, schedules, mutations)
			store = pc.workflowStore
			for _, child := range []exactJoinScope{left, right} {
				for _, member := range []string{"a", "b"} {
					if err := deliverExactJoinMember(t, fixture, mutations, pc, ctx, source, child, member); err != nil {
						t.Fatalf("complete child %s member %s: %v", child.path, member, err)
					}
				}
				activation := exactJoinActivationForScope(t, pc, store, ctx, child)
				if activation.Status != joinruntime.StatusClosed || activation.FlowPath() != "orders" || activation.Completed() != 2 {
					t.Fatalf("child %s activation = %#v", child.path, activation)
				}
			}
			if rootActivation := exactJoinActivationForScope(t, pc, store, ctx, root); rootActivation.Status != joinruntime.StatusOpen || rootActivation.FlowPath() != "." {
				t.Fatalf("child completion mutated root activation: %#v", rootActivation)
			}
			for _, member := range []string{"a", "b"} {
				if err := deliverExactJoinMember(t, fixture, mutations, pc, ctx, source, root, member); err != nil {
					t.Fatalf("complete root diamond member %s: %v", member, err)
				}
			}
			if activation := exactJoinActivationForScope(t, pc, store, ctx, root); activation.Status != joinruntime.StatusClosed || activation.FlowPath() != "." || activation.Completed() != 2 {
				t.Fatalf("root diamond activation = %#v", activation)
			}

			before := make([]WorkflowInstance, 0, 3)
			for _, scope := range []exactJoinScope{root, left, right} {
				instance, _, _ := store.Load(ctx, testRunScopedWorkflowRoute(ctx, scope.route))
				before = append(before, instance)
			}
			hostile := root
			hostile.executionFlowID = "returns"
			if err := deliverExactJoinMember(t, fixture, mutations, pc, ctx, source, hostile, "a"); err == nil {
				t.Fatal("unrelated same-leaf flow scope selected the root declaration")
			}
			for index, scope := range []exactJoinScope{root, left, right} {
				after, _, _ := store.Load(ctx, testRunScopedWorkflowRoute(ctx, scope.route))
				if !exactJoinSemanticStateEqual(before[index], after) {
					t.Fatalf("hostile same-leaf event mutated %s", scope.path)
				}
			}
			_, cancellations := mutations.schedules()
			if len(cancellations) != 6 {
				t.Fatalf("diamond cancellations=%d, want both exact obligations for all three owners", len(cancellations))
			}
			for _, scope := range []exactJoinScope{root, left, right} {
				var owned []runtimegenericschedule.Activation
				for _, cancellation := range cancellations {
					if cancellation.Command.EntityID == scope.entityID {
						owned = append(owned, cancellation)
					}
				}
				assertJoinCompletionCancellationsForTest(t, owned, exactJoinActivationForScope(t, pc, store, ctx, scope).JoinRef())
			}
		})
	}
}

func VerifyNativeReentrantJoinCompletionDoesNotCancelNextGenerationForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		t.Run(storeCase.name, func(t *testing.T) {
			bundle := nativeWorkflowJoinLoopBundleForTest(t, "reentrant")
			h := newNativeExactWorkflowJoinHarness(t, storeCase.name, "orders", "dispatching", []any{"a", "b"}, bundle, open)
			loop := h.startLoop()
			createdAt := loop.StartedAt

			firstSchedule := h.armedSchedule()
			handler := h.source.ExecutableNodeEventHandlers(mustPipelineNode(h.flowID, "join-node"))["item.completed"]
			for _, member := range []string{"a", "b"} {
				event := eventtest.ExistingRunRootIngressWithRoutingSource(
					uuid.NewString(), events.EventType("item.completed"), "operator", "",
					mustJSON(map[string]any{"member_id": member, "result": map[string]any{"value": member}, "revision_id": loop.Generation().RevisionID}), 0,
					runtimecorrelation.RunIDFromContext(h.ctx), h.envelope(), exactJoinRoutingSource(h.flowID, h.path, h.entityID), time.Now().UTC(),
				)
				if _, err := executeNativePublishedWorkflowJoinForTest(t, h.fixture, h.mutations, h.pc, h.ctx, pipelineNode(t, h.flowID, "join-node"), handler, workflowTriggerContext{
					Event: event, State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID), HandlerEventKey: "item.completed",
				}); err != nil {
					t.Fatalf("complete first generation member %s: %v", member, err)
				}
			}
			if got := h.instance().CurrentState; got != "ready" {
				t.Fatalf("first generation state = %q", got)
			}

			repeatEventID := uuid.NewString()
			repeatPayload := mustJSON(map[string]any{"revision_id": loop.Generation().RevisionID})
			repeatAt := createdAt.Add(time.Minute)
			repeatEvent := eventtest.ExistingRunRootIngressWithRoutingSource(
				repeatEventID, events.EventType("loop.repeat"), "operator", "", repeatPayload, 0,
				runtimecorrelation.RunIDFromContext(h.ctx), h.envelope(), exactJoinRoutingSource(h.flowID, h.path, h.entityID), repeatAt,
			)
			repeat := runtimecontracts.SystemNodeEventHandler{
				Loop: &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "ready"}, AdvancesTo: "awaiting",
			}
			result, err := executeNativePublishedWorkflowJoinForTest(t, h.fixture, h.mutations, h.pc, h.ctx, pipelineNode(t, h.flowID, "observer"), repeat, workflowTriggerContext{
				Event: repeatEvent, State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID),
			})
			if err != nil || !result.Handled {
				t.Fatalf("re-enter next generation = handled:%v err:%v", result.Handled, err)
			}

			instance := h.instance()
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			nextLoop, found, err := loopruntime.Load(carrier.StateBuckets, "orders", "revision")
			if err != nil || !found || nextLoop.Generation().Equal(loop.Generation()) {
				t.Fatalf("next loop generation = found:%v loop:%#v err:%v", found, nextLoop, err)
			}
			entry, entryFound, err := runtimeworkflowlifecycle.LoadStageEntry(instance.Bookkeeping)
			if err != nil || !entryFound {
				t.Fatalf("next lifecycle entry: found=%v err=%v", entryFound, err)
			}
			nextRef, err := timeridentity.NewJoinRef(pipelineNode(t, "orders", "join-node"), "item.completed", "awaiting", "awaiting")
			if err != nil {
				t.Fatal(err)
			}
			nextRef, err = nextRef.BindStageEntry(entry, nextLoop.Generation())
			if err != nil {
				t.Fatal(err)
			}
			nextKey := joinruntime.ActivationKey(nextRef)
			nextJoin, found, err := joinruntime.Load(carrier.StateBuckets, pipelineNode(t, "orders", "join-node"), nextKey)
			if err != nil || !found || nextJoin.Status != joinruntime.StatusOpen || !nextJoin.Generation().Equal(nextLoop.Generation()) {
				t.Fatalf("next generation join = found:%v activation:%#v err:%v", found, nextJoin, err)
			}

			beforeStale := h.instance()
			h.restart()
			staleResult, staleErr := h.fire(h.scheduleEvent(firstSchedule, "stale-first-generation"))
			if staleErr != nil || staleResult.Outcome == nil || staleResult.Outcome.Status != HandlerOutcomeDiscarded {
				t.Errorf("completed timer redelivery = %#v err=%v", staleResult, staleErr)
			}
			afterStale := h.instance()
			if !exactJoinSemanticStateEqual(beforeStale, afterStale) {
				t.Fatal("stale first-generation occurrence mutated the live next generation")
			}
			abort := nativeWorkflowJoinEventForTest(h.ctx, "orders", h.path, h.entityID, "manual.abort", mustJSON(map[string]any{"revision_id": nextLoop.RevisionID}), time.Now().UTC())
			dispatchNativeWorkflowJoinEventForTest(t, h.fixture, h.pc, h.ctx, abort, "dispatcher")
			instance = h.instance()
			carrier, err = workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			nextJoin, found, err = joinruntime.Load(carrier.StateBuckets, pipelineNode(t, "orders", "join-node"), nextKey)
			if err != nil || !found || nextJoin.Status != joinruntime.StatusClosed || !nextJoin.Generation().Equal(nextLoop.Generation()) || !nextJoin.TimerCancelled {
				t.Fatalf("next generation cancellation = found:%v activation:%#v err:%v", found, nextJoin, err)
			}
		})
	}
}

func VerifyNativeConcurrentRootAndFlowSameLeafJoinsRemainDistinctAcrossRestartForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, fireRoot := range []bool{false, true} {
			name := "fire-flow-cancel-root"
			if fireRoot {
				name = "fire-root-cancel-flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				bundle := workflowJoinLifecycleBundle(t)
				source := exactRootAndFlowJoinSource(bundle)
				schedules := &recordingGenericScheduleWakeupOwner{}
				fixture := open(t, storeCase.name, source)
				ctx := runtimecorrelation.WithRunID(fixture.Context, uuid.NewString())
				if err := fixture.RequireRun(ctx, runtimecorrelation.RunIDFromContext(ctx)); err != nil {
					t.Fatal(err)
				}
				mutations := &nativePipelineLifecycleMutationObservationForTest{}
				pc := nativeWorkflowJoinSourceCoordinatorForTest(t, fixture, source, schedules, mutations)
				store := pc.workflowStore
				runID := runtimecorrelation.RunIDFromContext(ctx)
				root := seedExactJoinScope(t, fixture, ctx, source, "", runID)
				flow := seedExactJoinScope(t, fixture, ctx, source, "orders", "orders/"+uuid.NewString())
				for _, scope := range []exactJoinScope{root, flow} {
					if err := applyTestInitialEntryEffect(ctx, pc, scope.route, scope.entityID); err != nil {
						t.Fatalf("arm %s: %v", scope.path, err)
					}
				}
				upserts, _ := mutations.schedules()
				if len(upserts) != 2 {
					t.Fatalf("same-leaf schedules = %#v", upserts)
				}
				scheduleByEntity := map[string]runtimegenericschedule.Activation{}
				for _, schedule := range upserts {
					scheduleByEntity[schedule.Command.EntityID] = schedule
				}
				rootSchedule, flowSchedule := scheduleByEntity[root.entityID], scheduleByEntity[flow.entityID]
				_, rootRef, rootOK := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, rootSchedule)))
				_, flowRef, flowOK := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, flowSchedule)))
				if !rootOK || !flowOK || rootRef.FlowPath() != "." || flowRef.FlowPath() != "orders" || rootRef.Equal(flowRef) {
					t.Fatalf("same-leaf handles = root:%#v/%v flow:%#v/%v", rootRef, rootOK, flowRef, flowOK)
				}

				fixture = fixture.ReopenExecution()
				ctx = runtimecorrelation.WithRunID(fixture.Context, runtimecorrelation.RunIDFromContext(ctx))
				pc = nativeWorkflowJoinSourceCoordinatorForTest(t, fixture, source, schedules, mutations)
				store = pc.workflowStore
				firedScope, cancelledScope := flow, root
				firedSchedule := flowSchedule
				if fireRoot {
					firedScope, cancelledScope = root, flow
					firedSchedule = rootSchedule
				}
				fireEnvelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: firedScope.executionFlowID, FlowInstance: firedScope.path, EntityID: firedScope.entityID})
				fireEvent := workflowJoinScheduleEventForTest(t, uuid.NewString(), firedSchedule, runID, fireEnvelope, time.Now().UTC())
				result, err := executeNativeResolvedJoinForTest(t, fixture, pc, ctx, fireEvent, workflowTriggerContext{
					Event: fireEvent, State: mustCurrentWorkflowState(t, pc, ctx, firedScope.route, firedScope.entityID),
				})
				if err != nil || !result.Handled {
					t.Fatalf("fire exact same-leaf join = handled:%v err:%v", result.Handled, err)
				}
				abort := nativeWorkflowJoinEventForTest(ctx, cancelledScope.declarationFlowID, cancelledScope.path, cancelledScope.entityID, "manual.abort", []byte("{}"), time.Now().UTC())
				dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, abort, "dispatcher")
				fired := exactJoinActivationForScope(t, pc, store, ctx, firedScope)
				cancelled := exactJoinActivationForScope(t, pc, store, ctx, cancelledScope)
				if fired.CloseReason != joinruntime.CloseReasonDeadline || fired.FlowPath() != pipelineDeclarationFlowPath(firedScope.declarationFlowID) || !fired.OutcomeFired {
					t.Fatalf("fired same-leaf activation = %#v", fired)
				}
				if cancelled.CloseReason != joinruntime.CloseReasonStageExit || cancelled.FlowPath() != pipelineDeclarationFlowPath(cancelledScope.declarationFlowID) || !cancelled.TimerCancelled {
					t.Fatalf("cancelled same-leaf activation = %#v", cancelled)
				}
			})
		}
	}
}

func pipelineJoinHandle(t *testing.T, flowID string, kind timeridentity.TimerHandleKind, runID, path, entityID string) timeridentity.TimerHandle {
	t.Helper()
	ref := initialWorkflowJoinRefForTest(t, runtimecorrelation.WithRunID(context.Background(), runID), pipelineNode(t, flowID, "join-node"), path, entityID, "awaiting", "awaiting")
	if kind == timeridentity.TimerHandleJoinComplete {
		handle, err := timeridentity.JoinCompleteHandle(ref)
		if err != nil {
			t.Fatal(err)
		}
		return handle
	}
	handle, err := timeridentity.JoinTimeoutHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func exactJoinOccurrenceEvent(t *testing.T, id string, handle timeridentity.TimerHandle, source events.RoutingSource, envelope events.EventEnvelope) events.Event {
	t.Helper()
	return exactJoinOccurrenceEventWithFacts(t, id, handle.EventType(), "runtime.generic_schedule", handle.TaskID(), handle, source, envelope)
}

func exactJoinOccurrenceEventWithFacts(t *testing.T, id, eventType, producer, taskID string, handle timeridentity.TimerHandle, source events.RoutingSource, envelope events.EventEnvelope) events.Event {
	t.Helper()
	payload, err := json.Marshal(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	return eventtest.RuntimeControlWithRoutingSource(
		eventtest.UUID(id), events.EventType(eventType), producer, taskID, payload, 0,
		mustJoinRefForTest(t, handle).StageEntry().RunID, "", envelope, source, time.Now().UTC(),
	)
}
