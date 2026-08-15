package pipeline

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

type exactWorkflowJoinSource struct {
	semanticview.Source
	plans             []runtimecontracts.WorkflowJoinPlan
	nodeFlowID        string
	overrideNodeOwner bool
}

type exactWorkflowJoinHarness struct {
	t         *testing.T
	store     *workflowInstanceStore
	ctx       context.Context
	bundle    *runtimecontracts.WorkflowContractBundle
	source    exactWorkflowJoinSource
	schedules *recordingGenericScheduleWakeupOwner
	bus       *recordingPipelineBus
	pc        *PipelineCoordinator
	flowID    string
	path      string
	route     runtimeflowidentity.Route
	entityID  string
}

func (s exactWorkflowJoinSource) WorkflowJoins() []runtimecontracts.WorkflowJoinPlan {
	return append([]runtimecontracts.WorkflowJoinPlan(nil), s.plans...)
}

func (s exactWorkflowJoinSource) NodeContractSource(nodeID string) (runtimecontracts.ContractItemSource, bool) {
	if s.overrideNodeOwner && (nodeID == "join-node" || nodeID == "dispatcher" || nodeID == "observer") {
		layer := "flow"
		if s.nodeFlowID == "" {
			layer = "project"
		}
		return runtimecontracts.ContractItemSource{FlowID: s.nodeFlowID, Layer: layer}, true
	}
	return s.Source.NodeContractSource(nodeID)
}

func newExactWorkflowJoinHarness(
	t *testing.T,
	storeCase workflowJoinStoreCase,
	flowID string,
	initialState string,
	members []any,
) *exactWorkflowJoinHarness {
	t.Helper()
	store, ctx := storeCase.open(t)
	bundle := workflowJoinLifecycleBundle()
	plan := bundle.Semantics.Joins[0]
	plan.FlowID = flowID
	source := exactWorkflowJoinSource{
		Source: workflowJoinLifecycleSource(bundle), plans: []runtimecontracts.WorkflowJoinPlan{plan},
		nodeFlowID: flowID, overrideNodeOwner: true,
	}
	harness := &exactWorkflowJoinHarness{
		t: t, store: store, ctx: ctx, bundle: bundle, source: source,
		schedules: &recordingGenericScheduleWakeupOwner{}, bus: &recordingPipelineBus{}, flowID: flowID,
	}
	harness.path = runtimecorrelation.RunIDFromContext(ctx)
	workflowName := source.WorkflowName()
	if flowID != "" {
		harness.path = flowID + "/" + uuid.NewString()
		workflowName = flowID
	}
	harness.route = testWorkflowInstanceRoute(harness.path)
	harness.entityID = FlowInstanceEntityID(harness.path)
	harness.pc = harness.newCoordinator()
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: uuid.NewString(), StorageRef: harness.path, WorkflowName: workflowName, WorkflowVersion: "1.0.0",
		EntityID: harness.entityID, CurrentState: initialState, EnteredStageAt: time.Now().UTC(),
		Fields: map[string]any{"expected": append([]any{}, members...)},
	})); err != nil {
		t.Fatal(err)
	}
	return harness
}

func (h *exactWorkflowJoinHarness) newCoordinator() *PipelineCoordinator {
	h.t.Helper()
	return newWorkflowJoinPipelineCoordinator(h.bus, h.store.testDB(), PipelineCoordinatorOptions{
		Module: &pipelineFixtureWorkflowModule{source: h.source}, Persistence: workflowPersistenceForTest(h.store), GenericSchedules: h.schedules,
	})
}

func (h *exactWorkflowJoinHarness) restart() {
	h.t.Helper()
	h.pc = h.newCoordinator()
}

func (h *exactWorkflowJoinHarness) envelope() events.EventEnvelope {
	h.t.Helper()
	if h.flowID == "" {
		return events.EnvelopeForEntityID(events.EventEnvelope{}, h.entityID)
	}
	return workflowJoinTestEnvelope(h.path, h.entityID)
}

func (h *exactWorkflowJoinHarness) armInitial() runtimegenericschedule.Activation {
	h.t.Helper()
	if err := applyTestInitialEntryEffect(h.ctx, h.pc, h.route, h.entityID); err != nil {
		h.t.Fatalf("arm exact join: %v", err)
	}
	upserts, _ := committedWorkflowSchedulesForTest(h.t, h.store)
	if len(upserts) != 1 {
		h.t.Fatalf("armed schedules = %#v", upserts)
	}
	_, ref, ok := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(h.t, upserts[0])))
	if !ok || ref.FlowID() != h.flowID || upserts[0].Command.RoutingSource.Route().FlowID != h.flowID {
		h.t.Fatalf("armed declaration = ref:%#v schedule:%#v ok=%v", ref, upserts[0], ok)
	}
	return upserts[0]
}

func (h *exactWorkflowJoinHarness) transition(nextState, eventType string) {
	h.t.Helper()
	transitionCtx := testPersistedWorkflowStateTransitionContext(h.t, h.store, h.ctx, h.route, h.entityID, eventType)
	if err := h.pc.persistWorkflowStateForTest(transitionCtx, h.route, h.entityID, nextState, eventType); err != nil {
		h.t.Fatalf("transition workflow to %s: %v", nextState, err)
	}
}

func (h *exactWorkflowJoinHarness) instance() WorkflowInstance {
	h.t.Helper()
	instance, found, err := h.store.Load(h.ctx, h.route)
	if err != nil || !found {
		h.t.Fatalf("load workflow instance: found=%v err=%v", found, err)
	}
	return instance
}

func (h *exactWorkflowJoinHarness) activation() joinruntime.Activation {
	h.t.Helper()
	instance := h.instance()
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		h.t.Fatal(err)
	}
	activation, found, err := joinruntime.Load(carrier.StateBuckets, "join-node", workflowJoinActivationKey())
	if err != nil || !found {
		h.t.Fatalf("load join activation: found=%v err=%v", found, err)
	}
	return activation
}

func (h *exactWorkflowJoinHarness) scheduleEvent(schedule runtimegenericschedule.Activation, eventID string) events.Event {
	h.t.Helper()
	return workflowJoinScheduleEventForTest(
		h.t, eventID, schedule, runtimecorrelation.RunIDFromContext(h.ctx), h.envelope(), time.Now().UTC(),
	)
}

func (h *exactWorkflowJoinHarness) fire(event events.Event) (contractHandlerExecutionResult, error) {
	h.t.Helper()
	return h.pc.executeAuthoritativeNodeHandler(h.ctx, event, workflowTriggerContext{
		Event: event, State: mustCurrentWorkflowState(h.t, h.pc, h.ctx, h.route, h.entityID),
	})
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
	plan := bundle.Semantics.Joins[0]
	rootPlan := plan
	rootPlan.FlowID = ""
	flowPlan := plan
	flowPlan.FlowID = "orders"
	return exactWorkflowJoinSource{
		Source: workflowJoinLifecycleSource(bundle),
		plans:  []runtimecontracts.WorkflowJoinPlan{rootPlan, flowPlan},
	}
}

func seedExactJoinScope(t *testing.T, store *workflowInstanceStore, ctx context.Context, source semanticview.Source, declarationFlowID, path string) exactJoinScope {
	t.Helper()
	executionFlowID := declarationFlowID
	if executionFlowID == "" {
		executionFlowID = source.WorkflowName()
	}
	route := testWorkflowInstanceRoute(path)
	entityID := FlowInstanceEntityID(path)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: route.InstanceID, StorageRef: path, WorkflowName: executionFlowID, WorkflowVersion: "1.0.0",
		EntityID: entityID, CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(),
		Fields: map[string]any{"expected": []any{"a", "b"}},
	})); err != nil {
		t.Fatal(err)
	}
	return exactJoinScope{declarationFlowID: declarationFlowID, executionFlowID: executionFlowID, path: path, route: route, entityID: entityID}
}

func exactJoinActivationForScope(t *testing.T, pc *PipelineCoordinator, store *workflowInstanceStore, ctx context.Context, scope exactJoinScope) joinruntime.Activation {
	t.Helper()
	instance, found, err := store.Load(ctx, scope.route)
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

func deliverExactJoinMember(t *testing.T, pc *PipelineCoordinator, store *workflowInstanceStore, ctx context.Context, source semanticview.Source, scope exactJoinScope, member string) error {
	t.Helper()
	envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, scope.entityID)
	if scope.declarationFlowID != "" {
		envelope = workflowJoinTestEnvelope(scope.path, scope.entityID)
	}
	event := eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType("item.completed"), "operator", "",
		mustJSON(map[string]any{"member_id": member, "result": map[string]any{"value": member}}), 0,
		runtimecorrelation.RunIDFromContext(ctx), "", envelope, time.Now().UTC(),
	)
	persistExactJoinEvent(t, store, ctx, event)
	target := events.RouteIdentity{FlowID: scope.executionFlowID, FlowInstance: scope.path, EntityID: scope.entityID}
	deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("join-node"),
		Target:    events.MustExistingEntityTarget(target),
	})
	handler := source.NodeEventHandlers("join-node")["item.completed"]
	_, err := pc.executeNodeContractHandler(deliveryCtx, "join-node", handler, workflowTriggerContext{
		Event: event, State: mustCurrentWorkflowState(t, pc, ctx, scope.route, scope.entityID), HandlerEventKey: "item.completed",
	}, false)
	return err
}

func persistExactJoinEvent(t testing.TB, store *workflowInstanceStore, ctx context.Context, event events.Event) {
	t.Helper()
	dialect := authoractivityfixture.DialectPostgres
	if store.isSQLite() {
		dialect = authoractivityfixture.DialectSQLite
	}
	seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, event)
}

func TestJoinScheduleFactsAreDerivedOnlyFromTypedDeclarationHandle(t *testing.T) {
	source := workflowJoinLifecycleSource(workflowJoinLifecycleBundle())
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	entityID := uuid.NewString()
	instanceRoute := testWorkflowInstanceRoute("orders/order-1")
	for _, tc := range []struct {
		name             string
		flowID           string
		wantKind         events.RoutingSourceKind
		wantFlowInstance string
	}{
		{name: "explicit root", flowID: "", wantKind: events.RoutingSourceRoot},
		{name: "flow owned", flowID: "orders", wantKind: events.RoutingSourceFlowOwnedControl, wantFlowInstance: instanceRoute.InstancePath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handle := pipelineJoinHandle(t, tc.flowID, timeridentity.TimerHandleJoinTimeout)
			activation, err := joinruntime.NewActivation(handle, []string{"a"}, now, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			command, err := joinSchedule(source, entityID, instanceRoute, activation, executionmode.Live)
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
	bundle := workflowJoinLifecycleBundle()
	base := workflowJoinLifecycleSource(bundle)
	plan := bundle.Semantics.Joins[0]
	rootPlan := plan
	rootPlan.FlowID = ""
	flowPlan := plan
	flowPlan.FlowID = "orders"
	source := exactWorkflowJoinSource{Source: base, plans: []runtimecontracts.WorkflowJoinPlan{rootPlan, flowPlan}}
	entityID := uuid.NewString()
	flowInstance := "orders/order-1"
	rootHandle := pipelineJoinHandle(t, "", timeridentity.TimerHandleJoinTimeout)
	flowHandle := pipelineJoinHandle(t, "orders", timeridentity.TimerHandleJoinTimeout)
	otherFlowHandle := pipelineJoinHandle(t, "returns", timeridentity.TimerHandleJoinTimeout)
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
			resolution, ok, err := resolveWorkflowJoinOccurrence(source, tc.event)
			if err != nil || !ok || resolution.Ref.FlowID() != tc.flowID || resolution.Plan.FlowID != tc.flowID {
				t.Fatalf("resolution = %#v ok=%v err=%v", resolution, ok, err)
			}
			recipient, target, handler, ok, err := ResolveWorkflowJoinOccurrenceDeliveryTarget(source, tc.event)
			if err != nil || !ok || recipient.ID() != "join-node" || handler.NodeID() != "join-node" || handler.Empty() {
				t.Fatalf("delivery target = recipient:%#v target:%#v handler:%#v ok=%v err=%v", recipient, target, handler, ok, err)
			}
			wantExecutionFlow := tc.flowID
			wantInstance := flowInstance
			if tc.flowID == "" {
				wantExecutionFlow = source.WorkflowName()
				wantInstance = tc.event.RunID()
			}
			if target.FlowID != wantExecutionFlow || target.FlowInstance != wantInstance || target.EntityID != entityID || handler.FlowID() != wantExecutionFlow {
				t.Fatalf("delivery target = %#v handler=%#v, want flow=%q instance=%q entity=%q", target, handler, wantExecutionFlow, wantInstance, entityID)
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
	bundle := workflowJoinLifecycleBundle()
	plan := bundle.Semantics.Joins[0]
	rootPlan := plan
	rootPlan.FlowID = ""
	flowPlan := plan
	flowPlan.FlowID = "orders"
	source := exactWorkflowJoinSource{
		Source: workflowJoinLifecycleSource(bundle),
		plans:  []runtimecontracts.WorkflowJoinPlan{rootPlan, flowPlan},
	}
	handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]

	rootRef, err := workflowJoinDeclarationRef(source, source.WorkflowName(), "join-node", "item.completed", handler)
	if err != nil || rootRef.FlowID() != "" {
		t.Fatalf("root declaration = %#v err=%v", rootRef, err)
	}
	flowRef, err := workflowJoinDeclarationRef(source, "orders", "join-node", "item.completed", handler)
	if err != nil || flowRef.FlowID() != "orders" {
		t.Fatalf("flow declaration = %#v err=%v", flowRef, err)
	}
	if rootRef.Equal(flowRef) {
		t.Fatal("same-leaf root and flow declarations collapsed to one identity")
	}
	if _, err := workflowJoinDeclarationRef(source, "returns", "join-node", "item.completed", handler); err == nil {
		t.Fatal("unrelated flow scope selected a same-leaf join declaration")
	}
}

func TestRootAndFlowWorkflowJoinArrivalCompletionCancelsExactScheduleOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, scope := range []struct {
			name   string
			flowID string
		}{
			{name: "root", flowID: ""},
			{name: "flow", flowID: "orders"},
		} {
			t.Run(storeCase.name+"/"+scope.name, func(t *testing.T) {
				store, ctx := storeCase.open(t)
				bundle := workflowJoinLifecycleBundle()
				plan := bundle.Semantics.Joins[0]
				plan.FlowID = scope.flowID
				source := exactWorkflowJoinSource{
					Source: workflowJoinLifecycleSource(bundle), plans: []runtimecontracts.WorkflowJoinPlan{plan},
					nodeFlowID: scope.flowID, overrideNodeOwner: true,
				}
				schedules := &recordingGenericScheduleWakeupOwner{}
				newCoordinator := func() *PipelineCoordinator {
					return newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
						Module: &pipelineFixtureWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store), GenericSchedules: schedules,
					})
				}
				pc := newCoordinator()
				path := runtimecorrelation.RunIDFromContext(ctx)
				workflowName := source.WorkflowName()
				if scope.flowID != "" {
					path = scope.flowID + "/" + uuid.NewString()
					workflowName = scope.flowID
				}
				route := testWorkflowInstanceRoute(path)
				entityID := FlowInstanceEntityID(path)
				if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
					InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: workflowName, WorkflowVersion: "1.0.0",
					EntityID: entityID, CurrentState: "dispatching", EnteredStageAt: time.Now().UTC(),
					Fields: map[string]any{"expected": []any{"a", "b"}},
				})); err != nil {
					t.Fatal(err)
				}
				transitionCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, route, entityID, "dispatch.completed")
				if err := pc.persistWorkflowStateForTest(transitionCtx, route, entityID, "awaiting", "dispatch.completed"); err != nil {
					t.Fatalf("arm exact %s join: %v", scope.name, err)
				}
				upserts, _ := committedWorkflowSchedulesForTest(t, store)
				if len(upserts) != 1 {
					t.Fatalf("armed schedules = %#v", upserts)
				}
				_, armedRef, ok := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, upserts[0])))
				if !ok || armedRef.FlowID() != scope.flowID || upserts[0].Command.RoutingSource.Route().FlowID != scope.flowID {
					t.Fatalf("armed declaration = ref:%#v command:%#v ok=%v", armedRef, upserts[0].Command, ok)
				}
				armedInstance, found, err := store.Load(ctx, route)
				if err != nil || !found {
					t.Fatalf("load armed instance: found=%v err=%v", found, err)
				}
				armedCarrier, err := workflowInstanceStateCarrier(armedInstance)
				if err != nil {
					t.Fatal(err)
				}
				armedActivation, found, err := joinruntime.Load(armedCarrier.StateBuckets, "join-node", workflowJoinActivationKey())
				if err != nil || !found || !armedActivation.JoinRef().Equal(armedRef) {
					t.Fatalf("armed activation = found:%v activation:%#v ref:%#v err:%v", found, armedActivation, armedRef, err)
				}

				handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]
				deliver := func(coordinator *PipelineCoordinator, id, member string) error {
					envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, entityID)
					if scope.flowID != "" {
						envelope = workflowJoinTestEnvelope(path, entityID)
					}
					event := eventtest.RunCreatingRootIngress(id, events.EventType("item.completed"), "", "", mustJSON(map[string]any{"member_id": member, "result": map[string]any{"value": member}}), 0, runtimecorrelation.RunIDFromContext(ctx), "", envelope, time.Now().UTC())
					_, err := coordinator.executeNodeContractHandler(ctx, "join-node", handler, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, coordinator, ctx, route, entityID), HandlerEventKey: "item.completed"}, false)
					return err
				}
				if err := deliver(pc, "member-a", "a"); err != nil {
					t.Fatal(err)
				}
				pc = newCoordinator()
				if err := deliver(pc, "member-b", "b"); err != nil {
					t.Fatal(err)
				}
				instance, found, err := store.Load(ctx, route)
				if err != nil || !found {
					t.Fatalf("load closed instance: found=%v err=%v", found, err)
				}
				carrier, err := workflowInstanceStateCarrier(instance)
				if err != nil {
					t.Fatal(err)
				}
				activation, found, err := joinruntime.Load(carrier.StateBuckets, "join-node", workflowJoinActivationKey())
				if err != nil || !found || activation.Status != joinruntime.StatusClosed || activation.FlowID() != scope.flowID || activation.Completed() != 2 || !activation.TimerCancelled {
					t.Fatalf("closed activation = found:%v activation:%#v err:%v", found, activation, err)
				}
				_, cancellations := committedWorkflowSchedulesForTest(t, store)
				if len(cancellations) != 1 || cancellations[0].Command.ScheduleKey != upserts[0].Command.ScheduleKey {
					t.Fatalf("exact cancellation = %#v, want task %q", cancellations, upserts[0].Command.ScheduleKey)
				}
				if err := deliver(pc, "member-b-duplicate", "b"); err == nil {
					t.Fatal("stale duplicate mutated a closed join")
				}
			})
		}
	}
}

func TestRootAndFlowWorkflowJoinStageExitCancelsExactScheduleOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, flowID := range []string{"", "orders"} {
			name := "root"
			if flowID != "" {
				name = "flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				h := newExactWorkflowJoinHarness(t, storeCase, flowID, "awaiting", []any{"a", "b"})
				schedule := h.armInitial()
				h.transition("dispatching", "manual.abort")
				activation := h.activation()
				if activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonStageExit ||
					!activation.TimerCancelled || activation.FlowID() != flowID {
					t.Fatalf("stage-exit activation = %#v", activation)
				}
				_, cancellations := committedWorkflowSchedulesForTest(t, h.store)
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
				_, afterCancellations := committedWorkflowSchedulesForTest(t, h.store)
				if len(afterCancellations) != 1 {
					t.Fatalf("late occurrence emitted extra cancellations: %#v", afterCancellations)
				}
			})
		}
	}
}

func TestRootAndFlowWorkflowJoinImmediateCompletionFiresExactHandleAfterRestartOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, flowID := range []string{"", "orders"} {
			name := "root"
			if flowID != "" {
				name = "flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				h := newExactWorkflowJoinHarness(t, storeCase, flowID, "awaiting", nil)
				schedule := h.armInitial()
				if schedule.Command.EventType != joinCompleteEvent {
					t.Fatalf("immediate schedule = %#v", schedule)
				}
				activation := h.activation()
				if activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonComplete ||
					!activation.OutcomePending || activation.FlowID() != flowID {
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
				if !activation.OutcomeFired || activation.OutcomePending || !activation.TimerCancelled || activation.FlowID() != flowID {
					t.Fatalf("fired immediate activation = %#v", activation)
				}
				if afterDuplicate.CurrentState != "ready" {
					t.Fatalf("immediate completion state = %q", afterDuplicate.CurrentState)
				}
				_, cancellations := committedWorkflowSchedulesForTest(t, h.store)
				if len(cancellations) != 1 || cancellations[0].Command.ScheduleKey != schedule.Command.ScheduleKey {
					t.Fatalf("immediate completion cancellations = %#v", cancellations)
				}
			})
		}
	}
}

func TestRootAndFlowWorkflowJoinTimeoutFiresExactHandleAfterRestartOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, flowID := range []string{"", "orders"} {
			name := "root"
			if flowID != "" {
				name = "flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				h := newExactWorkflowJoinHarness(t, storeCase, flowID, "awaiting", []any{"a", "b"})
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
				if activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonTimeout ||
					!activation.OutcomeFired || !activation.TimerCancelled || activation.FlowID() != flowID {
					t.Fatalf("timed-out activation = %#v", activation)
				}
				if afterDuplicate.CurrentState != "attention" {
					t.Fatalf("timeout state = %q", afterDuplicate.CurrentState)
				}
				_, cancellations := committedWorkflowSchedulesForTest(t, h.store)
				if len(cancellations) != 1 || cancellations[0].Command.ScheduleKey != schedule.Command.ScheduleKey {
					t.Fatalf("timeout cancellations = %#v", cancellations)
				}
			})
		}
	}
}

func TestRootAndFlowWorkflowJoinLoopSupersessionCancelsExactGenerationOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, flowID := range []string{"", "orders"} {
			name := "root"
			if flowID != "" {
				name = "flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				h := newExactWorkflowJoinHarness(t, storeCase, flowID, "awaiting", []any{"a", "b"})
				observer := runtimecontracts.SystemNodeContract{ID: "observer", ExecutionType: "system_node"}
				h.bundle.Nodes["observer"] = observer
				h.bundle.FlowTree.ByID["orders"].Nodes["observer"] = observer
				h.bundle.Semantics.Loops = []runtimecontracts.WorkflowLoopPlan{{
					FlowID: flowID, ID: "revision", RevisionField: "revision_id",
					MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 3}, EntryStage: "awaiting", RegionStages: []string{"awaiting"},
				}}
				createdAt := time.Now().UTC()
				loop, err := loopruntime.New(
					runtimecorrelation.RunIDFromContext(h.ctx), h.entityID, flowID, "revision", "revision_id",
					uuid.NewString(), "awaiting", 3, createdAt,
				)
				if err != nil {
					t.Fatal(err)
				}
				instance := h.instance()
				carrier, err := workflowInstanceStateCarrier(instance)
				if err != nil {
					t.Fatal(err)
				}
				if err := loopruntime.Store(carrier.StateBuckets, loop); err != nil {
					t.Fatal(err)
				}
				instance.StateBuckets = carrier.PersistedStateBuckets()
				if err := h.store.upsert(h.ctx, instance); err != nil {
					t.Fatal(err)
				}

				schedule := h.armInitial()
				_, firstRef, ok := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, schedule)))
				if !ok || !firstRef.Generation().Equal(loop.Generation()) || firstRef.FlowID() != flowID {
					t.Fatalf("first generation handle = %#v ok=%v, loop=%#v", firstRef, ok, loop.Generation())
				}

				eventID := uuid.NewString()
				payload := mustJSON(map[string]any{"revision_id": loop.Generation().RevisionID})
				eventAt := createdAt.Add(time.Minute)
				event := eventtest.RunCreatingRootIngress(
					eventID, events.EventType("loop.repeat"), "operator", "", payload, 0,
					runtimecorrelation.RunIDFromContext(h.ctx), "", h.envelope(), eventAt,
				)
				persistWorkflowTimerEvent(t, h.store, h.ctx, eventID, "loop.repeat", runtimecorrelation.RunIDFromContext(h.ctx), h.entityID, payload, eventAt)
				repeat := runtimecontracts.SystemNodeEventHandler{
					Loop: &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "awaiting"}, AdvancesTo: "awaiting",
				}
				result, err := h.pc.executeNodeContractHandler(h.ctx, "observer", repeat, workflowTriggerContext{
					Event: event, State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID),
				}, false)
				if err != nil || !result.Handled {
					t.Fatalf("repeat loop = handled:%v err:%v", result.Handled, err)
				}

				instance = h.instance()
				carrier, err = workflowInstanceStateCarrier(instance)
				if err != nil {
					t.Fatal(err)
				}
				nextLoop, found, err := loopruntime.Load(carrier.StateBuckets, flowID, "revision")
				if err != nil || !found || nextLoop.Generation().Equal(loop.Generation()) {
					t.Fatalf("next loop = found:%v activation:%#v err:%v", found, nextLoop, err)
				}
				firstActivation, found, err := joinruntime.Load(
					carrier.StateBuckets, "join-node",
					joinruntime.ActivationKeyForGeneration("awaiting", "awaiting", "", loop.Generation()),
				)
				if err != nil || !found || firstActivation.Status != joinruntime.StatusClosed ||
					firstActivation.CloseReason != joinruntime.CloseReasonStageExit || !firstActivation.TimerCancelled ||
					!firstActivation.JoinRef().Equal(firstRef) {
					t.Fatalf("superseded join = found:%v activation:%#v err:%v", found, firstActivation, err)
				}
				_, cancellations := committedWorkflowSchedulesForTest(t, h.store)
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
			})
		}
	}
}

func TestNestedFanOutDiamondJoinsRetainIndependentDeclarationHandles(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		t.Run(storeCase.name, func(t *testing.T) {
			store, ctx := storeCase.open(t)
			bundle := workflowJoinLifecycleBundle()
			source := exactRootAndFlowJoinSource(bundle)
			schedules := &recordingGenericScheduleWakeupOwner{}
			newCoordinator := func() *PipelineCoordinator {
				return newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
					Module: &pipelineFixtureWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store), GenericSchedules: schedules,
				})
			}
			pc := newCoordinator()
			runID := runtimecorrelation.RunIDFromContext(ctx)
			root := seedExactJoinScope(t, store, ctx, source, "", runID)
			left := seedExactJoinScope(t, store, ctx, source, "orders", "orders/"+uuid.NewString())
			right := seedExactJoinScope(t, store, ctx, source, "orders", "orders/"+uuid.NewString())
			for _, scope := range []exactJoinScope{root, left, right} {
				if err := applyTestInitialEntryEffect(ctx, pc, scope.route, scope.entityID); err != nil {
					t.Fatalf("arm %s diamond join: %v", scope.path, err)
				}
			}
			upserts, _ := committedWorkflowSchedulesForTest(t, store)
			if len(upserts) != 3 {
				t.Fatalf("diamond schedules = %#v", upserts)
			}
			if upserts[1].Command.TaskID != upserts[2].Command.TaskID || upserts[1].Command.EntityID == upserts[2].Command.EntityID {
				t.Fatalf("concrete child schedules lost declaration/owner distinction: left=%#v right=%#v", upserts[1].Command, upserts[2].Command)
			}

			pc = newCoordinator()
			for _, child := range []exactJoinScope{left, right} {
				for _, member := range []string{"a", "b"} {
					if err := deliverExactJoinMember(t, pc, store, ctx, source, child, member); err != nil {
						t.Fatalf("complete child %s member %s: %v", child.path, member, err)
					}
				}
				activation := exactJoinActivationForScope(t, pc, store, ctx, child)
				if activation.Status != joinruntime.StatusClosed || activation.FlowID() != "orders" || activation.Completed() != 2 {
					t.Fatalf("child %s activation = %#v", child.path, activation)
				}
			}
			if rootActivation := exactJoinActivationForScope(t, pc, store, ctx, root); rootActivation.Status != joinruntime.StatusOpen || rootActivation.FlowID() != "" {
				t.Fatalf("child completion mutated root activation: %#v", rootActivation)
			}
			for _, member := range []string{"a", "b"} {
				if err := deliverExactJoinMember(t, pc, store, ctx, source, root, member); err != nil {
					t.Fatalf("complete root diamond member %s: %v", member, err)
				}
			}
			if activation := exactJoinActivationForScope(t, pc, store, ctx, root); activation.Status != joinruntime.StatusClosed || activation.FlowID() != "" || activation.Completed() != 2 {
				t.Fatalf("root diamond activation = %#v", activation)
			}

			before := make([]WorkflowInstance, 0, 3)
			for _, scope := range []exactJoinScope{root, left, right} {
				instance, _, _ := store.Load(ctx, scope.route)
				before = append(before, instance)
			}
			hostile := root
			hostile.executionFlowID = "returns"
			if err := deliverExactJoinMember(t, pc, store, ctx, source, hostile, "a"); err == nil {
				t.Fatal("unrelated same-leaf flow scope selected the root declaration")
			}
			for index, scope := range []exactJoinScope{root, left, right} {
				after, _, _ := store.Load(ctx, scope.route)
				if !exactJoinSemanticStateEqual(before[index], after) {
					t.Fatalf("hostile same-leaf event mutated %s", scope.path)
				}
			}
			_, cancellations := committedWorkflowSchedulesForTest(t, store)
			if len(cancellations) != 3 {
				t.Fatalf("diamond cancellations = %#v", cancellations)
			}
		})
	}
}

func TestReentrantJoinCompletionDoesNotCancelNextGeneration(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		t.Run(storeCase.name, func(t *testing.T) {
			h := newExactWorkflowJoinHarness(t, storeCase, "orders", "awaiting", []any{"a", "b"})
			observer := runtimecontracts.SystemNodeContract{ID: "observer", ExecutionType: "system_node"}
			h.bundle.Nodes["observer"] = observer
			h.bundle.FlowTree.ByID["orders"].Nodes["observer"] = observer
			h.bundle.Semantics.Loops = []runtimecontracts.WorkflowLoopPlan{{
				FlowID: "orders", ID: "revision", RevisionField: "revision_id",
				MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 3}, EntryStage: "awaiting", RegionStages: []string{"awaiting", "ready"},
			}}
			createdAt := time.Now().UTC()
			loop, err := loopruntime.New(
				runtimecorrelation.RunIDFromContext(h.ctx), h.entityID, "orders", "revision", "revision_id",
				uuid.NewString(), "awaiting", 3, createdAt,
			)
			if err != nil {
				t.Fatal(err)
			}
			instance := h.instance()
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			if err := loopruntime.Store(carrier.StateBuckets, loop); err != nil {
				t.Fatal(err)
			}
			instance.StateBuckets = carrier.PersistedStateBuckets()
			if err := h.store.upsert(h.ctx, instance); err != nil {
				t.Fatal(err)
			}

			firstSchedule := h.armInitial()
			handler := h.bundle.Nodes["join-node"].EventHandlers["item.completed"]
			handler.Loop = &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "awaiting"}
			for _, member := range []string{"a", "b"} {
				event := eventtest.RunCreatingRootIngress(
					uuid.NewString(), events.EventType("item.completed"), "operator", "",
					mustJSON(map[string]any{"member_id": member, "result": map[string]any{"value": member}, "revision_id": loop.Generation().RevisionID}), 0,
					runtimecorrelation.RunIDFromContext(h.ctx), "", h.envelope(), time.Now().UTC(),
				)
				persistExactJoinEvent(t, h.store, h.ctx, event)
				if _, err := h.pc.executeNodeContractHandler(h.ctx, "join-node", handler, workflowTriggerContext{
					Event: event, State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID), HandlerEventKey: "item.completed",
				}, false); err != nil {
					t.Fatalf("complete first generation member %s: %v", member, err)
				}
			}
			if got := h.instance().CurrentState; got != "ready" {
				t.Fatalf("first generation state = %q", got)
			}

			repeatEventID := uuid.NewString()
			repeatPayload := mustJSON(map[string]any{"revision_id": loop.Generation().RevisionID})
			repeatAt := createdAt.Add(time.Minute)
			repeatEvent := eventtest.RunCreatingRootIngress(
				repeatEventID, events.EventType("loop.repeat"), "operator", "", repeatPayload, 0,
				runtimecorrelation.RunIDFromContext(h.ctx), "", h.envelope(), repeatAt,
			)
			persistWorkflowTimerEvent(t, h.store, h.ctx, repeatEventID, "loop.repeat", runtimecorrelation.RunIDFromContext(h.ctx), h.entityID, repeatPayload, repeatAt)
			repeat := runtimecontracts.SystemNodeEventHandler{
				Loop: &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "ready"}, AdvancesTo: "awaiting",
			}
			result, err := h.pc.executeNodeContractHandler(h.ctx, "observer", repeat, workflowTriggerContext{
				Event: repeatEvent, State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID),
			}, false)
			if err != nil || !result.Handled {
				t.Fatalf("re-enter next generation = handled:%v err:%v", result.Handled, err)
			}

			instance = h.instance()
			carrier, err = workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			nextLoop, found, err := loopruntime.Load(carrier.StateBuckets, "orders", "revision")
			if err != nil || !found || nextLoop.Generation().Equal(loop.Generation()) {
				t.Fatalf("next loop generation = found:%v loop:%#v err:%v", found, nextLoop, err)
			}
			nextKey := joinruntime.ActivationKeyForGeneration("awaiting", "awaiting", "", nextLoop.Generation())
			nextJoin, found, err := joinruntime.Load(carrier.StateBuckets, "join-node", nextKey)
			if err != nil || !found || nextJoin.Status != joinruntime.StatusOpen || !nextJoin.Generation().Equal(nextLoop.Generation()) {
				t.Fatalf("next generation join = found:%v activation:%#v err:%v", found, nextJoin, err)
			}

			beforeStale := h.instance()
			h.restart()
			if _, err := h.fire(h.scheduleEvent(firstSchedule, "stale-first-generation")); err != nil {
				if envelope, ok := runtimefailures.EnvelopeFromError(err); !ok || envelope.Class != runtimefailures.ClassUnexpectedArrival {
					t.Fatalf("stale generation result = %v envelope=%#v", err, envelope)
				}
			}
			afterStale := h.instance()
			if !exactJoinSemanticStateEqual(beforeStale, afterStale) {
				t.Fatal("stale first-generation occurrence mutated the live next generation")
			}
			h.transition("dispatching", "manual.abort")
			instance = h.instance()
			carrier, err = workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			nextJoin, found, err = joinruntime.Load(carrier.StateBuckets, "join-node", nextKey)
			if err != nil || !found || nextJoin.Status != joinruntime.StatusClosed || !nextJoin.Generation().Equal(nextLoop.Generation()) || !nextJoin.TimerCancelled {
				t.Fatalf("next generation cancellation = found:%v activation:%#v err:%v", found, nextJoin, err)
			}
		})
	}
}

func TestConcurrentRootAndFlowSameLeafJoinsRemainDistinctAcrossRestart(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, fireRoot := range []bool{false, true} {
			name := "fire-flow-cancel-root"
			if fireRoot {
				name = "fire-root-cancel-flow"
			}
			t.Run(storeCase.name+"/"+name, func(t *testing.T) {
				store, ctx := storeCase.open(t)
				bundle := workflowJoinLifecycleBundle()
				source := exactRootAndFlowJoinSource(bundle)
				schedules := &recordingGenericScheduleWakeupOwner{}
				newCoordinator := func() *PipelineCoordinator {
					return newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
						Module: &pipelineFixtureWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store), GenericSchedules: schedules,
					})
				}
				pc := newCoordinator()
				runID := runtimecorrelation.RunIDFromContext(ctx)
				root := seedExactJoinScope(t, store, ctx, source, "", runID)
				flow := seedExactJoinScope(t, store, ctx, source, "orders", "orders/"+uuid.NewString())
				for _, scope := range []exactJoinScope{root, flow} {
					if err := applyTestInitialEntryEffect(ctx, pc, scope.route, scope.entityID); err != nil {
						t.Fatalf("arm %s: %v", scope.path, err)
					}
				}
				upserts, _ := committedWorkflowSchedulesForTest(t, store)
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
				if !rootOK || !flowOK || rootRef.FlowID() != "" || flowRef.FlowID() != "orders" || rootRef.Equal(flowRef) {
					t.Fatalf("same-leaf handles = root:%#v/%v flow:%#v/%v", rootRef, rootOK, flowRef, flowOK)
				}

				pc = newCoordinator()
				firedScope, cancelledScope := flow, root
				firedSchedule := flowSchedule
				if fireRoot {
					firedScope, cancelledScope = root, flow
					firedSchedule = rootSchedule
				}
				fireEnvelope := events.EnvelopeForEntityID(events.EventEnvelope{}, firedScope.entityID)
				if firedScope.declarationFlowID != "" {
					fireEnvelope = workflowJoinTestEnvelope(firedScope.path, firedScope.entityID)
				}
				fireEvent := workflowJoinScheduleEventForTest(t, uuid.NewString(), firedSchedule, runID, fireEnvelope, time.Now().UTC())
				persistExactJoinEvent(t, store, ctx, fireEvent)
				result, err := pc.executeAuthoritativeNodeHandler(ctx, fireEvent, workflowTriggerContext{
					Event: fireEvent, State: mustCurrentWorkflowState(t, pc, ctx, firedScope.route, firedScope.entityID),
				})
				if err != nil || !result.Handled {
					t.Fatalf("fire exact same-leaf join = handled:%v err:%v", result.Handled, err)
				}
				transitionCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, cancelledScope.route, cancelledScope.entityID, "manual.abort")
				if err := pc.persistWorkflowStateForTest(transitionCtx, cancelledScope.route, cancelledScope.entityID, "dispatching", "manual.abort"); err != nil {
					t.Fatalf("cancel other same-leaf join: %v", err)
				}
				fired := exactJoinActivationForScope(t, pc, store, ctx, firedScope)
				cancelled := exactJoinActivationForScope(t, pc, store, ctx, cancelledScope)
				if fired.CloseReason != joinruntime.CloseReasonTimeout || fired.FlowID() != firedScope.declarationFlowID || !fired.OutcomeFired {
					t.Fatalf("fired same-leaf activation = %#v", fired)
				}
				if cancelled.CloseReason != joinruntime.CloseReasonStageExit || cancelled.FlowID() != cancelledScope.declarationFlowID || !cancelled.TimerCancelled {
					t.Fatalf("cancelled same-leaf activation = %#v", cancelled)
				}
			})
		}
	}
}

func pipelineJoinHandle(t *testing.T, flowID string, kind timeridentity.TimerHandleKind) timeridentity.TimerHandle {
	t.Helper()
	ref, err := timeridentity.NewJoinRef(flowID, "join-node", "item.completed", "awaiting", "awaiting", "")
	if err != nil {
		t.Fatal(err)
	}
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
		id, events.EventType(eventType), producer, taskID, payload, 0,
		uuid.NewString(), "", envelope, source, time.Now().UTC(),
	)
}
