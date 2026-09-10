package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func newWorkflowJoinPipelineCoordinator(bus Bus, db *sql.DB, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	opts.PipelineObligations = unavailablePipelineTestObligationOwner{}
	return newDurablePipelineCoordinatorForTest(bus, db, opts)
}

type workflowJoinLifecycleSemanticSource struct {
	semanticview.Source
}

func (s workflowJoinLifecycleSemanticSource) ExecutableNodeSource(node identity.ExecutableNode) (runtimecontracts.ContractItemSource, bool) {
	switch node.NodeID() {
	case "join-node", "dispatcher":
		return runtimecontracts.ContractItemSource{FlowPath: node.FlowPath(), Family: "nodes"}, true
	default:
		return s.Source.ExecutableNodeSource(node)
	}
}

func workflowJoinLifecycleSource(bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	return workflowJoinLifecycleSemanticSource{Source: semanticview.Wrap(bundle)}
}

func TestWorkflowLifecycleOwnerIsConstructedBeforeDurableStoreReachabilityOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := tc.open(t)
			coordinator := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
				Module:      &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(workflowJoinLifecycleBundle(t))},
				Persistence: workflowPersistenceForTest(store),
			})
			if coordinator == nil || coordinator.workflowStore == nil || coordinator.workflowStore.lifecycleOwner == nil {
				t.Fatal("durable workflow store became reachable without its canonical lifecycle owner")
			}
		})
	}
}

func TestWorkflowJoinUsesSelectedStoreScheduleOwnerOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bundle := workflowJoinLifecycleBundle(t)
			schedules := &recordingGenericScheduleWakeupOwner{}
			pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
				Module:           &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)},
				Persistence:      workflowPersistenceForTest(store),
				GenericSchedules: schedules,
			})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": []any{"a"}},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}

			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatalf("arm selected-store join: %v", err)
			}
			instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok {
				t.Fatalf("load after rejected ownerless join = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			if activation, found, loadErr := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey()); loadErr != nil || !found {
				t.Fatalf("selected-store join activation = %#v, found=%v error=%v", activation, found, loadErr)
			}
			upserts, _ := committedWorkflowSchedulesForTest(t, store)
			if len(upserts) != 1 || upserts[0].Command.EventType != joinTimeoutEvent {
				t.Fatalf("selected-store join schedules = %#v, want one timeout", upserts)
			}
			if len(schedules.activationIDs) != 1 || schedules.activationIDs[0] != upserts[0].ID {
				t.Fatalf("selected-store join wakeup reconciliation = %#v, want activation %s", schedules.activationIDs, upserts[0].ID)
			}
		})
	}
}

func TestWorkflowJoinSchedulePreservesMockExecutionModeOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Mock)
			bundle := workflowJoinLifecycleBundle(t)
			schedules := &recordingGenericScheduleWakeupOwner{}
			pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
				Module:           &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)},
				Persistence:      workflowPersistenceForTest(store),
				GenericSchedules: schedules,
			})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			enteredAt := time.Now().UTC()
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "dispatching", EnteredStageAt: enteredAt, Fields: map[string]any{"entity_id": entityID, "expected": []any{"a"}},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			route := testWorkflowInstanceRoute(path)
			instance, found, err := store.Load(ctx, route)
			if err != nil || !found {
				t.Fatalf("load workflow instance: found=%v err=%v", found, err)
			}
			instance.CurrentState = "awaiting"
			instance.EnteredStageAt = enteredAt
			transition, err := compiledLifecycleTransitionForTest(pc, instance.WorkflowName, "dispatching", "awaiting", "order.accepted")
			if err != nil {
				t.Fatal(err)
			}
			inbound := workflowLifecycleEventForTest(t, store, ctx, "orders", path, entityID, "orders/order.accepted", enteredAt)
			ctx = runtimecorrelation.WithInboundEvent(ctx, inbound)
			effect, err := runtimeworkflowlifecycle.NewAcceptedEvent(
				route, identity.NormalizeEntityID(entityID), inbound.ID(), string(inbound.Type()), inbound.ExecutionMode(), inbound.CreatedAt(), transition,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := commitTestWorkflowLifecycleMutation(ctx, pc, route, instance, "dispatching", []runtimeworkflowlifecycle.Effect{effect}); err != nil {
				t.Fatalf("commit mock workflow transition: %v", err)
			}
			upserts, _ := committedWorkflowSchedulesForTest(t, store)
			if len(upserts) != 1 || upserts[0].Command.ExecutionMode != executionmode.Mock {
				t.Fatalf("mock join schedules = %#v, want one mock timeout", upserts)
			}
		})
	}
}

func TestArmWorkflowJoinPersistsActivationAndScheduleAtomically(t *testing.T) {
	for _, tc := range []struct {
		name       string
		members    []any
		wantStatus joinruntime.Status
		wantReason joinruntime.CloseReason
		wantEvent  string
		wantKind   timeridentity.TimerHandleKind
	}{
		{name: "members wait on timeout", members: []any{"a", "b"}, wantStatus: joinruntime.StatusOpen, wantEvent: joinTimeoutEvent, wantKind: timeridentity.TimerHandleJoinTimeout},
		{name: "zero members complete immediately", members: []any{}, wantStatus: joinruntime.StatusClosed, wantReason: joinruntime.CloseReasonComplete, wantEvent: joinCompleteEvent, wantKind: timeridentity.TimerHandleJoinComplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newSQLiteWorkflowInstanceStoreTestDB(t)
			store := newSQLiteWorkflowInstanceStoreForTest(t, db)
			schedules := &recordingGenericScheduleWakeupOwner{}
			bundle := workflowJoinLifecycleBundle(t)
			pc := &PipelineCoordinator{
				module:           &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)},
				workflowStore:    store,
				genericSchedules: schedules,
			}
			entityID := FlowInstanceEntityID("orders/order-1")
			runID := uuid.NewString()
			ensurePipelineTestRun(t, store, runID)
			ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), executionmode.Live)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: "order-1", StorageRef: "orders/order-1", WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": tc.members},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute("orders/order-1"), entityID); err != nil {
				t.Fatalf("arm join: %v", err)
			}
			instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute("orders/order-1"))
			if err != nil || !ok {
				t.Fatalf("load instance = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey())
			if err != nil || !ok {
				t.Fatalf("load activation = %#v, %v, %v", activation, ok, err)
			}
			if activation.Status != tc.wantStatus || activation.CloseReason != tc.wantReason {
				t.Fatalf("activation status = %s/%s, want %s/%s", activation.Status, activation.CloseReason, tc.wantStatus, tc.wantReason)
			}
			upserts, _ := committedWorkflowSchedulesForTest(t, store)
			if got := len(upserts); got != 1 {
				t.Fatalf("schedules = %d, want 1", got)
			}
			schedule := upserts[0]
			if schedule.Command.EventType != tc.wantEvent || schedule.Command.EntityID != entityID {
				t.Fatalf("schedule = %#v", schedule)
			}
			handle, ok := timeridentity.ParseTimerHandle(parsePayloadMap(genericSchedulePayloadForTest(t, schedule)))
			ref, refOK := handle.JoinRef()
			if !ok || !refOK || handle.Kind() != tc.wantKind || ref.JoinID() != "awaiting" {
				t.Fatalf("timer handle = %#v, %v", handle, ok)
			}
		})
	}
}

func TestArmWorkflowJoinPostgresParity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		members    []any
		wantStatus joinruntime.Status
		wantEvent  string
	}{
		{name: "members", members: []any{"a"}, wantStatus: joinruntime.StatusOpen, wantEvent: joinTimeoutEvent},
		{name: "expected zero", members: []any{}, wantStatus: joinruntime.StatusClosed, wantEvent: joinCompleteEvent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			runlifecyclefixture.RequirePostgres(t, testAuthorActivityContext(t, context.Background()), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
			ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), executionmode.Live)
			store := newPostgresWorkflowInstanceStoreForTest(db)
			schedules := &recordingGenericScheduleWakeupOwner{}
			pc := &PipelineCoordinator{module: &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(workflowJoinLifecycleBundle(t))}, workflowStore: store, genericSchedules: schedules}
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": tc.members},
				EntityType: "test_entity"})); err != nil {
				t.Fatal(err)
			}
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatal(err)
			}
			instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok {
				t.Fatalf("load = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey())
			if err != nil || !ok || activation.Status != tc.wantStatus {
				t.Fatalf("activation = %#v, %v, %v", activation, ok, err)
			}
			upserts, _ := committedWorkflowSchedulesForTest(t, store)
			if len(upserts) != 1 || upserts[0].Command.EventType != tc.wantEvent {
				t.Fatalf("schedule parity = schedules:%#v", upserts)
			}
		})
	}
}

func TestWorkflowJoinCustomCompletionControlsExpectedZeroOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bundle := workflowJoinLifecycleBundle(t)
			node := bundle.FlowTree.ByID["orders"].Nodes["join-node"]
			handler := node.EventHandlers["item.completed"]
			spec := *handler.Join
			spec.CompleteWhen = "join.completed >= 1"
			spec.Remaining = runtimecontracts.JoinRemainingIgnore
			handler.Join = &spec
			node.EventHandlers["item.completed"] = handler
			bundle.FlowTree.ByID["orders"].Nodes["join-node"] = node
			bundle.Semantics.Joins[0].Spec = spec
			bundle.Semantics.NodeHandlers["join-node"] = node.EventHandlers

			schedules := &recordingGenericScheduleWakeupOwner{}
			pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
				Module:           &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)},
				Persistence:      workflowPersistenceForTest(store),
				GenericSchedules: schedules,
			})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": []any{}},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatalf("arm custom join: %v", err)
			}
			instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok {
				t.Fatalf("load custom join = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey())
			if err != nil || !ok || activation.Status != joinruntime.StatusOpen || activation.CloseReason != "" {
				t.Fatalf("custom zero activation = %#v, %v, %v, want open", activation, ok, err)
			}
			upserts, _ := committedWorkflowSchedulesForTest(t, store)
			if len(upserts) != 1 || upserts[0].Command.EventType != joinTimeoutEvent {
				t.Fatalf("custom zero schedules = %#v, want timeout", upserts)
			}
		})
	}
}

func TestWorkflowJoinArmRejectsCatalogInvalidNamedResultExpression(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	bundle := workflowJoinLifecycleBundle(t)
	node := bundle.FlowTree.ByID["orders"].Nodes["join-node"]
	handler := node.EventHandlers["item.completed"]
	spec := *handler.Join
	spec.CompleteWhen = "join.results[0] > 1"
	spec.Remaining = runtimecontracts.JoinRemainingIgnore
	handler.Join = &spec
	node.EventHandlers["item.completed"] = handler
	bundle.FlowTree.ByID["orders"].Nodes["join-node"] = node
	bundle.Semantics.Joins[0].Spec = spec
	bundle.Semantics.Joins[0].ResultType = runtimecontracts.CatalogTypeReference{
		Type: "JoinResult",
		Catalog: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"JoinResult": {Fields: map[string]runtimecontracts.TypeFieldSpec{"value": {Type: "text"}}},
		}},
	}

	pc := &PipelineCoordinator{
		module:        &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)},
		workflowStore: store,
	}
	entityID := FlowInstanceEntityID("orders/order-typed")
	runID := uuid.NewString()
	ensurePipelineTestRun(t, store, runID)
	ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), executionmode.Live)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: "order-typed", StorageRef: "orders/order-typed", WorkflowName: "orders", WorkflowVersion: "1.0.0",
		CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": []any{}},
		EntityType: "test_entity",
	})); err != nil {
		t.Fatal(err)
	}
	err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute("orders/order-typed"), entityID)
	if err == nil || !strings.Contains(err.Error(), "no matching overload") {
		t.Fatalf("arm join error = %v, want catalog-backed typed rejection", err)
	}
}

func TestWorkflowJoinDurableIdentityIncludesStageOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bundle := workflowJoinLifecycleBundleWithReview(t, true)
			schedules := &recordingGenericScheduleWakeupOwner{}
			pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
				Module:           &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)},
				Persistence:      workflowPersistenceForTest(store),
				GenericSchedules: schedules,
			})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": []any{"a"}},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatal(err)
			}
			transitionAt := time.Now().UTC()
			transition, err := compiledLifecycleTransitionForTest(pc, "orders", "awaiting", "reviewing", "review.requested")
			if err != nil {
				t.Fatal(err)
			}
			inbound := workflowLifecycleEventForTest(t, store, ctx, "orders", path, entityID, "orders/review.requested", transitionAt)
			ctx = runtimecorrelation.WithInboundEvent(ctx, inbound)
			effect, err := runtimeworkflowlifecycle.NewAcceptedEvent(testWorkflowInstanceRoute(path), identity.NormalizeEntityID(entityID), inbound.ID(), string(inbound.Type()), executionmode.Live, inbound.CreatedAt(), transition)
			if err != nil {
				t.Fatal(err)
			}
			instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok {
				t.Fatalf("load before stage transition = %v, %v", ok, err)
			}
			instance.CurrentState = "reviewing"
			instance.EnteredStageAt = transitionAt
			if err := commitTestWorkflowLifecycleMutation(ctx, pc, testWorkflowInstanceRoute(path), instance, "awaiting", []runtimeworkflowlifecycle.Effect{effect}); err != nil {
				t.Fatal(err)
			}
			instance, ok, err = store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok {
				t.Fatalf("load stage-scoped joins = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			awaiting, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), joinruntime.ActivationKey("awaiting", "shared", ""))
			if err != nil || !ok || awaiting.CloseReason != joinruntime.CloseReasonStageExit {
				t.Fatalf("awaiting activation = %#v, %v, %v", awaiting, ok, err)
			}
			reviewing, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), joinruntime.ActivationKey("reviewing", "shared", ""))
			if err != nil || !ok || reviewing.Status != joinruntime.StatusOpen || reviewing.Stage() != "reviewing" {
				t.Fatalf("reviewing activation = %#v, %v, %v", reviewing, ok, err)
			}
			upserts, _ := committedWorkflowSchedulesForTest(t, store)
			if awaiting.Key() == reviewing.Key() || len(upserts) != 2 {
				t.Fatalf("stage identities/schedules = awaiting:%q reviewing:%q schedules:%#v", awaiting.Key(), reviewing.Key(), upserts)
			}
		})
	}
}

type workflowJoinStoreCase struct {
	name string
	open func(*testing.T) (*workflowInstanceStore, context.Context)
}

func committedWorkflowSchedulesForTest(t *testing.T, store *workflowInstanceStore) ([]runtimegenericschedule.Activation, []runtimegenericschedule.Activation) {
	t.Helper()
	runner, ok := store.engineMutations.(*recordingRuntimeMutationRunner)
	if !ok || runner == nil {
		t.Fatalf("workflow test store has unexpected engine mutation owner %T", store.engineMutations)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return append([]runtimegenericschedule.Activation(nil), runner.committedGenericScheduleActivations...), append([]runtimegenericschedule.Activation(nil), runner.committedGenericScheduleCancellations...)
}

func genericSchedulePayloadForTest(t *testing.T, activation runtimegenericschedule.Activation) json.RawMessage {
	t.Helper()
	payload, err := canonicaljson.Encode(activation.Command.Payload)
	if err != nil {
		t.Fatalf("encode generic schedule payload: %v", err)
	}
	return payload
}

func workflowJoinScheduleEventForTest(t *testing.T, id string, activation runtimegenericschedule.Activation, runID string, envelope events.EventEnvelope, at time.Time) events.Event {
	t.Helper()
	return eventtest.RuntimeControlWithRoutingSource(
		id,
		events.EventType(activation.Command.EventType),
		runtimegenericschedule.OccurrenceProducerID(),
		activation.Command.TaskID,
		genericSchedulePayloadForTest(t, activation),
		0,
		runID,
		"",
		envelope,
		activation.Command.RoutingSource,
		at,
	)
}

func workflowJoinTimerEventForTest(t *testing.T, id, eventType string, handle timeridentity.TimerHandle, runID string, envelope events.EventEnvelope, at time.Time) events.Event {
	t.Helper()
	routingSource, err := events.NewFlowOwnedControlRoutingSource(envelope.Source)
	if err != nil {
		t.Fatal(err)
	}
	return eventtest.RuntimeControlWithRoutingSource(
		id,
		events.EventType(eventType),
		runtimegenericschedule.OccurrenceProducerID(),
		handle.TaskID(),
		mustJSON(handle.PayloadMetadata()),
		0,
		runID,
		"",
		envelope,
		routingSource,
		at,
	)
}

func workflowJoinStoreCases() []workflowJoinStoreCase {
	return []workflowJoinStoreCase{
		{name: "sqlite", open: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			store, ctx := newSQLiteWorkflowJoinStore(t)
			return store, runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
		}},
		{name: "postgres", open: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			runlifecyclefixture.RequirePostgres(t, testAuthorActivityContext(t, context.Background()), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
			return newPostgresWorkflowInstanceStoreForTest(db), runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
		}},
	}
}

func newSQLiteWorkflowJoinStore(t *testing.T) (*workflowInstanceStore, context.Context) {
	t.Helper()
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	runID := uuid.NewString()
	ensurePipelineTestRun(t, store, runID)
	return store, runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), executionmode.Live)
}

func TestWorkflowJoinArrivalTimeoutRaceHasOneCloseWinnerOnBothStores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (*workflowInstanceStore, context.Context)
	}{
		{name: "sqlite", store: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			return newSQLiteWorkflowJoinStore(t)
		}},
		{name: "postgres", store: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			runlifecyclefixture.RequirePostgres(t, testAuthorActivityContext(t, context.Background()), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
			return newPostgresWorkflowInstanceStoreForTest(db), runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), executionmode.Live)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.store(t)
			bundle := workflowJoinLifecycleBundle(t)
			bus := &recordingPipelineBus{}
			schedules := &recordingGenericScheduleWakeupOwner{}
			pc := newWorkflowJoinPipelineCoordinator(bus, store.testDB(), PipelineCoordinatorOptions{
				Module:      &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)},
				Persistence: workflowPersistenceForTest(store), GenericSchedules: schedules,
			})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			now := time.Now().UTC()
			ref, err := timeridentity.NewJoinRef(mustPipelineNode("orders", "join-node"), "item.completed", "awaiting", "awaiting", "")
			if err != nil {
				t.Fatal(err)
			}
			handle, err := timeridentity.JoinTimeoutHandle(ref)
			if err != nil {
				t.Fatal(err)
			}
			activation, err := joinruntime.NewActivation(handle, []string{"a"}, now, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			carrier := runtimeengine.NewStateCarrier(map[string]any{"expected": []any{"a"}}, nil, map[string]map[string]any{})
			if err := joinruntime.Store(carrier.StateBuckets, activation); err != nil {
				t.Fatal(err)
			}
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "awaiting", EnteredStageAt: now, Fields: map[string]any{"entity_id": entityID, "expected": []any{"a"}}, StateBuckets: carrier.PersistedStateBuckets(),
				EntityType: "test_entity"})); err != nil {
				t.Fatal(err)
			}
			handler := pc.SemanticSource().ExecutableNodeEventHandlers(mustPipelineNode("orders", "join-node"))["item.completed"]
			member := eventtest.RunCreatingRootIngress("member-a", events.EventType("item.completed"), "", "", json.RawMessage(`{"member_id":"a","result":{"ok":true}}`), 0, runtimecorrelation.RunIDFromContext(ctx), "", workflowJoinTestEnvelope(path, entityID), now)
			timeout := workflowJoinTimerEventForTest(t, "timeout-a", joinTimeoutEvent, handle, runtimecorrelation.RunIDFromContext(ctx), workflowJoinTestEnvelope(path, entityID), now.Add(time.Hour))
			triggerState := mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID)
			type raceResult struct {
				result contractHandlerExecutionResult
				err    error
			}
			start := make(chan struct{})
			results := make(chan raceResult, 2)
			var wg sync.WaitGroup
			for _, evt := range []events.Event{member, timeout} {
				evt := evt
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					result, err := pc.executeNodeContractHandler(ctx, mustPipelineNode("orders", "join-node"), handler, workflowTriggerContext{Event: evt, State: triggerState, HandlerEventKey: "item.completed"}, false)
					results <- raceResult{result: result, err: err}
				}()
			}
			close(start)
			wg.Wait()
			close(results)
			for result := range results {
				if result.err != nil {
					envelope, ok := runtimefailures.EnvelopeFromError(result.err)
					if !ok || envelope.Class != runtimefailures.ClassStaleArrival {
						t.Fatalf("race error = %v, envelope=%#v", result.err, envelope)
					}
				}
			}
			instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok {
				t.Fatalf("load final instance = %v, %v", ok, err)
			}
			if len(instance.TransitionHistory) != 1 {
				t.Fatalf("persisted transition winners = %d, want 1: %#v", len(instance.TransitionHistory), instance.TransitionHistory)
			}
			finalCarrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			closed, ok, err := joinruntime.Load(finalCarrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey())
			if err != nil || !ok || closed.Status != joinruntime.StatusClosed {
				t.Fatalf("closed activation = %#v, %v, %v", closed, ok, err)
			}
			if closed.CloseReason == joinruntime.CloseReasonComplete && instance.CurrentState != "ready" {
				t.Fatalf("complete close state = %s", instance.CurrentState)
			}
			if closed.CloseReason == joinruntime.CloseReasonTimeout && instance.CurrentState != "attention" {
				t.Fatalf("timeout close state = %s", instance.CurrentState)
			}
			_, cancellations := committedWorkflowSchedulesForTest(t, store)
			if len(cancellations) != 1 || len(schedules.activationIDs) != 1 || schedules.activationIDs[0] != cancellations[0].ID {
				t.Fatalf("join close wakeup reconciliation = ids:%#v cancellations:%#v", schedules.activationIDs, cancellations)
			}
		})
	}
}

func TestWorkflowJoinArmArrivalRaceIsEarlyOrAdmittedOnBothStores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (*workflowInstanceStore, context.Context)
	}{
		{name: "sqlite", store: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			return newSQLiteWorkflowJoinStore(t)
		}},
		{name: "postgres", store: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			runlifecyclefixture.RequirePostgres(t, testAuthorActivityContext(t, context.Background()), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
			return newPostgresWorkflowInstanceStoreForTest(db), runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), executionmode.Live)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.store(t)
			bundle := workflowJoinLifecycleBundle(t)
			bus := &recordingPipelineBus{}
			schedules := &recordingGenericScheduleWakeupOwner{}
			pc := newWorkflowJoinPipelineCoordinator(bus, store.testDB(), PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)}, Persistence: workflowPersistenceForTest(store), GenericSchedules: schedules})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "dispatching", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": []any{"a", "b"}},
				EntityType: "test_entity"})); err != nil {
				t.Fatal(err)
			}
			handler := pc.SemanticSource().ExecutableNodeEventHandlers(mustPipelineNode("orders", "join-node"))["item.completed"]
			arrival := eventtest.RunCreatingRootIngress("member-a", events.EventType("item.completed"), "", "", json.RawMessage(`{"member_id":"a","result":{"ok":true}}`), 0, runtimecorrelation.RunIDFromContext(ctx), "", workflowJoinTestEnvelope(path, entityID), time.Now().UTC())
			triggerState := mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID)
			start := make(chan struct{})
			armErr := make(chan error, 1)
			arrivalErr := make(chan error, 1)
			transitionCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, testWorkflowInstanceRoute(path), entityID, "dispatch.completed")
			go func() {
				<-start
				unlock := pc.lockWorkflowEntity(entityID)
				defer unlock()
				armErr <- pc.persistWorkflowStateForTest(transitionCtx, testWorkflowInstanceRoute(path), entityID, "awaiting", "dispatch.completed")
			}()
			go func() {
				<-start
				_, err := pc.executeNodeContractHandler(ctx, mustPipelineNode("orders", "join-node"), handler, workflowTriggerContext{Event: arrival, State: triggerState, HandlerEventKey: "item.completed"}, false)
				arrivalErr <- err
			}()
			close(start)
			if err := <-armErr; err != nil {
				t.Fatalf("arm: %v", err)
			}
			err := <-arrivalErr
			if err != nil {
				envelope, ok := runtimefailures.EnvelopeFromError(err)
				if !ok || envelope.Class != runtimefailures.ClassEarlyArrival {
					t.Fatalf("arrival error = %v, envelope=%#v", err, envelope)
				}
			}
			instance, ok, loadErr := store.Load(ctx, testWorkflowInstanceRoute(path))
			if loadErr != nil || !ok {
				t.Fatalf("load = %v, %v", ok, loadErr)
			}
			carrier, loadErr := workflowInstanceStateCarrier(instance)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			activation, ok, loadErr := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey())
			if loadErr != nil || !ok || activation.Status != joinruntime.StatusOpen {
				t.Fatalf("activation = %#v, %v, %v", activation, ok, loadErr)
			}
			if activation.Completed() < 0 || activation.Completed() > 1 {
				t.Fatalf("completed = %d, want early 0 or admitted 1", activation.Completed())
			}
			if (err == nil) != (activation.Completed() == 1) {
				t.Fatalf("arrival err=%v completed=%d; want exact early/admitted alternatives", err, activation.Completed())
			}
			upserts, _ := committedWorkflowSchedulesForTest(t, store)
			if len(upserts) != 1 {
				t.Fatalf("schedule intents = %d, want 1", len(upserts))
			}
		})
	}
}

func TestWorkflowJoinPersistedArrivalClassificationOnBothStores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (*workflowInstanceStore, context.Context)
	}{
		{name: "sqlite", store: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			return newSQLiteWorkflowJoinStore(t)
		}},
		{name: "postgres", store: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			runlifecyclefixture.RequirePostgres(t, testAuthorActivityContext(t, context.Background()), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
			return newPostgresWorkflowInstanceStoreForTest(db), runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), executionmode.Live)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.store(t)
			bundle := workflowJoinLifecycleBundle(t)
			schedules := &recordingGenericScheduleWakeupOwner{}
			newCoordinator := func() *PipelineCoordinator {
				return newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)}, Persistence: workflowPersistenceForTest(store), GenericSchedules: schedules})
			}
			pc := newCoordinator()
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "dispatching", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": []any{"a", "b"}},
				EntityType: "test_entity"})); err != nil {
				t.Fatal(err)
			}
			handler := pc.SemanticSource().ExecutableNodeEventHandlers(mustPipelineNode("orders", "join-node"))["item.completed"]
			deliver := func(coordinator *PipelineCoordinator, id, member, result string) error {
				evt := eventtest.RunCreatingRootIngress(id, events.EventType("item.completed"), "", "", mustJSON(map[string]any{"member_id": member, "result": map[string]any{"value": result}}), 0, runtimecorrelation.RunIDFromContext(ctx), "", workflowJoinTestEnvelope(path, entityID), time.Now().UTC())
				_, err := coordinator.executeNodeContractHandler(ctx, mustPipelineNode("orders", "join-node"), handler, workflowTriggerContext{Event: evt, State: mustCurrentWorkflowState(t, coordinator, ctx, testWorkflowInstanceRoute(path), entityID), HandlerEventKey: "item.completed"}, false)
				return err
			}
			assertClass := func(err error, want runtimefailures.Class) {
				t.Helper()
				envelope, ok := runtimefailures.EnvelopeFromError(err)
				if err == nil || !ok || envelope.Class != want {
					t.Fatalf("error = %v, envelope=%#v, want %s", err, envelope, want)
				}
			}

			assertClass(deliver(pc, "early", "a", "one"), runtimefailures.ClassEarlyArrival)
			transitionCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, testWorkflowInstanceRoute(path), entityID, "dispatch.completed")
			if err := pc.persistWorkflowStateForTest(transitionCtx, testWorkflowInstanceRoute(path), entityID, "awaiting", "dispatch.completed"); err != nil {
				t.Fatal(err)
			}
			assertClass(deliver(pc, "unexpected", "c", "other"), runtimefailures.ClassUnexpectedArrival)
			if err := deliver(pc, "a-first", "a", "one"); err != nil {
				t.Fatal(err)
			}
			pc = newCoordinator()
			if err := deliver(pc, "a-exact", "a", "one"); err != nil {
				t.Fatal(err)
			}
			assertClass(deliver(pc, "a-conflict", "a", "changed"), runtimefailures.ClassConflictingDuplicate)
			if err := deliver(pc, "b-complete", "b", "two"); err != nil {
				t.Fatal(err)
			}
			assertClass(deliver(pc, "b-stale", "b", "two"), runtimefailures.ClassStaleArrival)

			instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok {
				t.Fatalf("load = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			closed, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey())
			if err != nil || !ok || closed.Status != joinruntime.StatusClosed || closed.Completed() != 2 {
				t.Fatalf("closed activation = %#v, %v, %v", closed, ok, err)
			}
			results := closed.Results()
			if len(results) != 2 || results[0].(map[string]any)["value"] != "one" || results[1].(map[string]any)["value"] != "two" {
				t.Fatalf("persisted results = %#v, want membership order", results)
			}
			if instance.CurrentState != "ready" || len(instance.TransitionHistory) != 2 {
				t.Fatalf("final lifecycle = state:%s history:%#v", instance.CurrentState, instance.TransitionHistory)
			}
			runner := store.engineMutations.(*recordingRuntimeMutationRunner)
			runner.mu.Lock()
			cancellations := append([]runtimegenericschedule.Activation(nil), runner.committedGenericScheduleCancellations...)
			runner.mu.Unlock()
			if len(cancellations) != 1 || cancellations[0].Command.EventType != joinTimeoutEvent {
				t.Fatalf("closed-operation timeout cancellations = %#v", cancellations)
			}
			if len(schedules.activationIDs) == 0 {
				t.Fatal("committed schedule evidence was not reconciled through the lifecycle owner")
			}
		})
	}
}

func TestWorkflowJoinExpectedZeroCompletesAfterRestartOnBothStores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (*workflowInstanceStore, context.Context)
	}{
		{name: "sqlite", store: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			return newSQLiteWorkflowJoinStore(t)
		}},
		{name: "postgres", store: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			runlifecyclefixture.RequirePostgres(t, testAuthorActivityContext(t, context.Background()), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
			return newPostgresWorkflowInstanceStoreForTest(db), runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), executionmode.Live)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.store(t)
			bundle := workflowJoinLifecycleBundle(t)
			schedules := &recordingGenericScheduleWakeupOwner{}
			newCoordinator := func() *PipelineCoordinator {
				pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)}, Persistence: workflowPersistenceForTest(store), GenericSchedules: schedules})
				configurePipelineTestDeliveryOwner(t, pc)
				return pc
			}
			pc := newCoordinator()
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "dispatching", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": []any{}},
				EntityType: "test_entity"})); err != nil {
				t.Fatal(err)
			}
			dispatchHandler := pc.SemanticSource().ExecutableNodeEventHandlers(mustPipelineNode("orders", "dispatcher"))["order.accepted"]
			dispatch := eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-empty"), events.EventType("order.accepted"), "", "", json.RawMessage(`{"line_items":[]}`), 0, runtimecorrelation.RunIDFromContext(ctx), "", workflowJoinTestEnvelope(path, entityID), time.Now().UTC())
			if dispatchHandler.FanOut == nil {
				t.Fatal("dispatcher fixture lost fan_out")
			}
			dispatchNode := mustPipelineNode("orders", "dispatcher")
			route := seedExactOnceEventDelivery(t, pc, ctx, dispatch, dispatchNode)
			handled, err := pc.executeNodeHandlerPlanResult(withWorkflowNodeDeliveryRoute(ctx, route), dispatchNode, dispatch)
			if err != nil || !handled {
				t.Fatalf("empty fan_out = handled:%v err:%v", handled, err)
			}
			runner := store.engineMutations.(*recordingRuntimeMutationRunner)
			runner.mu.Lock()
			committedSchedules := append([]runtimegenericschedule.Activation(nil), runner.committedGenericScheduleActivations...)
			runner.mu.Unlock()
			if len(committedSchedules) != 1 || committedSchedules[0].Command.EventType != joinCompleteEvent {
				t.Fatalf("closed-operation completion schedules = %#v", committedSchedules)
			}
			armed, ok, err := store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok {
				t.Fatalf("load armed zero join = %v, %v", ok, err)
			}
			armedCarrier, err := workflowInstanceStateCarrier(armed)
			if err != nil {
				t.Fatal(err)
			}
			if err := pc.reconcileClosedJoinSchedules(ctx, testWorkflowInstanceRoute(path), entityID, armedCarrier); err != nil {
				t.Fatalf("reconcile pending zero join: %v", err)
			}
			schedule := committedSchedules[0]
			pc = newCoordinator()
			fire := workflowJoinScheduleEventForTest(t, "join-zero-fire", schedule, runtimecorrelation.RunIDFromContext(ctx), workflowJoinTestEnvelope(path, entityID), time.Now().UTC())
			result, err := pc.executeAuthoritativeNodeHandler(ctx, fire, workflowTriggerContext{Event: fire, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID)})
			if err != nil || !result.Handled {
				t.Fatalf("completion fire = handled:%v err:%v", result.Handled, err)
			}
			if _, err := pc.executeAuthoritativeNodeHandler(ctx, fire, workflowTriggerContext{Event: fire, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID)}); err != nil {
				t.Fatalf("duplicate completion fire: %v", err)
			}
			instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok {
				t.Fatalf("load = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey())
			if err != nil || !ok || !activation.OutcomeFired || activation.OutcomePending || !activation.TimerCancelled {
				t.Fatalf("zero activation = %#v, %v, %v", activation, ok, err)
			}
			if instance.CurrentState != "ready" || len(instance.TransitionHistory) != 2 {
				t.Fatalf("zero completion lifecycle = state:%s history:%#v", instance.CurrentState, instance.TransitionHistory)
			}
			runner.mu.Lock()
			cancellations := append([]runtimegenericschedule.Activation(nil), runner.committedGenericScheduleCancellations...)
			runner.mu.Unlock()
			if len(cancellations) != 1 || cancellations[0].Command.TaskID != schedule.Command.TaskID {
				t.Fatalf("closed-operation expected-zero cancellation = %#v", cancellations)
			}
		})
	}
}

func TestWorkflowJoinExpectedZeroStageExitCancelsPendingCompletionOnBothStores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (*workflowInstanceStore, context.Context)
	}{
		{name: "sqlite", store: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			return newSQLiteWorkflowJoinStore(t)
		}},
		{name: "postgres", store: func(t *testing.T) (*workflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			runlifecyclefixture.RequirePostgres(t, testAuthorActivityContext(t, context.Background()), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
			return newPostgresWorkflowInstanceStoreForTest(db), runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), executionmode.Live)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.store(t)
			bundle := workflowJoinLifecycleBundle(t)
			schedules := &recordingGenericScheduleWakeupOwner{}
			pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
				Module:           &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)},
				Persistence:      workflowPersistenceForTest(store),
				GenericSchedules: schedules,
			})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": []any{}},
				EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			if err := applyTestInitialEntryEffect(ctx, pc, testWorkflowInstanceRoute(path), entityID); err != nil {
				t.Fatalf("arm zero join: %v", err)
			}
			upserts, _ := committedWorkflowSchedulesForTest(t, store)
			if len(upserts) != 1 || upserts[0].Command.EventType != joinCompleteEvent {
				t.Fatalf("completion schedules = %#v", upserts)
			}
			completion := upserts[0]
			transitionCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, testWorkflowInstanceRoute(path), entityID, "manual.abort")
			if err := pc.persistWorkflowStateForTest(transitionCtx, testWorkflowInstanceRoute(path), entityID, "dispatching", "manual.abort"); err != nil {
				t.Fatalf("exit join stage: %v", err)
			}
			_, cancellations := committedWorkflowSchedulesForTest(t, store)
			if len(cancellations) != 1 || cancellations[0].Command.EventType != joinCompleteEvent {
				t.Fatalf("completion cancellations = %#v", cancellations)
			}

			instance, ok, err := store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok {
				t.Fatalf("load exited instance = %v, %v", ok, err)
			}
			carrier, err := workflowInstanceStateCarrier(instance)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, mustPipelineNode("orders", "join-node"), workflowJoinActivationKey())
			if err != nil || !ok || activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonStageExit || activation.OutcomePending || activation.OutcomeFired || !activation.TimerCancelled {
				t.Fatalf("exited zero activation = %#v, %v, %v", activation, ok, err)
			}

			fire := workflowJoinScheduleEventForTest(t, "join-zero-after-exit", completion, runtimecorrelation.RunIDFromContext(ctx), workflowJoinTestEnvelope(path, entityID), time.Now().UTC())
			result, err := pc.executeAuthoritativeNodeHandler(ctx, fire, workflowTriggerContext{Event: fire, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(path), entityID)})
			if err != nil || result.Handled {
				t.Fatalf("late discarded completion fire = handled:%v err:%v, want unhandled", result.Handled, err)
			}
			instance, ok, err = store.Load(ctx, testWorkflowInstanceRoute(path))
			if err != nil || !ok || instance.CurrentState != "dispatching" || len(instance.TransitionHistory) != 1 {
				t.Fatalf("lifecycle after late completion = instance:%#v found:%v err:%v", instance, ok, err)
			}
			_, afterLateCancellations := committedWorkflowSchedulesForTest(t, store)
			if len(afterLateCancellations) != 1 {
				t.Fatalf("late completion repeated cancellation: %#v", afterLateCancellations)
			}
		})
	}
}

func TestWorkflowJoinFailurePersistsCanonicalDeliveryOutcomeAndRuntimeLog(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	ctx := sqliteExactOnceRunContext(t, db)
	bundle := workflowJoinLifecycleBundle(t)
	bus := &recordingPipelineBus{}
	pc := newWorkflowJoinPipelineCoordinator(bus, db, PipelineCoordinatorOptions{
		Module:      &pipelineFixtureWorkflowModule{source: workflowJoinLifecycleSource(bundle)},
		Persistence: workflowPersistenceForTest(store),
	})
	configurePipelineTestDeliveryOwner(t, pc)
	path := "orders/" + uuid.NewString()
	entityID := FlowInstanceEntityID(path)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "dispatching", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"entity_id": entityID, "expected": []any{"a"}},
		EntityType: "test_entity"})); err != nil {
		t.Fatal(err)
	}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("item.completed"), "", "", json.RawMessage(`{"member_id":"a","result":{"ok":true}}`), 0, runtimecorrelation.RunIDFromContext(ctx), "", workflowJoinTestEnvelope(path, entityID), time.Now().UTC())
	joinNode := pipelineNode(t, "orders", "join-node")
	route := seedExactOnceEventDelivery(t, pc, ctx, evt, joinNode)
	if resolved := workflowNodeEventHandlerResolutionForDelivery(pc.SemanticSource(), mustPipelineNode("orders", "join-node"), evt); !resolved.Matched {
		t.Fatalf("join handler did not resolve: %#v", resolved)
	}
	if _, err := store.deliveryStore.ProveHandoff(ctx, evt.ID(), route); err != nil {
		t.Fatalf("seeded join delivery was not authorized: %v", err)
	}
	ctx = withWorkflowNodeDeliveryRoute(ctx, route)
	handled, err := pc.executeNodeHandlerPlanResult(ctx, joinNode, evt)
	if !handled {
		t.Fatal("join failure was not handled")
	}
	envelope, ok := runtimefailures.EnvelopeFromError(err)
	if !ok || envelope.Class != runtimefailures.ClassEarlyArrival {
		t.Fatalf("execution failure = %v, envelope=%#v", err, envelope)
	}
	var status, failureRaw, deliveryOutcome string
	if err := db.QueryRowContext(ctx, `
		SELECT d.status, COALESCE(d.failure, ''), COALESCE(o.outcome, '')
		FROM event_deliveries d
		LEFT JOIN event_delivery_outcomes o ON o.delivery_id = d.delivery_id
		WHERE d.event_id = ? AND d.subscriber_type = 'node' AND d.subscriber_id = ?
	`, evt.ID(), joinNode.Key()).Scan(&status, &failureRaw, &deliveryOutcome); err != nil {
		t.Fatal(err)
	}
	var persisted runtimefailures.Envelope
	if err := json.Unmarshal([]byte(failureRaw), &persisted); err != nil {
		t.Fatalf("decode persisted failure %q: %v", failureRaw, err)
	}
	if status != "dead_letter" || deliveryOutcome != "dead_letter" || persisted.Class != runtimefailures.ClassEarlyArrival {
		t.Fatalf("persisted failure = status:%s outcome:%s failure:%#v", status, deliveryOutcome, persisted)
	}
	logs := bus.runtimeLogEntries()
	if len(logs) == 0 || logs[len(logs)-1].Failure == nil || logs[len(logs)-1].Failure.Class != runtimefailures.ClassEarlyArrival {
		t.Fatalf("runtime logs = %#v", logs)
	}
}

func workflowJoinLifecycleBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return workflowJoinLifecycleBundleWithReview(t, false)
}

func workflowJoinLifecycleBundleWithReview(t *testing.T, review bool) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return workflowJoinLifecycleBundleWithOptions(t, review, "")
}

func workflowJoinLifecycleBundleWithOptions(t *testing.T, review bool, loop string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle := loadWorkflowTempBundle(t, workflowJoinLifecycleFixtureFiles(review, loop))
	// Existing fixture consumers select the child plan for explicit scope tests.
	for i, plan := range bundle.Semantics.Joins {
		if plan.Node.FlowPath() == "orders" {
			bundle.Semantics.Joins[0], bundle.Semantics.Joins[i] = bundle.Semantics.Joins[i], bundle.Semantics.Joins[0]
			break
		}
	}
	return bundle
}

func workflowJoinLifecycleFixtureFiles(review bool, loop string) map[string]string {
	files := map[string]string{
		"schema.yaml":   "name: workflow-join-lifecycle\n",
		"entities.yaml": "test_entity: {}\n",
		"orders/schema.yaml": `name: orders
mode: template
stages:
  dispatching: {}
  awaiting: {initial: true}
  ready: {terminal: true}
  attention: {terminal: true}
`,
		"orders/entities.yaml": "test_entity:\n  expected: list<text>\n",
		"orders/types.yaml":    "types:\n  ItemResult:\n    ok: boolean\n  LineItem:\n    id: text\n",
		"orders/events.yaml": `item.completed:
  member_id: text
  result: ItemResult
order.accepted:
  line_items: list<LineItem>
line_item.requested:
  line_item_id: text
dispatch.completed: {}
manual.abort: {}
`,
		"orders/nodes.yaml": `dispatcher:
  id: dispatcher
  execution_type: system_node
  event_handlers:
    order.accepted:
      fan_out:
        items_from: payload.line_items
        as: line_item
        identity: line_item.id
        emit:
          event: line_item.requested
          fields:
            line_item_id: {cel: line_item.id}
      advances_to: awaiting
    dispatch.completed:
      advances_to: awaiting
    manual.abort:
      advances_to: dispatching
join-node:
  id: join-node
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        id: awaiting
        stage: awaiting
        members: {from: entity.expected, by: payload.member_id}
        output: payload.result
        on_complete: {advances_to: ready}
        timeout: {after: 1h, advances_to: attention}
`,
	}
	if review {
		files["orders/schema.yaml"] = strings.Replace(files["orders/schema.yaml"], "  awaiting: {initial: true}", "  awaiting: {initial: true}\n  reviewing: {}", 1)
		files["orders/events.yaml"] += "review.requested: {}\napproval.completed:\n  member_id: text\n  result: ItemResult\n"
		files["orders/nodes.yaml"] = strings.Replace(files["orders/nodes.yaml"], "    manual.abort:", "    review.requested:\n      advances_to: reviewing\n    manual.abort:", 1)
		files["orders/nodes.yaml"] = strings.Replace(files["orders/nodes.yaml"], "id: awaiting", "id: shared", 1)
		files["orders/nodes.yaml"] += `    approval.completed:
      join:
        id: shared
        stage: reviewing
        members: {from: entity.expected, by: payload.member_id}
        output: payload.result
        on_complete: {advances_to: ready}
        timeout: {after: 1h, advances_to: attention}
`
	}
	if loop != "" {
		files["orders/schema.yaml"] = strings.Replace(files["orders/schema.yaml"], "  ready: {terminal: true}", "  ready: {}", 1)
		files["orders/schema.yaml"] += "loops:\n  revision:\n    revision_field: revision_id\n    max_attempts: 3\n    escape: {advances_to: attention}\n"
		from := "awaiting"
		if loop == "reentrant" {
			from = "ready"
			files["orders/nodes.yaml"] = strings.Replace(files["orders/nodes.yaml"], "    item.completed:\n      join:", "    item.completed:\n      loop: {admit: revision, from: awaiting}\n      join:", 1)
		}
		files["orders/events.yaml"] += "loop.start: {}\nloop.repeat:\n  revision_id: text\n"
		files["orders/events.yaml"] = strings.Replace(files["orders/events.yaml"], "item.completed:\n", "item.completed:\n  revision_id: text\n", 1)
		files["orders/nodes.yaml"] += "observer:\n  id: observer\n  execution_type: system_node\n  event_handlers:\n    loop.start:\n      loop: {start: revision, from: dispatching}\n      advances_to: awaiting\n    loop.repeat:\n      loop: {repeat: revision, from: " + from + "}\n      advances_to: awaiting\n"
	}
	// Root and child executions use independently compiled declarations, even
	// when the fixture deliberately gives them identical local names.
	for _, name := range []string{"schema.yaml", "entities.yaml", "events.yaml", "types.yaml", "nodes.yaml"} {
		files[name] = files["orders/"+name]
	}
	files["schema.yaml"] = strings.Replace(files["schema.yaml"], "name: orders\nmode: template", "name: workflow-join-lifecycle", 1)
	return files
}

func workflowJoinActivationKey() string {
	return joinruntime.ActivationKey("awaiting", "awaiting", "")
}

func workflowJoinTestEnvelope(instancePath, entityID string) events.EventEnvelope {
	return events.EnvelopeForSourceRoute(
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), instancePath),
		events.RouteIdentity{FlowID: "orders", FlowInstance: instancePath, EntityID: entityID},
	)
}
