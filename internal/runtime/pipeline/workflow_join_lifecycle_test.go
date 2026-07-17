package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

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
			schedules := &recordingSchedulePersistence{}
			bundle := workflowJoinLifecycleBundle()
			pc := &PipelineCoordinator{
				module:             &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)},
				workflowStore:      store,
				timerScheduleStore: schedules,
			}
			entityID := FlowInstanceEntityID("orders/order-1")
			runID := uuid.NewString()
			ensurePipelineTestRun(t, store, runID)
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			if err := store.Upsert(ctx, WorkflowInstance{
				InstanceID: "order-1", StorageRef: "orders/order-1", WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "expected": tc.members},
			}); err != nil {
				t.Fatal(err)
			}
			if err := pc.armWorkflowCurrentStageLifecycle(ctx, entityID, "state:awaiting"); err != nil {
				t.Fatalf("arm join: %v", err)
			}
			instance, ok, err := store.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("load instance = %v, %v", ok, err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, "join-node", workflowJoinActivationKey())
			if err != nil || !ok {
				t.Fatalf("load activation = %#v, %v, %v", activation, ok, err)
			}
			if activation.Status != tc.wantStatus || activation.CloseReason != tc.wantReason {
				t.Fatalf("activation status = %s/%s, want %s/%s", activation.Status, activation.CloseReason, tc.wantStatus, tc.wantReason)
			}
			if got := len(schedules.schedules); got != 1 {
				t.Fatalf("schedules = %d, want 1", got)
			}
			schedule := schedules.schedules[0]
			if schedule.EventType != tc.wantEvent || schedule.EntityID != entityID {
				t.Fatalf("schedule = %#v", schedule)
			}
			handle, ok := timeridentity.ParseTimerHandle(parsePayloadMap(schedule.Payload))
			if !ok || handle.Kind != tc.wantKind || handle.Join.JoinID != "awaiting" {
				t.Fatalf("timer handle = %#v, %v", handle, ok)
			}
			if len(schedules.upsertTx) != 1 || !schedules.upsertTx[0] {
				t.Fatalf("schedule persistence transaction flags = %#v, want [true]", schedules.upsertTx)
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
			if _, err := db.ExecContext(testAuthorActivityContext(context.Background()), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
				t.Fatal(err)
			}
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			store := NewWorkflowInstanceStore(db)
			schedules := &recordingSchedulePersistence{}
			pc := &PipelineCoordinator{module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(workflowJoinLifecycleBundle())}, workflowStore: store, timerScheduleStore: schedules}
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.Upsert(ctx, WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "expected": tc.members}}); err != nil {
				t.Fatal(err)
			}
			if err := pc.armWorkflowCurrentStageLifecycle(ctx, entityID, "state:awaiting"); err != nil {
				t.Fatal(err)
			}
			instance, ok, err := store.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("load = %v, %v", ok, err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, "join-node", workflowJoinActivationKey())
			if err != nil || !ok || activation.Status != tc.wantStatus {
				t.Fatalf("activation = %#v, %v, %v", activation, ok, err)
			}
			if len(schedules.schedules) != 1 || schedules.schedules[0].EventType != tc.wantEvent || len(schedules.upsertTx) != 1 || !schedules.upsertTx[0] {
				t.Fatalf("schedule parity = schedules:%#v tx:%#v", schedules.schedules, schedules.upsertTx)
			}
		})
	}
}

func TestWorkflowJoinCustomCompletionControlsExpectedZeroOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bundle := workflowJoinLifecycleBundle()
			node := bundle.Nodes["join-node"]
			handler := node.EventHandlers["item.completed"]
			spec := *handler.Join
			spec.CompleteWhen = "join.completed >= 1"
			spec.Remaining = runtimecontracts.JoinRemainingIgnore
			handler.Join = &spec
			node.EventHandlers["item.completed"] = handler
			bundle.Nodes["join-node"] = node
			bundle.Semantics.Joins[0].Spec = spec
			bundle.Semantics.NodeHandlers["join-node"] = node.EventHandlers

			schedules := &recordingSchedulePersistence{}
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{
				Module:             &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)},
				WorkflowStore:      store,
				TimerScheduleStore: schedules,
			})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.Upsert(ctx, WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "expected": []any{}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := pc.armWorkflowCurrentStageLifecycle(ctx, entityID, "state:awaiting"); err != nil {
				t.Fatalf("arm custom join: %v", err)
			}
			instance, ok, err := store.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("load custom join = %v, %v", ok, err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, "join-node", workflowJoinActivationKey())
			if err != nil || !ok || activation.Status != joinruntime.StatusOpen || activation.CloseReason != "" {
				t.Fatalf("custom zero activation = %#v, %v, %v, want open", activation, ok, err)
			}
			if len(schedules.schedules) != 1 || schedules.schedules[0].EventType != joinTimeoutEvent {
				t.Fatalf("custom zero schedules = %#v, want timeout", schedules.schedules)
			}
		})
	}
}

func TestWorkflowJoinArmRejectsCatalogInvalidNamedResultExpression(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	bundle := workflowJoinLifecycleBundle()
	node := bundle.Nodes["join-node"]
	handler := node.EventHandlers["item.completed"]
	spec := *handler.Join
	spec.CompleteWhen = "join.results[0] > 1"
	spec.Remaining = runtimecontracts.JoinRemainingIgnore
	handler.Join = &spec
	node.EventHandlers["item.completed"] = handler
	bundle.Nodes["join-node"] = node
	bundle.Semantics.Joins[0].Spec = spec
	bundle.Semantics.Joins[0].ResultType = runtimecontracts.CatalogTypeReference{
		Type: "JoinResult",
		Catalog: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"JoinResult": {Fields: map[string]runtimecontracts.TypeFieldSpec{"value": {Type: "text"}}},
		}},
	}

	pc := &PipelineCoordinator{
		module:        &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)},
		workflowStore: store,
	}
	entityID := FlowInstanceEntityID("orders/order-typed")
	runID := uuid.NewString()
	ensurePipelineTestRun(t, store, runID)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID: "order-typed", StorageRef: "orders/order-typed", WorkflowName: "orders", WorkflowVersion: "1.0.0",
		CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "expected": []any{}},
	}); err != nil {
		t.Fatal(err)
	}
	err := pc.armWorkflowCurrentStageLifecycle(ctx, entityID, "state:awaiting")
	if err == nil || !strings.Contains(err.Error(), "no matching overload") {
		t.Fatalf("arm join error = %v, want catalog-backed typed rejection", err)
	}
}

func TestWorkflowJoinDurableIdentityIncludesStageOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bundle := workflowJoinLifecycleBundle()
			node := bundle.Nodes["join-node"]
			first := *node.EventHandlers["item.completed"].Join
			first.ID = "shared"
			first.Stage = "awaiting"
			second := first
			second.Stage = "reviewing"
			node.EventHandlers["item.completed"] = runtimecontracts.SystemNodeEventHandler{Join: &first}
			node.EventHandlers["approval.completed"] = runtimecontracts.SystemNodeEventHandler{Join: &second}
			bundle.Nodes["join-node"] = node
			bundle.Events["approval.completed"] = bundle.Events["item.completed"]
			bundle.RootSchema.StageDeclarations.Entries = append(bundle.RootSchema.StageDeclarations.Entries, runtimecontracts.FlowStageDeclaration{ID: "reviewing"})
			bundle.Semantics.Stages = append(bundle.Semantics.Stages, runtimecontracts.WorkflowStageContract{ID: "reviewing"})
			resultType := runtimecontracts.CatalogTypeReference{Type: "jsonb"}
			bundle.Semantics.Joins = []runtimecontracts.WorkflowJoinPlan{
				{FlowID: "", NodeID: "join-node", HandlerEvent: "item.completed", Spec: first, ResultType: resultType},
				{FlowID: "", NodeID: "join-node", HandlerEvent: "approval.completed", Spec: second, ResultType: resultType},
			}
			bundle.Semantics.NodeHandlers["join-node"] = node.EventHandlers
			bundle.Semantics.EffectiveNodes["join-node"] = runtimecontracts.SystemNodeEffectiveSemantics{ID: "join-node", RuntimeSubscriptions: runtimecontracts.EffectiveSystemNodeSubscriptions(node)}

			schedules := &recordingSchedulePersistence{}
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{
				Module:             &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)},
				WorkflowStore:      store,
				TimerScheduleStore: schedules,
			})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.Upsert(ctx, WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "expected": []any{"a"}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := pc.armWorkflowCurrentStageLifecycle(ctx, entityID, "state:awaiting"); err != nil {
				t.Fatal(err)
			}
			if err := pc.applyWorkflowJoinIntents(ctx, entityID, "awaiting", "reviewing"); err != nil {
				t.Fatal(err)
			}
			instance, ok, err := store.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("load stage-scoped joins = %v, %v", ok, err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			awaiting, ok, err := joinruntime.Load(carrier.StateBuckets, "join-node", joinruntime.ActivationKey("awaiting", "shared", ""))
			if err != nil || !ok || awaiting.CloseReason != joinruntime.CloseReasonStageExit {
				t.Fatalf("awaiting activation = %#v, %v, %v", awaiting, ok, err)
			}
			reviewing, ok, err := joinruntime.Load(carrier.StateBuckets, "join-node", joinruntime.ActivationKey("reviewing", "shared", ""))
			if err != nil || !ok || reviewing.Status != joinruntime.StatusOpen || reviewing.Stage != "reviewing" {
				t.Fatalf("reviewing activation = %#v, %v, %v", reviewing, ok, err)
			}
			if awaiting.Key() == reviewing.Key() || len(schedules.schedules) != 2 {
				t.Fatalf("stage identities/schedules = awaiting:%q reviewing:%q schedules:%#v", awaiting.Key(), reviewing.Key(), schedules.schedules)
			}
		})
	}
}

type workflowJoinStoreCase struct {
	name string
	open func(*testing.T) (*WorkflowInstanceStore, context.Context)
}

func workflowJoinStoreCases() []workflowJoinStoreCase {
	return []workflowJoinStoreCase{
		{name: "sqlite", open: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			store, ctx := newSQLiteWorkflowJoinStore(t)
			return store, runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
		}},
		{name: "postgres", open: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			if _, err := db.ExecContext(testAuthorActivityContext(context.Background()), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
				t.Fatal(err)
			}
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			return NewWorkflowInstanceStore(db), runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
		}},
	}
}

func newSQLiteWorkflowJoinStore(t *testing.T) (*WorkflowInstanceStore, context.Context) {
	t.Helper()
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	runID := uuid.NewString()
	ensurePipelineTestRun(t, store, runID)
	return store, runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
}

func TestWorkflowJoinArrivalTimeoutRaceHasOneCloseWinnerOnBothStores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (*WorkflowInstanceStore, context.Context)
	}{
		{name: "sqlite", store: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			return newSQLiteWorkflowJoinStore(t)
		}},
		{name: "postgres", store: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			if _, err := db.ExecContext(testAuthorActivityContext(context.Background()), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
				t.Fatal(err)
			}
			return NewWorkflowInstanceStore(db), runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.store(t)
			bundle := workflowJoinLifecycleBundle()
			bus := &recordingPipelineBus{}
			pc := NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, WorkflowStore: store})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			now := time.Now().UTC()
			activation, err := joinruntime.NewActivation("awaiting", "awaiting", "join-node", "item.completed", "", []string{"a"}, now, now.Add(time.Hour), "join-timeout", joinTimeoutEvent)
			if err != nil {
				t.Fatal(err)
			}
			carrier := runtimeengine.NewStateCarrier(map[string]any{"expected": []any{"a"}}, nil, map[string]map[string]any{})
			if err := joinruntime.Store(carrier.StateBuckets, activation); err != nil {
				t.Fatal(err)
			}
			if err := store.Upsert(ctx, WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "awaiting", EnteredStageAt: now, Metadata: map[string]any{"entity_id": entityID, "expected": []any{"a"}}, StateBuckets: carrier.PersistedStateBuckets()}); err != nil {
				t.Fatal(err)
			}
			handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]
			member := eventtest.RootIngress("member-a", events.EventType("item.completed"), "", "", json.RawMessage(`{"member_id":"a","result":{"ok":true}}`), 0, runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), now)
			ref := timeridentity.NewJoinRef("join-node", "item.completed", "awaiting", "awaiting", "")
			handle := timeridentity.JoinTimeoutHandle(ref)
			timeout := eventtest.RootIngress("timeout-a", events.EventType(joinTimeoutEvent), "", handle.TaskID(), mustJSON(handle.PayloadMetadata()), 0, runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), now.Add(time.Hour))
			triggerState := pc.currentWorkflowState(ctx, entityID)
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
					result, err := pc.executeNodeContractHandler(ctx, "join-node", handler, workflowTriggerContext{Event: evt, State: triggerState, HandlerEventKey: "item.completed"}, false)
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
			instance, ok, err := store.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("load final instance = %v, %v", ok, err)
			}
			if len(instance.TransitionHistory) != 1 {
				t.Fatalf("persisted transition winners = %d, want 1: %#v", len(instance.TransitionHistory), instance.TransitionHistory)
			}
			finalCarrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			closed, ok, err := joinruntime.Load(finalCarrier.StateBuckets, "join-node", workflowJoinActivationKey())
			if err != nil || !ok || closed.Status != joinruntime.StatusClosed {
				t.Fatalf("closed activation = %#v, %v, %v", closed, ok, err)
			}
			if closed.CloseReason == joinruntime.CloseReasonComplete && instance.CurrentState != "ready" {
				t.Fatalf("complete close state = %s", instance.CurrentState)
			}
			if closed.CloseReason == joinruntime.CloseReasonTimeout && instance.CurrentState != "attention" {
				t.Fatalf("timeout close state = %s", instance.CurrentState)
			}
		})
	}
}

func TestWorkflowJoinArmArrivalRaceIsEarlyOrAdmittedOnBothStores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (*WorkflowInstanceStore, context.Context)
	}{
		{name: "sqlite", store: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			return newSQLiteWorkflowJoinStore(t)
		}},
		{name: "postgres", store: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			if _, err := db.ExecContext(testAuthorActivityContext(context.Background()), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
				t.Fatal(err)
			}
			return NewWorkflowInstanceStore(db), runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.store(t)
			bundle := workflowJoinLifecycleBundle()
			bus := &recordingPipelineBus{}
			schedules := &recordingSchedulePersistence{}
			pc := NewPipelineCoordinatorWithOptions(bus, store.db, PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, WorkflowStore: store, TimerScheduleStore: schedules})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.Upsert(ctx, WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "dispatching", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "expected": []any{"a", "b"}}}); err != nil {
				t.Fatal(err)
			}
			handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]
			arrival := eventtest.RootIngress("member-a", events.EventType("item.completed"), "", "", json.RawMessage(`{"member_id":"a","result":{"ok":true}}`), 0, runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
			triggerState := pc.currentWorkflowState(ctx, entityID)
			start := make(chan struct{})
			armErr := make(chan error, 1)
			arrivalErr := make(chan error, 1)
			transitionCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, entityID, "dispatch.completed")
			go func() {
				<-start
				unlock := pc.lockWorkflowEntity(entityID)
				defer unlock()
				armErr <- pc.updateEntityState(transitionCtx, entityID, "awaiting", "dispatch.completed")
			}()
			go func() {
				<-start
				_, err := pc.executeNodeContractHandler(ctx, "join-node", handler, workflowTriggerContext{Event: arrival, State: triggerState, HandlerEventKey: "item.completed"}, false)
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
			instance, ok, loadErr := store.Load(ctx, entityID)
			if loadErr != nil || !ok {
				t.Fatalf("load = %v, %v", ok, loadErr)
			}
			carrier, loadErr := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			activation, ok, loadErr := joinruntime.Load(carrier.StateBuckets, "join-node", workflowJoinActivationKey())
			if loadErr != nil || !ok || activation.Status != joinruntime.StatusOpen {
				t.Fatalf("activation = %#v, %v, %v", activation, ok, loadErr)
			}
			if activation.Completed() < 0 || activation.Completed() > 1 {
				t.Fatalf("completed = %d, want early 0 or admitted 1", activation.Completed())
			}
			if (err == nil) != (activation.Completed() == 1) {
				t.Fatalf("arrival err=%v completed=%d; want exact early/admitted alternatives", err, activation.Completed())
			}
			if len(schedules.schedules) != 1 {
				t.Fatalf("schedule intents = %d, want 1", len(schedules.schedules))
			}
		})
	}
}

func TestWorkflowJoinPersistedArrivalClassificationOnBothStores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (*WorkflowInstanceStore, context.Context)
	}{
		{name: "sqlite", store: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			return newSQLiteWorkflowJoinStore(t)
		}},
		{name: "postgres", store: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			if _, err := db.ExecContext(testAuthorActivityContext(context.Background()), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
				t.Fatal(err)
			}
			return NewWorkflowInstanceStore(db), runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.store(t)
			bundle := workflowJoinLifecycleBundle()
			schedules := &recordingSchedulePersistence{}
			newCoordinator := func() *PipelineCoordinator {
				return NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, WorkflowStore: store, TimerScheduleStore: schedules})
			}
			pc := newCoordinator()
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.Upsert(ctx, WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "dispatching", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "expected": []any{"a", "b"}}}); err != nil {
				t.Fatal(err)
			}
			handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]
			deliver := func(coordinator *PipelineCoordinator, id, member, result string) error {
				evt := eventtest.RootIngress(id, events.EventType("item.completed"), "", "", mustJSON(map[string]any{"member_id": member, "result": map[string]any{"value": result}}), 0, runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
				_, err := coordinator.executeNodeContractHandler(ctx, "join-node", handler, workflowTriggerContext{Event: evt, State: coordinator.currentWorkflowState(ctx, entityID), HandlerEventKey: "item.completed"}, false)
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
			transitionCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, entityID, "dispatch.completed")
			if err := pc.updateEntityState(transitionCtx, entityID, "awaiting", "dispatch.completed"); err != nil {
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

			instance, ok, err := store.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("load = %v, %v", ok, err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			closed, ok, err := joinruntime.Load(carrier.StateBuckets, "join-node", workflowJoinActivationKey())
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
			if schedules.cancelOwned != 1 || len(schedules.cancelTx) != 1 || !schedules.cancelTx[0] {
				t.Fatalf("timeout cancellation = count:%d tx:%#v", schedules.cancelOwned, schedules.cancelTx)
			}
		})
	}
}

func TestWorkflowJoinExpectedZeroCompletesAfterRestartOnBothStores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (*WorkflowInstanceStore, context.Context)
	}{
		{name: "sqlite", store: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			return newSQLiteWorkflowJoinStore(t)
		}},
		{name: "postgres", store: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			if _, err := db.ExecContext(testAuthorActivityContext(context.Background()), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
				t.Fatal(err)
			}
			return NewWorkflowInstanceStore(db), runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.store(t)
			bundle := workflowJoinLifecycleBundle()
			schedules := &recordingSchedulePersistence{}
			newCoordinator := func() *PipelineCoordinator {
				return NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, WorkflowStore: store, TimerScheduleStore: schedules})
			}
			pc := newCoordinator()
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.Upsert(ctx, WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "dispatching", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "expected": []any{}}}); err != nil {
				t.Fatal(err)
			}
			dispatchHandler := bundle.Nodes["dispatcher"].EventHandlers["order.accepted"]
			dispatch := eventtest.RootIngress("fan-out-empty", events.EventType("order.accepted"), "", "", json.RawMessage(`{"line_items":[]}`), 0, runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
			result, err := pc.executeNodeContractHandler(ctx, "dispatcher", dispatchHandler, workflowTriggerContext{Event: dispatch, State: pc.currentWorkflowState(ctx, entityID), HandlerEventKey: "order.accepted"}, false)
			if err != nil || !result.Handled || result.Outcome == nil || result.Outcome.FanOutCount != 0 {
				t.Fatalf("empty fan_out = handled:%v outcome:%#v err:%v", result.Handled, result.Outcome, err)
			}
			if len(schedules.schedules) != 1 || schedules.schedules[0].EventType != joinCompleteEvent {
				t.Fatalf("completion schedules = %#v", schedules.schedules)
			}
			armed, ok, err := store.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("load armed zero join = %v, %v", ok, err)
			}
			armedCarrier, err := runtimeengine.StateCarrierFromPersisted(armed.Metadata, armed.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			if err := pc.reconcileClosedJoinSchedules(ctx, entityID, armedCarrier); err != nil {
				t.Fatalf("reconcile pending zero join: %v", err)
			}
			if schedules.cancelOwned != 0 {
				t.Fatalf("pending expected-zero completion was canceled before fire: %#v", schedules.cancels)
			}
			schedule := schedules.schedules[0]
			pc = newCoordinator()
			fire := eventtest.RootIngress("join-zero-fire", events.EventType(schedule.EventType), "", schedule.TaskID, schedule.Payload, 0, runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
			result, err = pc.executeAuthoritativeNodeHandler(ctx, fire, workflowTriggerContext{Event: fire, State: pc.currentWorkflowState(ctx, entityID)})
			if err != nil || !result.Handled {
				t.Fatalf("completion fire = handled:%v err:%v", result.Handled, err)
			}
			if _, err := pc.executeAuthoritativeNodeHandler(ctx, fire, workflowTriggerContext{Event: fire, State: pc.currentWorkflowState(ctx, entityID)}); err != nil {
				t.Fatalf("duplicate completion fire: %v", err)
			}
			instance, ok, err := store.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("load = %v, %v", ok, err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, "join-node", workflowJoinActivationKey())
			if err != nil || !ok || !activation.OutcomeFired || activation.OutcomePending || !activation.TimerCancelled {
				t.Fatalf("zero activation = %#v, %v, %v", activation, ok, err)
			}
			if instance.CurrentState != "ready" || len(instance.TransitionHistory) != 2 {
				t.Fatalf("zero completion lifecycle = state:%s history:%#v", instance.CurrentState, instance.TransitionHistory)
			}
			if schedules.cancelOwned != 1 {
				t.Fatalf("fired expected-zero completion cancellation count = %d, want 1", schedules.cancelOwned)
			}
		})
	}
}

func TestWorkflowJoinExpectedZeroStageExitCancelsPendingCompletionOnBothStores(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (*WorkflowInstanceStore, context.Context)
	}{
		{name: "sqlite", store: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			return newSQLiteWorkflowJoinStore(t)
		}},
		{name: "postgres", store: func(t *testing.T) (*WorkflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			runID := uuid.NewString()
			if _, err := db.ExecContext(testAuthorActivityContext(context.Background()), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
				t.Fatal(err)
			}
			return NewWorkflowInstanceStore(db), runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.store(t)
			bundle := workflowJoinLifecycleBundle()
			schedules := &recordingSchedulePersistence{}
			pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, store.db, PipelineCoordinatorOptions{
				Module:             &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)},
				WorkflowStore:      store,
				TimerScheduleStore: schedules,
			})
			path := "orders/" + uuid.NewString()
			entityID := FlowInstanceEntityID(path)
			if err := store.Upsert(ctx, WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0",
				CurrentState: "awaiting", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "expected": []any{}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := pc.armWorkflowCurrentStageLifecycle(ctx, entityID, "state:awaiting"); err != nil {
				t.Fatalf("arm zero join: %v", err)
			}
			if len(schedules.schedules) != 1 || schedules.schedules[0].EventType != joinCompleteEvent {
				t.Fatalf("completion schedules = %#v", schedules.schedules)
			}
			completion := schedules.schedules[0]
			transitionCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, entityID, "manual.abort")
			if err := pc.updateEntityState(transitionCtx, entityID, "dispatching", "manual.abort"); err != nil {
				t.Fatalf("exit join stage: %v", err)
			}
			if schedules.cancelOwned != 1 || len(schedules.cancels) != 1 || schedules.cancels[0].EventType != joinCompleteEvent || len(schedules.cancelTx) != 1 || !schedules.cancelTx[0] {
				t.Fatalf("completion cancellation = count:%d cancels:%#v tx:%#v", schedules.cancelOwned, schedules.cancels, schedules.cancelTx)
			}

			instance, ok, err := store.Load(ctx, entityID)
			if err != nil || !ok {
				t.Fatalf("load exited instance = %v, %v", ok, err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			activation, ok, err := joinruntime.Load(carrier.StateBuckets, "join-node", workflowJoinActivationKey())
			if err != nil || !ok || activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonStageExit || activation.OutcomePending || activation.OutcomeFired || !activation.TimerCancelled {
				t.Fatalf("exited zero activation = %#v, %v, %v", activation, ok, err)
			}

			fire := eventtest.RootIngress("join-zero-after-exit", events.EventType(completion.EventType), "", completion.TaskID, completion.Payload, 0, runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
			result, err := pc.executeAuthoritativeNodeHandler(ctx, fire, workflowTriggerContext{Event: fire, State: pc.currentWorkflowState(ctx, entityID)})
			if err != nil || result.Handled {
				t.Fatalf("late discarded completion fire = handled:%v err:%v, want unhandled", result.Handled, err)
			}
			instance, ok, err = store.Load(ctx, entityID)
			if err != nil || !ok || instance.CurrentState != "dispatching" || len(instance.TransitionHistory) != 1 {
				t.Fatalf("lifecycle after late completion = instance:%#v found:%v err:%v", instance, ok, err)
			}
			if schedules.cancelOwned != 1 {
				t.Fatalf("late completion repeated cancellation: %d", schedules.cancelOwned)
			}
		})
	}
}

func TestWorkflowJoinFailurePersistsCanonicalReceiptAndRuntimeLog(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	ctx := sqliteExactOnceRunContext(t, db)
	bundle := workflowJoinLifecycleBundle()
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		Module:        &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)},
		WorkflowStore: store,
	})
	path := "orders/" + uuid.NewString()
	entityID := FlowInstanceEntityID(path)
	if err := store.Upsert(ctx, WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1.0.0", CurrentState: "dispatching", EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID, "expected": []any{"a"}}}); err != nil {
		t.Fatal(err)
	}
	evt := eventtest.RootIngress(uuid.NewString(), events.EventType("item.completed"), "", "", json.RawMessage(`{"member_id":"a","result":{"ok":true}}`), 0, runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
	seedExactOnceEventDelivery(t, store, ctx, evt, "join-node")
	if resolved := workflowNodeEventHandlerResolutionForDelivery(pc.SemanticSource(), "join-node", evt); !resolved.Matched {
		t.Fatalf("join handler did not resolve: %#v", resolved)
	}
	if !pc.workflowNodeDeliveryAuthorized(ctx, "join-node", evt) {
		t.Fatal("seeded join delivery was not authorized")
	}
	handled, err := pc.executeNodeHandlerPlanResult(ctx, "join-node", evt)
	if !handled {
		t.Fatal("join failure was not handled")
	}
	envelope, ok := runtimefailures.EnvelopeFromError(err)
	if !ok || envelope.Class != runtimefailures.ClassEarlyArrival {
		t.Fatalf("execution failure = %v, envelope=%#v", err, envelope)
	}
	var status, failureRaw, receiptOutcome string
	if err := db.QueryRowContext(ctx, `
		SELECT d.status, COALESCE(d.failure, ''), COALESCE(r.outcome, '')
		FROM event_deliveries d
		LEFT JOIN event_receipts r ON r.event_id = d.event_id AND r.subscriber_type = d.subscriber_type AND r.subscriber_id = d.subscriber_id
		WHERE d.event_id = ? AND d.subscriber_type = 'node' AND d.subscriber_id = 'join-node'
	`, evt.ID()).Scan(&status, &failureRaw, &receiptOutcome); err != nil {
		t.Fatal(err)
	}
	var persisted runtimefailures.Envelope
	if err := json.Unmarshal([]byte(failureRaw), &persisted); err != nil {
		t.Fatalf("decode persisted failure %q: %v", failureRaw, err)
	}
	if status != "dead_letter" || receiptOutcome != "dead_letter" || persisted.Class != runtimefailures.ClassEarlyArrival {
		t.Fatalf("persisted failure = status:%s receipt:%s failure:%#v", status, receiptOutcome, persisted)
	}
	logs := bus.runtimeLogEntries()
	if len(logs) == 0 || logs[len(logs)-1].Failure == nil || logs[len(logs)-1].Failure.Class != runtimefailures.ClassEarlyArrival {
		t.Fatalf("runtime logs = %#v", logs)
	}
}

func workflowJoinLifecycleBundle() *runtimecontracts.WorkflowContractBundle {
	resultType := runtimecontracts.CatalogTypeReference{Type: "jsonb"}
	spec := runtimecontracts.JoinSpec{
		ID: "awaiting", Stage: "awaiting",
		Members: runtimecontracts.JoinMembersSpec{From: "entity.expected", FromPath: runtimepaths.Parse("entity.expected"), By: "payload.member_id", ByPath: runtimepaths.Parse("payload.member_id")},
		Output:  "payload.result", OutputPath: runtimepaths.Parse("payload.result"), OnComplete: runtimecontracts.HandlerRuleEntry{AdvancesTo: "ready"}, OnCompleteFound: true,
		Timeout: runtimecontracts.JoinTimeoutSpec{After: "1h", Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "attention"}}, TimeoutFound: true,
	}
	fanOut := runtimecontracts.FanOutSpec{
		ItemsFrom: "payload.line_items", ItemsPath: runtimepaths.Parse("payload.line_items"), As: "line_item", Identity: "line_item.id",
		Emit: runtimecontracts.EmitSpec{Event: "line_item.requested", Fields: map[string]runtimecontracts.ExpressionValue{"line_item_id": runtimecontracts.CELExpression("line_item.id")}},
	}
	joinNode := runtimecontracts.SystemNodeContract{EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
		"item.completed": {Join: &spec},
	}}
	dispatcher := runtimecontracts.SystemNodeContract{EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
		"order.accepted": {FanOut: &fanOut, AdvancesTo: "awaiting"},
	}}
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{{ID: "dispatching", Initial: true}, {ID: "awaiting"}, {ID: "ready", Terminal: true}, {ID: "attention", Terminal: true}}}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"join-node":  joinNode,
			"dispatcher": dispatcher,
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.completed":      {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"member_id": {Type: "text"}, "result": {Type: "jsonb"}}}},
			"order.accepted":      {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"line_items": {Type: "list<jsonb>"}}}},
			"line_item.requested": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"line_item_id": {Type: "text"}}}},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "orders", Version: "1.0.0", InitialStage: "dispatching", Stages: []runtimecontracts.WorkflowStageContract{{ID: "dispatching"}, {ID: "awaiting"}, {ID: "ready"}, {ID: "attention"}}, TerminalStages: []string{"ready", "attention"},
			Joins: []runtimecontracts.WorkflowJoinPlan{{FlowID: "", NodeID: "join-node", HandlerEvent: "item.completed", Spec: spec, ResultType: resultType}},
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				"join-node":  {ID: "join-node", RuntimeSubscriptions: runtimecontracts.EffectiveSystemNodeSubscriptions(joinNode)},
				"dispatcher": {ID: "dispatcher", RuntimeSubscriptions: runtimecontracts.EffectiveSystemNodeSubscriptions(dispatcher)},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"join-node":  joinNode.EventHandlers,
				"dispatcher": dispatcher.EventHandlers,
			},
			EventOwners: map[string][]string{
				"item.completed": {"join-node"},
				"order.accepted": {"dispatcher"},
			},
		},
	}
}

func workflowJoinActivationKey() string {
	return joinruntime.ActivationKey("awaiting", "awaiting", "")
}
