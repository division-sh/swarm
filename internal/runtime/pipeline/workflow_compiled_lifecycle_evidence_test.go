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
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func VerifyNativePipelineCompiledTimerTransitionEvidenceOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, operation := range []string{"advance", "emit and advance", "emit only", "self advance", "emit and self advance", "loop self advance", "loop emit and self advance"} {
			t.Run(storeCase.name+"/"+operation, func(t *testing.T) {
				timer := "        advances_to: done\n"
				loopOwned := strings.HasPrefix(operation, "loop ")
				plainOperation := strings.TrimPrefix(operation, "loop ")
				if plainOperation == "self advance" {
					timer = "        advances_to: waiting\n"
				} else if plainOperation == "emit and self advance" {
					timer = "        advances_to: waiting\n        emit: review.expired\n"
				} else if operation == "emit only" {
					timer = "        emit: review.expired\n"
				} else if operation == "emit and advance" {
					timer += "        emit: review.expired\n"
				}
				files := map[string]string{
					"schema.yaml":   "name: timer-evidence\nstages:\n  waiting:\n    timers:\n      - after: 1h\n" + timer + "  done: {final: true}\n",
					"entities.yaml": "test_entity: {}\n",
					"events.yaml":   "review.expired:\n",
				}
				if loopOwned {
					files["schema.yaml"] += "  ready: {}\n  escaped: {}\nloops:\n  revision:\n    revision_field: revision_id\n    max_attempts: 3\n    escape: {advances_to: escaped}\n"
					files["events.yaml"] += "loop.start:\nloop.repeat:\n  revision_id: text\n"
					files["nodes.yaml"] = "owner:\n  execution_type: system_node\n  event_handlers:\n    loop.start:\n      loop: {start: revision, from: ready}\n      advances_to: waiting\n    loop.repeat:\n      loop: {repeat: revision, from: waiting}\n      advances_to: waiting\n"
				}
				bundle := loadWorkflowTempBundle(t, files)
				fixture, pc, ctx := nativePilotPipelineForTest(t, storeCase.name, bundle, open)
				store := pc.workflowStore
				bus := observeNativePipelineDeliveryBusForTest(t, pc)
				pc.workflowTimers.publication, pc.workflowTimers.dispatcher, pc.workflowTimers.logger = bus, bus.EngineDispatcher(), bus
				pc.timerScheduler = newWorkflowTimerTestScheduler(t, pc.workOwner)
				if err := pc.timerScheduler.PrepareStartup(); err != nil {
					t.Fatal(err)
				}
				if err := pc.workflowTimers.bindScheduler(pc.timerScheduler); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
						t.Error(err)
					}
				})
				route := workflowTimerRootRoute(ctx)
				entityID := runtimecorrelation.RunIDFromContext(ctx)
				now := canonicalWorkflowTimerTime(time.Now().UTC().Add(-2 * time.Hour))
				instance := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, ".")
				instance.CreatedAt, instance.EnteredStageAt = now, now
				if loopOwned {
					activation, err := loopruntime.New(runtimecorrelation.RunIDFromContext(ctx), entityID, ".", "revision", "revision_id", uuid.NewString(), "waiting", 3, now)
					if err != nil {
						t.Fatal(err)
					}
					carrier := runtimeengine.NewStateCarrier(map[string]any{}, nil, map[string]map[string]any{})
					if err := loopruntime.Store(carrier.StateBuckets, activation); err != nil {
						t.Fatal(err)
					}
					instance.StateBuckets = carrier.PersistedStateBuckets()
				}
				{
					preparedInstance, preparedLifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef), instance, now)
					if err != nil {
						t.Fatalf("prepare fixture lifecycle: %v", err)
					}
					committedLifecycle, err := fixture.ConstructInitial(ctx, preparedInstance, preparedLifecycle)
					if err != nil {
						t.Fatalf("construct native fixture lifecycle: %v", err)
					}
					if err := pc.FinalizeInitialEntryLifecycle(ctx, committedLifecycle); err != nil {
						t.Fatalf("finalize fixture lifecycle: %v", err)
					}
				}
				if err := pc.ArmInitialEntryTimers(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath)); err != nil {
					t.Fatal(err)
				}
				activations := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true)
				if len(activations) != 1 {
					t.Fatalf("armed timers = %#v", activations)
				}
				if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activations[0]); err != nil || outcome != WorkflowTimerFireCommitted {
					t.Fatalf("fire = %s, %v", outcome, err)
				}
				if bus.publishedCount() != 1 {
					t.Fatalf("native timer publications=%d, want one", bus.publishedCount())
				}
				accepted := bus.persistedPublishedEvent(t, fixture, ctx, 0)
				loaded, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
				if err != nil || !found {
					t.Fatalf("reload = %v, %v", found, err)
				}
				if operation == "emit only" || strings.Contains(operation, "self advance") {
					if loaded.CurrentState != "waiting" || len(loaded.TransitionHistory) != 0 || !loaded.EnteredStageAt.Equal(now) {
						t.Fatalf("emit-only timer changed lifecycle: %#v", loaded)
					}
				} else {
					if loaded.CurrentState != "done" || len(loaded.TransitionHistory) != 1 {
						t.Fatalf("timer transition = %s, %#v", loaded.CurrentState, loaded.TransitionHistory)
					}
					record := loaded.TransitionHistory[0]
					compiled, ok := record.Evidence.Compiled()
					if !ok || compiled.FlowID() != "." || compiled.Edge().Source != "timer" || compiled.Edge().TimerID != bundle.Semantics.Timers[0].ID || record.TriggerEventID != accepted.ID() || record.TransitionID != record.Evidence.ID() {
						t.Fatalf("timer evidence = %#v", record)
					}
					assertCompiledLifecycleHistoryRoundTrip(t, record)
					if recognized, fired, err := pc.handleWorkflowStageTimerFire(ctx, accepted); err != nil || !recognized || fired {
						t.Fatalf("duplicate occurrence = %v/%v, %v", recognized, fired, err)
					}
					after, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
					if err != nil || !found || !reflect.DeepEqual(loaded.TransitionHistory, after.TransitionHistory) {
						t.Fatalf("duplicate occurrence changed history: %v, %v", found, err)
					}
				}
				if plainOperation != "advance" && plainOperation != "self advance" && string(accepted.Type()) != "review.expired" {
					t.Fatalf("public timer output = %s", accepted.Type())
				}
				if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, activations[0]); err != nil || outcome != WorkflowTimerFireTerminal {
					t.Fatalf("predecessor duplicate wakeup = %s, %v", outcome, err)
				}
				join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := pc.StopWorkflowTimerLifecycle(join); err != nil {
					cancel()
					t.Fatal(err)
				}
				cancel()
				nextFixture := fixture.ReopenExecution()
				restarted := nextFixture.NewCoordinator(PipelineCoordinatorOptions{Module: pc.module})
				successorBus := observeNativePipelineDeliveryBusForTest(t, restarted)
				restarted.workflowTimers.publication, restarted.workflowTimers.dispatcher, restarted.workflowTimers.logger = successorBus, successorBus.EngineDispatcher(), successorBus
				restartedCtx := runtimecorrelation.WithRunID(nextFixture.Context, entityID)
				store = restarted.workflowStore
				ctx = restartedCtx
				t.Cleanup(func() {
					join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := restarted.StopWorkflowTimerLifecycle(join); err != nil {
						t.Error(err)
					}
				})
				{
					if outcome, err := fireWorkflowTimerTestWakeup(ctx, restarted, activations[0]); err != nil || outcome != WorkflowTimerFireTerminal {
						t.Fatalf("duplicate wakeup = %s, %v", outcome, err)
					}
					if recognized, _, err := restarted.handleWorkflowStageTimerFire(ctx, accepted); err != nil || !recognized {
						t.Fatalf("duplicate accepted occurrence = %v, %v", recognized, err)
					}
				}
				after, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
				if err != nil || !found || !reflect.DeepEqual(loaded, after) {
					t.Fatalf("duplicate/reconstructed owner changed workflow: before=%#v after=%#v err=%v", loaded, after, err)
				}
				if active := listWorkflowTimerOwnerActivations(t, store, ctx, entityID, true); len(active) != 0 || bus.publishedCount() != 1 || successorBus.publishedCount() != 0 || successorBus.committedCount() != 0 {
					t.Fatalf("occurrence rearmed or republished: %#v, predecessor publications=%d successor publications=%d successor commits=%d", active, bus.publishedCount(), successorBus.publishedCount(), successorBus.committedCount())
				}
				if loopOwned {
					if err := store.mutateE(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath), func(current *WorkflowInstance) error {
						carrier, err := workflowInstanceStateCarrier(*current)
						if err != nil {
							return err
						}
						activation, found, err := loopruntime.Load(carrier.StateBuckets, ".", "revision")
						if err != nil || !found {
							t.Fatalf("missing current loop: %v", err)
						}
						if _, err := activation.Repeat("waiting", uuid.NewString(), now.Add(time.Hour)); err != nil {
							return err
						}
						if err := loopruntime.Store(carrier.StateBuckets, activation); err != nil {
							return err
						}
						current.StateBuckets = carrier.PersistedStateBuckets()
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					before, _, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
					if err != nil {
						t.Fatal(err)
					}
					if recognized, fired, err := restarted.handleWorkflowStageTimerFire(ctx, accepted); err != nil || !recognized || fired {
						t.Fatalf("stale same-stage occurrence bypassed generation: %v/%v %v", recognized, fired, err)
					}
					after, _, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
					if err != nil || !reflect.DeepEqual(before, after) || bus.publishedCount() != 1 || successorBus.publishedCount() != 0 || successorBus.committedCount() != 0 {
						t.Fatalf("stale same-stage occurrence changed lifecycle: %v", err)
					}
				}
			})
		}
	}
}

func VerifyAcceptedLifecycleConsumerRejectsUnownedTransitionOnBothStoresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	bundle := lifecycleStateFixtureForTest(t, "orders", "queued", "active", "lifecycle.transitioned")
	source := semanticview.Wrap(bundle)
	fixture := open(t, source)
	ctx := nativeWorkflowHandlerRunContextForTest(t, fixture)
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: source}})
	path := "orders/" + uuid.NewString()
	route := testWorkflowInstanceRoute(path)
	entityID := FlowInstanceEntityID(path)
	now := time.Now().UTC()
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, EntityID: entityID, WorkflowName: "orders", WorkflowVersion: source.WorkflowVersion(), CurrentState: "active", EnteredStageAt: now, EntityType: "test_entity"})
	if err := fixture.Construct(ctx, instance); err != nil {
		t.Fatal(err)
	}
	accepted := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "orders/lifecycle.transitioned", "operator", "", []byte(`{}`), 0, runtimecorrelation.RunIDFromContext(ctx), handlerTestWorkflowEnvelope("orders", path, entityID), testWorkflowRoutingSource("orders", path, entityID), now, executionmode.Live)
	gateAccepted := eventtest.RuntimeControl(uuid.NewString(), workflowGateDecisionEventType, "platform", "", []byte(`{}`), 0,
		runtimecorrelation.RunIDFromContext(ctx), "", handlerTestWorkflowEnvelope("orders", path, entityID), now)
	valid := lifecycleTransitionRecordFixtureForTest(t, "orders", "queued", "active", uuid.NewString(), now).Evidence
	node, _, found := valid.HandlerOrigin()
	if !found {
		t.Fatal("selected lifecycle cause has no declared handler")
	}
	deliveryRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "orders", FlowInstance: path, EntityID: entityID})}
	fixture.Publish(ctx, accepted, deliveryRoute)
	id, err := deliverylifecycle.DeliveryID(accepted.ID(), deliveryRoute)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := pc.deliveryStore.Snapshot(ctx, id)
	if err != nil || pending.EventID != accepted.ID() || pending.Route.Target != deliveryRoute.Target || pending.Route.Recipient != deliveryRoute.Recipient || pending.Status != deliverylifecycle.StatusPending {
		t.Fatalf("lifecycle planning publication differs from exact pending delivery: %+v err=%v", pending, err)
	}
	foreign := lifecycleTransitionRecordFixtureForTest(t, "sibling", "queued", "active", uuid.NewString(), now).Evidence
	wrongStage := lifecycleTransitionRecordFixtureForTest(t, "orders", "queued", "other", uuid.NewString(), now).Evidence
	otherSource := &PipelineCoordinator{module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(lifecycleStateFixtureForTest(t, "orders", "queued", "active", "other.handler"))}}
	unowned, err := compiledLifecycleTransitionForTest(otherSource, "orders", "queued", "active", "other.handler")
	if err != nil {
		t.Fatal(err)
	}
	gateBundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":          "name: frozen-gate\n",
		"orders/schema.yaml":   "name: orders\nstages:\n  queued:\n    gate:\n      decision: review\n      outcomes:\n        approve: {advances_to: active}\n  active: {}\n",
		"orders/entities.yaml": "test_entity: {}\n",
	})
	gateGraph, found := gateBundle.WorkflowStageTopology("orders")
	if !found {
		t.Fatal("gate fixture has no compiled topology")
	}
	gateCompiled, err := gateGraph.AdmitTransition(runtimecontracts.WorkflowTransitionSite{DecisionID: "review", Verdict: "approve"}, "queued", "active")
	if err != nil {
		t.Fatal(err)
	}
	frozenGate, err := runtimeworkflowlifecycle.NewCompiledTransition(gateCompiled, handlerselection.NotApplicable(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		cause     runtimeworkflowlifecycle.Transition
		wantError bool
	}{
		{"selected cause", valid, false},
		{"wrong flow", foreign, true},
		{"wrong prepared target", wrongStage, true},
		{"unowned same-flow carrier", *unowned, true},
		{"standalone gate lacks card proof", frozenGate, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inbound := accepted
			if tc.name == "standalone gate lacks card proof" {
				inbound = gateAccepted
			}
			effect, err := runtimeworkflowlifecycle.NewAcceptedEvent(route, identity.NormalizeEntityID(entityID), inbound.ID(), string(inbound.Type()), executionmode.Live, inbound.CreatedAt(), &tc.cause)
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "selected cause" {
				effect, err = effect.WithExecutionOccurrence("delivery", pending.DeliveryID)
				if err != nil {
					t.Fatal(err)
				}
			}
			candidate := instance
			plan, err := pc.prepareWorkflowLifecycleMutation(runtimecorrelation.WithInboundEvent(ctx, inbound), testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath), &candidate, []runtimeworkflowlifecycle.Effect{effect}, true)
			if (err != nil) != tc.wantError {
				t.Fatalf("plan error = %v, wantError=%v", err, tc.wantError)
			}
			if tc.name == "standalone gate lacks card proof" && (err == nil || !strings.Contains(err.Error(), "gate transition has no authoritative activation/card")) {
				t.Fatalf("standalone gate rejection = %v, want missing activation/card proof", err)
			}
			if tc.wantError && (!reflect.DeepEqual(plan, PreparedWorkflowLifecycleMutation{}) || !reflect.DeepEqual(candidate, instance)) {
				t.Fatal("rejected lifecycle cause produced a partial plan or changed the instance")
			}
		})
	}
}

func VerifyNativeCompiledTransitionEvidenceRoundTripOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		t.Run(storeCase.name, func(t *testing.T) {
			bundle := lifecycleStateFixtureForTest(t, "orders", "queued", "active", "order.accepted")
			fixture, pc, ctx, mutations := nativeWorkflowJoinCoordinatorForTest(t, storeCase.name, bundle, nil, open)
			store := pc.workflowStore
			instance := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, "orders")
			path, entityID := instance.StorageRef, instance.EntityID
			route := testRunScopedWorkflowInstanceFromContext(ctx, path).Route
			event := nativeWorkflowJoinEventForTest(ctx, "orders", path, entityID, "order.accepted", []byte("{}"), time.Now().UTC())
			node := pipelineNode(t, "orders", "lifecycle-owner")
			handler := pc.SemanticSource().ExecutableNodeEventHandlers(node)["order.accepted"]
			if _, err := executeNativePublishedWorkflowJoinForTest(t, fixture, mutations, pc, ctx, node, handler, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, pc, ctx, route, entityID), HandlerEventKey: "order.accepted"}); err != nil {
				t.Fatal(err)
			}
			loaded, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
			if err != nil || !found || len(loaded.TransitionHistory) != 1 {
				t.Fatalf("persisted lifecycle = %#v, %v, %v", loaded, found, err)
			}
			assertCompiledLifecycleHistoryRoundTrip(t, loaded.TransitionHistory[0])
			restarted := fixture.Persistence
			reloaded, found, err := restarted.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
			if err != nil || !found || !reflect.DeepEqual(reloaded.TransitionHistory, loaded.TransitionHistory) {
				t.Fatalf("restarted history = %#v, %v", reloaded.TransitionHistory, err)
			}
			foreign := lifecycleTransitionRecordFixtureForTest(t, "sibling", "queued", "active", loaded.TransitionHistory[0].TriggerEventID, loaded.TransitionHistory[0].FiredAt)
			for _, tc := range []struct {
				name   string
				mutate func(*WorkflowTransitionRecord)
			}{
				{"foreign valid cause", func(record *WorkflowTransitionRecord) { *record = foreign }},
				{"identity", func(record *WorkflowTransitionRecord) { record.TransitionID = "legacy_transition" }},
				{"source", func(record *WorkflowTransitionRecord) { record.From = "foreign" }},
				{"target", func(record *WorkflowTransitionRecord) { record.To = "foreign" }},
				{"guards", func(record *WorkflowTransitionRecord) { record.GuardsEvaluated = []string{"not_evaluated"} }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					bad := loaded
					bad.TransitionHistory = append([]WorkflowTransitionRecord(nil), loaded.TransitionHistory...)
					tc.mutate(&bad.TransitionHistory[0])
					record, err := workflowEngineStateRecord(testRunScopedWorkflowRoute(ctx, route), bad, loaded.CurrentState, loaded.Revision, WorkflowEngineStateTransitionUpdateStateAndCompanion, time.Now().UTC())
					if err == nil {
						_, err = store.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{State: record})
					}
					if err == nil {
						t.Fatal("writer accepted history contradicting its evidence or persisted flow")
					}
					unchanged, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
					if err != nil || !found || !reflect.DeepEqual(loaded.TransitionHistory, unchanged.TransitionHistory) || unchanged.Revision != loaded.Revision {
						t.Fatalf("rejected write changed persisted state: %#v, %v", unchanged, err)
					}
					original, err := fixture.TransitionWire(ctx, runtimecorrelation.RunIDFromContext(ctx), path)
					if err != nil {
						t.Fatal(err)
					}
					var config map[string]any
					if err := json.Unmarshal(original, &config); err != nil {
						t.Fatal(err)
					}
					config["transition_history"] = bad.TransitionHistory
					hostile, err := json.Marshal(config)
					if err != nil {
						t.Fatal(err)
					}
					if changed, err := fixture.SetTransitionWire(ctx, runtimecorrelation.RunIDFromContext(ctx), path, hostile); err != nil || changed != 1 {
						t.Fatal(err)
					}
					defer func() {
						if changed, err := fixture.SetTransitionWire(ctx, runtimecorrelation.RunIDFromContext(ctx), path, original); err != nil || changed != 1 {
							t.Error(err)
						}
					}()
					if _, _, err := restarted.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath)); err == nil {
						t.Fatal("reader accepted hostile persisted history")
					}
				})
			}
			projection := fixture.ReopenProjection()
			cold, found, err := projection.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !found || !reflect.DeepEqual(cold.TransitionHistory, loaded.TransitionHistory) {
				t.Fatalf("cold history changed: found=%t error=%v", found, err)
			}

		})
	}
}

func VerifyNativePipelineCompiledJoinTransitionEvidenceOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, outcome := range []string{"arrival", "deferred complete", "timeout", "loop timeout"} {
			t.Run(storeCase.name+"/"+outcome, func(t *testing.T) {
				bundle := workflowJoinLifecycleBundle(t)
				initial := "awaiting"
				members := []any{"a"}
				if outcome == "deferred complete" {
					members = []any{}
				}
				if outcome == "loop timeout" {
					bundle = workflowJoinLifecycleBundleWithOptions(t, false, "reentrant")
					initial = "dispatching"
				}
				h := newNativeExactWorkflowJoinHarness(t, storeCase.name, "orders", initial, members, bundle, open)
				pc, ctx, store := h.pc, h.ctx, h.store
				route, path, entityID := h.route, h.path, h.entityID
				var schedule runtimegenericschedule.Activation
				if outcome == "loop timeout" {
					h.startLoop()
					schedule = h.armedSchedule()
				} else {
					schedule = h.armInitial()
				}
				schedules := []runtimegenericschedule.Activation{schedule}
				before := h.instance()
				wantPriorRecords := 0
				if outcome == "loop timeout" {
					wantPriorRecords = 1
				}
				if before.CurrentState != "awaiting" || len(before.TransitionHistory) != wantPriorRecords {
					t.Fatalf("join precondition = state:%s evidence:%#v", before.CurrentState, before.TransitionHistory)
				}
				if outcome == "loop timeout" {
					prior, ok := before.TransitionHistory[0].Evidence.Compiled()
					if !ok || prior.Edge().From != "dispatching" || prior.Edge().To != "awaiting" || prior.Edge().LoopID != "revision" || prior.Edge().LoopOperation != runtimecontracts.LoopOperationStart {
						t.Fatalf("native loop start lost its admitted transition: %#v", before.TransitionHistory[0])
					}
				}

				event := workflowJoinScheduleEventForTest(t, uuid.NewString(), schedules[0], runtimecorrelation.RunIDFromContext(ctx), h.envelope(), time.Now().UTC())
				wantStage, wantContext := "ready", handlerselection.ContextJoinComplete
				if outcome == "arrival" {
					event = eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType("item.completed"), "", "", json.RawMessage(`{"member_id":"a","result":{"ok":true}}`), 0, runtimecorrelation.RunIDFromContext(ctx), h.envelope(), testWorkflowRoutingSource("orders", path, entityID), time.Now().UTC())
					node := mustPipelineNode("orders", "join-node")
					handler := pc.SemanticSource().ExecutableNodeEventHandlers(node)["item.completed"]
					result, err := executeNativePublishedWorkflowJoinForTest(t, h.fixture, h.mutations, pc, ctx, node, handler, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, pc, ctx, route, entityID), HandlerEventKey: "item.completed"})
					if err != nil || !result.Handled {
						t.Fatalf("arrival = %v, %v", result.Handled, err)
					}
					upserts, _ := h.mutations.schedules()
					var completionFound bool
					for _, schedule := range upserts {
						if schedule.Command.EventType == joinCompleteEvent {
							event = workflowJoinScheduleEventForTest(t, schedule.Command.TaskID+":fixture-completion", schedule, runtimecorrelation.RunIDFromContext(ctx), h.envelope(), schedule.InitialDueAt)
							completionFound = true
						}
					}
					if !completionFound {
						t.Fatal("arrival did not persist its completion control")
					}
				} else {
					if strings.HasSuffix(outcome, "timeout") {
						wantStage, wantContext = "attention", handlerselection.ContextJoinTimeout
					}
					result, err := executeNativeResolvedJoinForTest(t, h.fixture, pc, ctx, event, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, pc, ctx, route, entityID)})
					if err != nil || !result.Handled {
						t.Fatalf("join outcome = %v, %v", result.Handled, err)
					}
				}
				loaded, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
				// The persisted header retains current evidence, not cumulative history.
				if err != nil || !found || loaded.CurrentState != wantStage || len(loaded.TransitionHistory) != 1 || loaded.Revision <= before.Revision {
					t.Fatalf("join lifecycle = %s, %#v, %v", loaded.CurrentState, loaded.TransitionHistory, err)
				}
				record := loaded.TransitionHistory[0]
				compiled, ok := record.Evidence.Compiled()
				if !ok || compiled.FlowID() != "orders" || compiled.Edge().HandlerEvent != "item.completed" || !compiled.Edge().Node.Equal(mustPipelineNode("orders", "join-node")) || record.Evidence.RuleSelection().Context() != wantContext || !record.Evidence.RuleSelection().Ref().Equal(compiled.Edge().RuleRef) || record.TriggerEventID != event.ID() {
					t.Fatalf("join evidence = %#v", record)
				}
				edge := compiled.Edge()
				if edge.From != "awaiting" || edge.To != wantStage {
					t.Fatalf("join evidence changed its authored source/target: %#v", edge)
				}
				if strings.HasSuffix(outcome, "timeout") {
					if edge.EventType != "platform.join_timeout" || string(event.Type()) != "platform.join_timeout" || !edge.Timed || edge.After != "1h" || edge.TimerID != "awaiting" || edge.AdvanceCarrier != runtimecontracts.HandlerAdvanceCarrierJoinOnDeadline {
						t.Fatalf("join timeout lost its protocol/timer carrier: %#v", edge)
					}
				} else if edge.EventType != "item.completed" || edge.Timed || edge.TimerID != "" || edge.AdvanceCarrier != runtimecontracts.HandlerAdvanceCarrierJoinOnComplete {
					t.Fatalf("join completion acquired timeout authority: %#v", edge)
				}
				if outcome == "loop timeout" {
					if edge.Source != "loop.admit" || edge.LoopID != "revision" || edge.LoopOperation != runtimecontracts.LoopOperationAdmit {
						t.Fatalf("loop-owned join timeout lost its loop source: %#v", edge)
					}
				} else if edge.LoopID != "" || edge.LoopOperation != "" {
					t.Fatalf("ordinary join borrowed a loop carrier: %#v", edge)
				}
				assertCompiledLifecycleHistoryRoundTrip(t, record)
			})
		}
	}
}

func assertCompiledLifecycleHistoryRoundTrip(t *testing.T, record WorkflowTransitionRecord) {
	t.Helper()
	raw, err := json.Marshal([]WorkflowTransitionRecord{record})
	if err != nil {
		t.Fatal(err)
	}
	var history any
	if err := json.Unmarshal(raw, &history); err != nil {
		t.Fatal(err)
	}
	got, err := workflowInstanceTransitionHistoryFromConfig(map[string]any{"transition_history": history})
	if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], record) {
		t.Fatalf("history round trip = %#v, %v", got, err)
	}
	for _, tc := range []struct{ name, old, replacement string }{
		{"identity", record.TransitionID, "legacy_transition"},
		{"from", `"from":"` + record.From + `"`, `"from":"foreign"`},
		{"to", `"to":"` + record.To + `"`, `"to":"foreign"`},
		{"guards", `"guards":null`, `"guards":["not_evaluated"]`},
	} {
		t.Run("hostile "+tc.name, func(t *testing.T) {
			mutated := strings.Replace(string(raw), tc.old, tc.replacement, 1)
			if mutated == string(raw) {
				t.Fatal("hostile fixture was not changed")
			}
			var hostile any
			if err := json.Unmarshal([]byte(mutated), &hostile); err != nil {
				t.Fatal(err)
			}
			if _, err := workflowInstanceTransitionHistoryFromConfig(map[string]any{"transition_history": hostile}); err == nil {
				t.Fatalf("hostile persisted evidence accepted: %s", mutated)
			}
		})
	}
}
