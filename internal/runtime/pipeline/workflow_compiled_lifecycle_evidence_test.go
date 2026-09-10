package pipeline

import (
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
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestPipelineCompiledTimerTransitionEvidenceOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, operation := range []string{"advance", "emit and advance", "emit only"} {
			t.Run(storeCase.name+"/"+operation, func(t *testing.T) {
				store, ctx := storeCase.open(t)
				timer := "        advances_to: done\n"
				if operation == "emit only" {
					timer = "        emit: review.expired\n"
				} else if operation == "emit and advance" {
					timer += "        emit: review.expired\n"
				}
				bundle := loadWorkflowTempBundle(t, map[string]string{
					"schema.yaml":   "name: timer-evidence\nstages:\n  waiting:\n    initial: true\n    timers:\n      - after: 1h\n" + timer + "  done: {terminal: true}\n",
					"entities.yaml": "test_entity: {}\n",
					"events.yaml":   "review.expired: {}\n",
				})
				source := semanticview.Wrap(bundle)
				bus := &recordingPipelineBus{}
				owner := pipelineTestWorkOwner(t)
				pc := newWorkflowTimerOwnerPipelineCoordinator(bus, store.testDB(), PipelineCoordinatorOptions{
					Module: &pipelineFixtureWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store),
					WorkOwner: owner,
				})
				route := workflowTimerRootRoute(ctx)
				entityID := uuid.NewString()
				now := canonicalWorkflowTimerTime(time.Now().UTC().Add(-2 * time.Hour))
				instance := workflowTimerMaterializedInstance(ctx, entityID, route.InstancePath, WorkflowInstance{
					WorkflowVersion: "1", CurrentState: "waiting", CreatedAt: now, EntityType: "test_entity",
				})
				if _, err := pc.MaterializeInitialEntry(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef), instance, now); err != nil {
					t.Fatal(err)
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
				if len(bus.publishes) != 1 {
					t.Fatalf("timer publications = %#v", bus.publishes)
				}
				accepted := bus.publishes[0]
				dialect := authoractivityfixture.DialectPostgres
				if store.isSQLite() {
					dialect = authoractivityfixture.DialectSQLite
				}
				seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, accepted)
				if recognized, fired, err := pc.handleWorkflowStageTimerFire(ctx, accepted); err != nil || !recognized || !fired {
					t.Fatalf("accepted occurrence = %v/%v, %v", recognized, fired, err)
				}
				loaded, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
				if err != nil || !found {
					t.Fatalf("reload = %v, %v", found, err)
				}
				if operation == "emit only" {
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
				if operation != "advance" && string(accepted.Type()) != "review.expired" {
					t.Fatalf("public timer output = %s", accepted.Type())
				}
			})
		}
	}
}

func TestAcceptedLifecycleConsumerRejectsUnownedTransitionOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		t.Run(storeCase.name, func(t *testing.T) {
			store, ctx := storeCase.open(t)
			bundle := lifecycleStateFixtureForTest(t, "orders", "queued", "active", "lifecycle.transitioned")
			pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, Persistence: workflowPersistenceForTest(store),
			})
			path := "orders/" + uuid.NewString()
			route := testWorkflowInstanceRoute(path)
			entityID := FlowInstanceEntityID(path)
			now := time.Now().UTC()
			instance := materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: uuid.NewString(), StorageRef: path, EntityID: entityID, WorkflowName: "orders", WorkflowVersion: "1", CurrentState: "active", EnteredStageAt: now, EntityType: "test_entity"})
			if err := store.upsert(ctx, instance); err != nil {
				t.Fatal(err)
			}
			accepted := workflowLifecycleEventForTest(t, store, ctx, "orders", path, entityID, "orders/lifecycle.transitioned", now)
			gateAccepted := eventtest.RuntimeControl(uuid.NewString(), workflowGateDecisionEventType, "platform", "", []byte(`{}`), 0,
				runtimecorrelation.RunIDFromContext(ctx), "", handlerTestWorkflowEnvelope("orders", path, entityID), now)
			valid := lifecycleTransitionRecordFixtureForTest(t, "orders", "queued", "active", uuid.NewString(), now).Evidence
			foreign := lifecycleTransitionRecordFixtureForTest(t, "sibling", "queued", "active", uuid.NewString(), now).Evidence
			wrongStage := lifecycleTransitionRecordFixtureForTest(t, "orders", "queued", "other", uuid.NewString(), now).Evidence
			otherSource := &PipelineCoordinator{module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(lifecycleStateFixtureForTest(t, "orders", "queued", "active", "other.handler"))}}
			unowned, err := compiledLifecycleTransitionForTest(otherSource, "orders", "queued", "active", "other.handler")
			if err != nil {
				t.Fatal(err)
			}
			gateBundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":          "name: frozen-gate\n",
				"orders/schema.yaml":   "name: orders\nstages:\n  queued:\n    initial: true\n    gate:\n      decision: review\n      outcomes:\n        approve: {advances_to: active}\n  active: {}\n",
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
		})
	}
}

func TestCompiledTransitionEvidenceRoundTripOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		t.Run(storeCase.name, func(t *testing.T) {
			store, ctx := storeCase.open(t)
			bundle := lifecycleStateFixtureForTest(t, "orders", "queued", "active", "order.accepted")
			pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
				Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, Persistence: workflowPersistenceForTest(store),
			})
			path := "orders/" + uuid.NewString()
			route := testWorkflowInstanceRoute(path)
			entityID := FlowInstanceEntityID(path)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: uuid.NewString(), StorageRef: path, EntityID: entityID, WorkflowName: "orders", WorkflowVersion: "1",
				CurrentState: "queued", EnteredStageAt: time.Now().UTC(), EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			acceptedCtx := testPersistedWorkflowStateTransitionContext(t, store, ctx, route, entityID, "order.accepted")
			if err := pc.persistWorkflowStateForTest(acceptedCtx, route, entityID, "active", "order.accepted"); err != nil {
				t.Fatal(err)
			}
			loaded, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
			if err != nil || !found || len(loaded.TransitionHistory) != 1 {
				t.Fatalf("persisted lifecycle = %#v, %v, %v", loaded, found, err)
			}
			assertCompiledLifecycleHistoryRoundTrip(t, loaded.TransitionHistory[0])
			restarted := newPostgresWorkflowInstanceStoreForTest(store.testDB())
			if store.isSQLite() {
				restarted = newSQLiteWorkflowInstanceStoreForTest(t, store.testDB())
			}
			reloaded, found, err := restarted.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
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
					if err := store.upsert(ctx, bad); err == nil {
						t.Fatal("writer accepted history contradicting its evidence or persisted flow")
					}
					unchanged, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
					if err != nil || !found || !reflect.DeepEqual(loaded.TransitionHistory, unchanged.TransitionHistory) || unchanged.Revision != loaded.Revision {
						t.Fatalf("rejected write changed persisted state: %#v, %v", unchanged, err)
					}
					selectSQL := "SELECT config FROM flow_instances WHERE instance_path = ? AND run_id = ?"
					updateSQL := "UPDATE flow_instances SET config = ? WHERE instance_path = ? AND run_id = ?"
					if !store.isSQLite() {
						selectSQL = "SELECT config FROM flow_instances WHERE instance_path = $1 AND run_id = $2"
						updateSQL = "UPDATE flow_instances SET config = $1::jsonb WHERE instance_path = $2 AND run_id = $3"
					}
					var original []byte
					if err := store.testDB().QueryRowContext(ctx, selectSQL, path, runtimecorrelation.RunIDFromContext(ctx)).Scan(&original); err != nil {
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
					if _, err := store.testDB().ExecContext(ctx, updateSQL, string(hostile), path, runtimecorrelation.RunIDFromContext(ctx)); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if _, err := store.testDB().ExecContext(ctx, updateSQL, string(original), path, runtimecorrelation.RunIDFromContext(ctx)); err != nil {
							t.Error(err)
						}
					}()
					if _, _, err := restarted.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath)); err == nil {
						t.Fatal("reader accepted hostile persisted history")
					}
				})
			}
		})
	}
}

func TestPipelineCompiledJoinTransitionEvidenceOnBothStores(t *testing.T) {
	for _, storeCase := range workflowJoinStoreCases() {
		for _, outcome := range []string{"arrival", "deferred complete", "timeout", "loop timeout"} {
			t.Run(storeCase.name+"/"+outcome, func(t *testing.T) {
				store, ctx := storeCase.open(t)
				bundle := workflowJoinLifecycleBundle(t)
				if outcome == "loop timeout" {
					bundle = workflowJoinLifecycleBundleWithOptions(t, false, "reentrant")
				}
				pc := newWorkflowJoinPipelineCoordinator(&recordingPipelineBus{}, store.testDB(), PipelineCoordinatorOptions{
					Module: &pipelineFixtureWorkflowModule{source: semanticview.Wrap(bundle)}, Persistence: workflowPersistenceForTest(store),
					GenericSchedules: &recordingGenericScheduleWakeupOwner{},
				})
				configurePipelineTestDeliveryOwner(t, pc)
				path := "orders/" + uuid.NewString()
				route := testWorkflowInstanceRoute(path)
				entityID := FlowInstanceEntityID(path)
				members := []any{"a"}
				if outcome == "deferred complete" {
					members = []any{}
				}
				instance := materializedWorkflowInstanceForTest(WorkflowInstance{
					InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "orders", WorkflowVersion: "1", CurrentState: "awaiting",
					EnteredStageAt: time.Now().UTC(), Fields: map[string]any{"expected": members}, EntityType: "test_entity",
				})
				if outcome == "loop timeout" {
					activation, err := loopruntime.New(runtimecorrelation.RunIDFromContext(ctx), entityID, "orders", "revision", "revision_id", uuid.NewString(), "awaiting", 3, instance.EnteredStageAt)
					if err != nil {
						t.Fatal(err)
					}
					carrier, err := workflowInstanceStateCarrier(instance)
					if err != nil {
						t.Fatal(err)
					}
					if err := loopruntime.Store(carrier.StateBuckets, activation); err != nil {
						t.Fatal(err)
					}
					instance.StateBuckets = carrier.PersistedStateBuckets()
				}
				if err := store.upsert(ctx, instance); err != nil {
					t.Fatal(err)
				}
				if err := applyTestInitialEntryEffect(ctx, pc, route, entityID); err != nil {
					t.Fatal(err)
				}
				schedules, _ := committedWorkflowSchedulesForTest(t, store)
				if len(schedules) != 1 {
					t.Fatalf("join schedules = %#v", schedules)
				}
				event := workflowJoinScheduleEventForTest(t, uuid.NewString(), schedules[0], runtimecorrelation.RunIDFromContext(ctx), workflowJoinTestEnvelope(path, entityID), time.Now().UTC())
				wantStage, wantContext := "ready", handlerselection.ContextJoinComplete
				if outcome == "arrival" {
					event = eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("item.completed"), "", "", json.RawMessage(`{"member_id":"a","result":{"ok":true}}`), 0, runtimecorrelation.RunIDFromContext(ctx), "", workflowJoinTestEnvelope(path, entityID), time.Now().UTC())
					node := mustPipelineNode("orders", "join-node")
					delivery := seedExactOnceEventDelivery(t, pc, ctx, event, node)
					if handled, err := pc.executeNodeHandlerPlanResult(withWorkflowNodeDeliveryRoute(ctx, delivery), node, event); err != nil || !handled {
						t.Fatalf("arrival = %v, %v", handled, err)
					}
				} else {
					if strings.HasSuffix(outcome, "timeout") {
						wantStage, wantContext = "attention", handlerselection.ContextJoinTimeout
					}
					dialect := authoractivityfixture.DialectPostgres
					if store.isSQLite() {
						dialect = authoractivityfixture.DialectSQLite
					}
					seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, event)
					result, err := pc.executeAuthoritativeNodeHandler(ctx, event, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, pc, ctx, route, entityID)})
					if err != nil || !result.Handled {
						t.Fatalf("join outcome = %v, %v", result.Handled, err)
					}
				}
				loaded, found, err := store.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, route.InstancePath))
				if err != nil || !found || loaded.CurrentState != wantStage || len(loaded.TransitionHistory) != 1 {
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
					if edge.EventType != "platform.join_timeout" || string(event.Type()) != "platform.join_timeout" || !edge.Timed || edge.After != "1h" || edge.TimerID != "awaiting" || edge.AdvanceCarrier != runtimecontracts.HandlerAdvanceCarrierJoinTimeout {
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
