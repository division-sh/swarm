package pipeline_test

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

// Exercise the existing E carrier through the real manager planner, activation
// transaction and readiness finalizer, including recursively constructed sources.
func TestA2ActivationCarriesInitialJoinAtomicallyOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, scenario := range []struct {
			name       string
			count      int
			fault      bool
			descendant string
		}{{"arrival", 1, false, ""}, {"zero", 0, false, ""}, {"rollback_retry", 1, true, ""}, {"restart_before_arrival", 1, false, ""},
			{"nested_arrival", 1, false, "orders/child"}, {"nested_zero", 0, false, "orders/child/leaf"},
			{"nested_rollback_retry", 1, true, "orders/child/leaf"}, {"nested_restart_before_arrival", 1, false, "orders/child/leaf"},
			{"nested_two_parents", 1, false, "orders/child/leaf"}, {"nested_two_parents_restart_before_arrival", 1, false, "orders/child/leaf"}} {
			t.Run(backend.name+"/"+scenario.name, func(t *testing.T) {
				selected := backend.open(t)
				runID := uuid.NewString()
				files := a2ActivationJoinFiles(scenario.count)
				if scenario.descendant != "" {
					files = a2ConstructedDescendantJoinFiles(scenario.count, scenario.descendant)
				}
				source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, files))
				bundle, _ := semanticview.Bundle(source)
				fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
				if err != nil {
					t.Fatal(err)
				}
				ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContextForSource(t, context.Background(), fact), runID))
				fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: fact, Artifact: bundle.SourceArtifact}
				if selected.postgres {
					runlifecyclefixture.RequirePostgres(t, ctx, selected.db, fixture)
				} else {
					runlifecyclefixture.RequireSQLite(t, ctx, selected.db, fixture)
				}
				targetFlow := "orders"
				if scenario.descendant != "" {
					targetFlow = scenario.descendant
				}
				node := externalPipelineSourceNode(t, source, targetFlow, "collector")
				module := proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{
					{Node: node, Subscriptions: []events.EventType{"item.completed"}, ExecutionType: contracts.SystemNodeExecutionType},
				}}
				probe, logger := lifecycleprobe.New(), &exactJoinRuntimeLogger{}
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
					ContractBundle: source, SourceArtifactFact: fact, WorkOwner: pipelineExternalTestWorkOwnerForSource(t, fact), TestLifecycleProbe: probe, Logger: logger,
				}, "platform.join_complete", "platform.join_timeout")
				if err != nil {
					t.Fatal(err)
				}
				schedules, driver := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
				pc := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{
					Module: module, SourceArtifactFact: fact, GenericSchedules: schedules, TestLifecycleProbe: probe,
				})
				bus.SetInterceptors(pc)
				newManager, binding := a2ActivationJoinManagerFactory(t, ctx, selected, source)
				am := newManager(pc, bus)
				parent, err := flowidentity.StandingForGeneration(source, ".", runID)
				if err != nil {
					t.Fatal(err)
				}
				identity, err := flowidentity.KeyedChild(source, parent, "orders", "order-1")
				if err != nil {
					t.Fatal(err)
				}
				owner, err := flowidentity.NewRunScopedFlowInstance(runID, identity.Route())
				if err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC().Truncate(time.Microsecond)
				constructA2StructuralRoot(t, ctx, am, bus, source, parent, now)
				count := func(table string) int {
					t.Helper()
					var n int
					if err := selected.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", runID).Scan(&n); err != nil {
						t.Fatal(err)
					}
					return n
				}
				baseline := make(map[string]int)
				for _, table := range []string{"entity_state", "flow_instances", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness", "timers", "entity_mutations"} {
					baseline[table] = count(table)
				}
				req := pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: identity,
					ConstructorInput: "order.created", ResolvedKey: identity.InstanceID,
					TriggerEvent: eventtest.ExistingRunRootIngress(uuid.NewString(), "order.created", "operator", "", []byte(`{"order_id":"order-1"}`), 0, runID, events.EventEnvelope{}, now), OccurredAt: now}
				plan, err := am.PrepareFlowInstanceActivation(ctx, req)
				if err != nil {
					t.Fatalf("manager activation preparation: %v", err)
				}
				targetPlan := plan
				if scenario.descendant != "" {
					for _, construction := range plan.ConstructionPlans() {
						if construction.Identity.TemplateID == scenario.descendant {
							targetPlan = construction
						}
					}
					if targetPlan.Identity.TemplateID != scenario.descendant {
						t.Fatal("recursive constructor omitted descendant")
					}
					identity = targetPlan.Identity
					owner, err = flowidentity.NewRunScopedFlowInstance(runID, identity.Route())
					if err != nil {
						t.Fatal(err)
					}
				}
				entry, found, err := workflowlifecycle.LoadStageEntry(targetPlan.Instance.Bookkeeping)
				plannedArm := exactJoinPersistedArmForOwner(t, targetPlan.Instance, owner)
				if err != nil || !found || entry.Cause != "construction" || entry.FlowScope != identity.ScopeKey ||
					entry.InstancePath != identity.InstancePath || entry.EntityID != identity.EntityID ||
					targetPlan.Lifecycle.StageEntry == nil || *targetPlan.Lifecycle.StageEntry != entry ||
					plannedArm.JoinRef().StageEntry() != entry || len(targetPlan.Lifecycle.Schedules) != 1 {
					t.Fatalf("activation omitted exact initial lifecycle: entry=%#v arm=%#v plan=%#v err=%v", entry, plannedArm, targetPlan.Lifecycle, err)
				}
				for table, before := range baseline {
					if count(table) != before {
						t.Fatalf("preparation persisted %s", table)
					}
				}
				if scenario.fault {
					drop := a2ActivationJoinScheduleFault(t, ctx, selected)
					failed, err := bus.CommitFlowInstanceActivation(ctx, plan)
					if err == nil || !strings.Contains(err.Error(), "a2_activation_join_sql_fault") || failed.Acknowledged {
						t.Fatalf("activation did not reach real schedule fault: result=%#v err=%v", failed, err)
					}
					for table, before := range baseline {
						if count(table) != before {
							t.Fatalf("failed activation leaked %s", table)
						}
					}
					drop()
				}
				committed, err := bus.CommitFlowInstanceActivation(ctx, plan)
				if err != nil || !committed.Acknowledged || !committed.Created {
					t.Fatalf("activation commit: result=%#v err=%v", committed, err)
				}
				load := func() pipeline.WorkflowInstance {
					t.Helper()
					instance, found, err := pc.Load(ctx, owner)
					if err != nil || !found {
						t.Fatalf("load activated receiver: found=%v err=%v", found, err)
					}
					return instance
				}
				persisted := load()
				wantTimers := 1
				var siblingOwner flowidentity.RunScopedFlowInstance
				var siblingBefore pipeline.WorkflowInstance
				var siblingCommit pipeline.CommittedFlowInstanceActivation
				if strings.HasPrefix(scenario.name, "nested_two_parents") {
					siblingReq := req
					siblingReq.Instance, err = flowidentity.KeyedChild(source, parent, "orders", "order-2")
					if err != nil {
						t.Fatal(err)
					}
					siblingReq.ResolvedKey = "order-2"
					siblingReq.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "order.created", "operator", "", []byte(`{"order_id":"order-2"}`), 0, runID, events.EventEnvelope{}, now)
					siblingPlan, err := am.PrepareFlowInstanceActivation(ctx, siblingReq)
					if err != nil {
						t.Fatalf("prepare independently keyed sibling tree: %v", err)
					}
					for _, construction := range siblingPlan.ConstructionPlans() {
						if construction.Identity.TemplateID == scenario.descendant {
							siblingOwner, err = flowidentity.NewRunScopedFlowInstance(runID, construction.Identity.Route())
							if err != nil {
								t.Fatal(err)
							}
						}
					}
					if siblingOwner.RunID == "" || siblingOwner == owner {
						t.Fatal("second keyed parent lost its independent descendant")
					}
					siblingCommit, err = bus.CommitFlowInstanceActivation(ctx, siblingPlan)
					if err != nil || !siblingCommit.Acknowledged || !siblingCommit.Created {
						t.Fatalf("commit sibling tree: result=%#v err=%v", siblingCommit, err)
					}
					siblingBefore, found, err = pc.Load(ctx, siblingOwner)
					if err != nil || !found {
						t.Fatalf("load sibling descendant: found=%v err=%v", found, err)
					}
					assertA2ConstructedGateSources(t, ctx, selected, siblingPlan)
					if err := am.FinalizeCommittedFlowInstanceActivation(ctx, siblingCommit); err != nil {
						t.Fatalf("attach sibling tree: %v", err)
					}
					wantTimers++
				}
				if scenario.descendant != "" {
					assertA2ConstructedGateSources(t, ctx, selected, plan)
				}
				if arm := exactJoinPersistedArmForOwner(t, persisted, owner); !reflect.DeepEqual(arm, plannedArm) {
					t.Fatalf("activation changed prepared membership/entry: actual=%#v planned=%#v", arm, plannedArm)
				}
				readiness, found, err := pc.LoadDynamicFlowRuntimeReadiness(ctx, runID, identity.Route())
				if err != nil || !found || readiness.AttemptOrdinal != committed.ReadinessAttemptOrdinal || readiness.Phase != pipeline.FlowAttachmentPlanned {
					t.Fatalf("commit did not retain unfinalized readiness: found=%v readiness=%#v err=%v", found, readiness, err)
				}
				history := count("entity_mutations")
				replayed, err := bus.CommitFlowInstanceActivation(ctx, plan)
				if err != nil || !replayed.Acknowledged || replayed.Created || count("timers") != wantTimers || count("entity_mutations") != history ||
					!reflect.DeepEqual(load(), persisted) {
					t.Fatalf("exact activation replay reapplied initial lifecycle: result=%#v err=%v", replayed, err)
				}
				if err := am.FinalizeCommittedFlowInstanceActivation(ctx, committed); err != nil {
					t.Fatalf("real manager readiness finalization: %v", err)
				}
				if err := am.FinalizeCommittedFlowInstanceActivation(ctx, replayed); err != nil {
					t.Fatalf("duplicate readiness finalization: %v", err)
				}
				readiness, found, err = pc.LoadDynamicFlowRuntimeReadiness(ctx, runID, identity.Route())
				if err != nil || !found || readiness.Phase != pipeline.FlowAttachmentReady || readiness.AttemptState != "accepted" || count("timers") != wantTimers {
					t.Fatalf("readiness did not converge once: found=%v readiness=%#v err=%v", found, readiness, err)
				}
				attempt, err := pipeline.NewDynamicFlowRuntimeActivationAttempt(strconv.FormatUint(readiness.AttemptOrdinal, 10), runID, owner.Route.InstancePath, binding)
				if err != nil {
					t.Fatal(err)
				}
				if err := pc.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt); err != nil {
					t.Fatalf("ready attachment has no exact admitted attempt: %v", err)
				}
				if strings.HasSuffix(scenario.name, "restart_before_arrival") {
					before := load()
					if err := am.Shutdown(); err != nil {
						t.Fatal(err)
					}
					if err := schedules.Stop(ctx); err != nil {
						t.Fatal(err)
					}
					bus, err = newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
						ContractBundle: source, SourceArtifactFact: fact, WorkOwner: pipelineExternalTestWorkOwnerForSource(t, fact), TestLifecycleProbe: probe, Logger: logger,
					}, "platform.join_complete", "platform.join_timeout")
					if err != nil {
						t.Fatal(err)
					}
					schedules, driver = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
					pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{
						Module: module, SourceArtifactFact: fact, GenericSchedules: schedules, TestLifecycleProbe: probe,
					})
					bus.SetInterceptors(pc)
					am = newManager(pc, bus)
					if err := am.FinalizeCommittedFlowInstanceActivation(ctx, replayed); err != nil {
						t.Fatalf("reconstruct exact activation readiness: %v", err)
					}
					if siblingOwner.RunID != "" {
						if err := am.FinalizeCommittedFlowInstanceActivation(ctx, siblingCommit); err != nil {
							t.Fatalf("reconstruct sibling readiness: %v", err)
						}
					}
					if !reflect.DeepEqual(load(), before) || count("timers") != wantTimers {
						t.Fatal("reconstruction reminted initial entry, membership or deadline")
					}
					readiness, found, err = pc.LoadDynamicFlowRuntimeReadiness(ctx, runID, identity.Route())
					if err != nil || !found || readiness.Phase != pipeline.FlowAttachmentReady || readiness.AttemptState != "accepted" || readiness.AttemptOrdinal <= attempt.Ordinal() {
						t.Fatalf("restart reused predecessor readiness: %+v found=%t err=%v", readiness, found, err)
					}
					if err := pc.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt); err == nil {
						t.Fatal("restarted attachment retained predecessor authority")
					}
					attempt, err = pipeline.NewDynamicFlowRuntimeActivationAttempt(strconv.FormatUint(readiness.AttemptOrdinal, 10), runID, owner.Route.InstancePath, binding)
					if err != nil {
						t.Fatal(err)
					}
					if err := pc.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt); err != nil {
						t.Fatalf("reconstruction omitted exact successor admission: %v", err)
					}
					if scenario.descendant != "" {
						assertA2ConstructedGateSources(t, ctx, selected, plan)
					}
				}
				var arrival events.Event
				if scenario.count != 0 {
					producer := eventtest.ConcreteTemplateRoutingSource(identity.TemplateID, identity.InstancePath, identity.EntityID)
					if scenario.descendant != "" {
						producer, err = events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: identity.TemplateID, FlowInstance: identity.InstancePath, EntityID: identity.EntityID})
						if err != nil {
							t.Fatal(err)
						}
					}
					arrival = eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(identity.InstancePath+"/item.completed"), "operator", "", []byte(`{"order_id":"order-1","member_id":"a","result":"accepted"}`), 0, runID,
						events.EnvelopeForEntityID(events.EventEnvelope{}, identity.EntityID), producer, now)
					if err := bus.PublishAcknowledged(ctx, arrival); err != nil {
						t.Fatal(err)
					}
					a2KnownTargetWaitForSettlement(t, ctx, bus, probe, arrival, node.Key(), "completed", "delivered", logger)
					publication, found, err := selected.events.LoadPreparedPublishEvent(ctx, arrival.ID())
					if err != nil || !found || len(publication.DeliveryRoutes) != 1 ||
						len(publication.DeliveryRoutes[0].Context.Joins) != 1 || !publication.DeliveryRoutes[0].Context.Joins[0].Ref.Equal(plannedArm.JoinRef()) {
						t.Fatalf("first publication omitted activation-owned binding: found=%v routes=%#v err=%v", found, publication.DeliveryRoutes, err)
					}
				}
				closed := exactJoinPersistedArmForOwner(t, load(), owner)
				if closed.Status != joinruntime.StatusClosed || !closed.OutcomePending || closed.Completed() != scenario.count || !closed.JoinRef().Equal(plannedArm.JoinRef()) {
					t.Fatalf("first delivery failed to use activated arm: %#v", closed)
				}
				if err := driver.Resume(ctx); err != nil {
					t.Fatal(err)
				}
				completionID := exactJoinOccurrenceEventID(t, selected, ctx, runID, "platform.join_complete")
				wait, cancel := context.WithTimeout(ctx, 5*time.Second)
				completion, err := probe.WaitForHandlerCompleted(wait, completionID, node.Key())
				cancel()
				if err != nil || completion.Status != "completed" {
					t.Fatalf("activated join continuation: completion=%#v err=%v", completion, err)
				}
				assertExactJoinDeliveryStatus(t, selected, ctx, completionID, node.Key(), "delivered")
				final := load()
				if siblingOwner.RunID != "" {
					siblingAfter, found, err := pc.Load(ctx, siblingOwner)
					if err != nil || !found || !reflect.DeepEqual(siblingAfter, siblingBefore) {
						t.Fatalf("target delivery changed equal-named sibling beneath a second parent: found=%v sibling=%#v err=%v", found, siblingAfter, err)
					}
				}
				if arm := exactJoinPersistedArmForOwner(t, final, owner); !arm.OutcomeFired || arm.OutcomePending ||
					!arm.JoinRef().Equal(plannedArm.JoinRef()) || final.Fields["final_count"] != int64(scenario.count) {
					t.Fatalf("activated continuation lost exact entry/result: arm=%#v fields=%#v", arm, final.Fields)
				}
				// The untouched sibling retains a future deadline. Join the clock
				// before run-wide quiescence, without cancelling its durable arm.
				if err := schedules.Stop(ctx); err != nil {
					t.Fatal(err)
				}
				if siblingOwner.RunID != "" {
					arm := exactJoinPersistedArmForOwner(t, siblingBefore, siblingOwner)
					pending := exactJoinPendingSchedule(t, selected, ctx, arm)
					if pending.Status != "active" || pending.Command.RoutingSource.Route() != (events.RouteIdentity{
						FlowID: siblingBefore.WorkflowName, FlowInstance: siblingOwner.Route.InstancePath, EntityID: siblingBefore.EntityID,
					}) {
						t.Fatalf("joining clock changed the exact sibling deadline/source: %#v", pending)
					}
				}
				quiet, cancelQuiet := context.WithTimeout(ctx, 5*time.Second)
				err = bus.WaitForQuiescence(quiet)
				cancelQuiet()
				if err != nil {
					t.Fatalf("finish actual activation pipeline before replay: %v", err)
				}
				summary, err := selected.events.PipelineObligations().SummarizeRun(ctx, runID)
				if err != nil || summary.HasOpenWork() || summary.TerminalNonSuccess != 0 {
					t.Fatalf("activation pipeline not durably complete before replay: summary=%#v err=%v", summary, err)
				}
				history = count("entity_mutations")
				completionPublication, found, err := selected.events.LoadPreparedPublishEvent(ctx, completionID)
				if err != nil || !found {
					t.Fatalf("read committed activation continuation: found=%v err=%v", found, err)
				}
				for _, duplicate := range []events.Event{arrival, completionPublication.Event.Event()} {
					if duplicate.ID() == "" {
						continue
					}
					if err := bus.PublishAcknowledged(ctx, duplicate); err != nil {
						t.Fatal(err)
					}
					quiet, cancel := context.WithTimeout(ctx, 5*time.Second)
					err := bus.WaitForQuiescence(quiet)
					cancel()
					if err != nil || !reflect.DeepEqual(load(), final) || count("entity_mutations") != history {
						t.Fatalf("duplicate activated work reexecuted: %v", err)
					}
					assertExactJoinDeliveryCount(t, selected, ctx, duplicate.ID(), node.Key(), 1)
				}
			})
		}
	}
}

func constructA2StructuralRoot(t *testing.T, ctx context.Context, am *manager.AgentManager, bus *runtimebus.EventBus, source semanticview.Source, parent flowidentity.Instance, at time.Time) {
	t.Helper()
	plan, err := am.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: parent, OccurredAt: at})
	if err != nil || len(plan.ConstructionPlans()) != 1 {
		t.Fatalf("prepare structural root only: %+v %v", plan, err)
	}
	committed, err := bus.CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct actual structural root: %+v %v", committed, err)
	}
	if err := am.FinalizeCommittedFlowInstanceActivation(ctx, committed); err != nil {
		t.Fatal(err)
	}
}

func a2ActivationJoinManagerFactory(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, source semanticview.Source) (func(*pipeline.PipelineCoordinator, *runtimebus.EventBus) *manager.AgentManager, processbinding.Binding) {
	t.Helper()
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found {
		t.Fatal("activation fixture requires its admitted source")
	}
	process, err := selected.events.(startupownership.Store).AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "a2-activation-join", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Release(context.Background()) })
	set, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: fact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: set}); err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fact.BundleHash(),
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: set.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	admission, err := agenttopology.StaticAdmission(set.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := grant.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	return func(pc *pipeline.PipelineCoordinator, bus *runtimebus.EventBus) *manager.AgentManager {
		t.Helper()
		am := manager.NewAgentManagerWithOptions(bus, nil, manager.AgentManagerOptions{
			BaseContext: ctx, SemanticSource: source, SourceArtifactFact: fact,
			WorkflowInstances: pc, LifecycleStore: grant, DeliveryStore: selected.events,
			WorkOwner: pipelineExternalTestWorkOwnerForSource(t, fact), ReceiverExecution: eventreceiver.NormalExecution(), ExecutionPosture: executionposture.Live,
			PersistenceRoles: manager.PersistenceRoles{FlowActivation: bus, AgentRoutes: bus, CreationPublisher: bus},
		}, selected.events.(manager.ManagerPersistence))
		if err := am.InstallStartupTopology(grant, admission, set); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := am.Shutdown(); err != nil {
				t.Errorf("shutdown actual activation manager: %v", err)
			}
		})
		return am
	}, binding
}

func a2ActivationJoinScheduleFault(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase) func() {
	t.Helper()
	create := `CREATE TRIGGER a2_activation_join_failure BEFORE INSERT ON timers BEGIN SELECT RAISE(ABORT,'a2_activation_join_sql_fault'); END`
	drop := "DROP TRIGGER a2_activation_join_failure"
	if selected.postgres {
		if _, err := selected.db.ExecContext(ctx, `CREATE FUNCTION a2_activation_join_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'a2_activation_join_sql_fault'; END $$`); err != nil {
			t.Fatal(err)
		}
		create = `CREATE TRIGGER a2_activation_join_failure BEFORE INSERT ON timers FOR EACH ROW EXECUTE FUNCTION a2_activation_join_fail()`
		drop += " ON timers"
	}
	if _, err := selected.db.ExecContext(ctx, create); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if _, err := selected.db.ExecContext(ctx, drop); err != nil {
			t.Fatal(err)
		}
	}
}

func a2ActivationJoinFiles(count int) map[string]string {
	return map[string]string{
		"schema.yaml":          "name: a2-activation-join\nstages:\n  active: {}\n",
		"orders/schema.yaml":   "name: orders\ninstance: order_id\npins:\n  inputs:\n    - order.created\nstages:\n  awaiting: {}\n",
		"orders/entities.yaml": "order_state:\n  order_id: {type: text, indexed: true}\n  final_count: {type: integer, initial: -1}\n",
		"orders/events.yaml":   "order.created:\n  order_id: text\nitem.completed:\n  order_id: text\n  member_id: text\n  result: text\n",
		"orders/nodes.yaml": fmt.Sprintf(`collector:
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        stage: awaiting
        members: {count: %d, by: payload.member_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        on_complete:
          data_accumulation:
            writes: [{target_field: final_count, value: join.completed}]
        on_deadline:
          data_accumulation:
            writes: [{target_field: final_count, value: join.completed}]
`, count),
	}
}
