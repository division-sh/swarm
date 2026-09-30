package pipeline_test

import (
	"context"
	"fmt"
	"reflect"
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
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

// Exercise the existing E carrier through the real manager planner, activation
// transaction and readiness finalizer. This is not eager recursive construction.
func TestA2ActivationCarriesInitialJoinAtomicallyOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, scenario := range []struct {
			name  string
			count int
			fault bool
		}{{"arrival", 1, false}, {"zero", 0, false}, {"rollback_retry", 1, true}, {"restart_before_arrival", 1, false}} {
			t.Run(backend.name+"/"+scenario.name, func(t *testing.T) {
				selected := backend.open(t)
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, a2ActivationJoinFiles(scenario.count)))
				node := externalPipelineSourceNode(t, source, "orders", "collector")
				module := proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{
					{Node: node, Subscriptions: []events.EventType{"item.completed"}, ExecutionType: contracts.SystemNodeExecutionType},
				}}
				probe, logger := lifecycleprobe.New(), &exactJoinRuntimeLogger{}
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
					ContractBundle: source, TestLifecycleProbe: probe, Logger: logger,
				}, "platform.join_complete", "platform.join_timeout")
				if err != nil {
					t.Fatal(err)
				}
				schedules, driver := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
				pc := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{
					Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
				})
				bus.SetInterceptors(pc)
				newManager := a2ActivationJoinManagerFactory(t, ctx, selected, source)
				am := newManager(pc, bus)
				identity := flowidentity.Instance{
					TemplateID: "orders", ScopeKey: "orders", InstanceID: "order-1", InstancePath: "orders/order-1",
					EntityID: flowidentity.EntityID("orders/order-1"), HasStoredPath: true,
				}
				owner, err := flowidentity.NewRunScopedFlowInstance(runID, identity.Route())
				if err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC().Truncate(time.Microsecond)
				req := pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: identity,
					Config: map[string]any{"order_id": identity.InstanceID},
					Fields: map[string]any{"order_id": identity.InstanceID, "final_count": int64(-1)}, OccurredAt: now}
				plan, err := am.PrepareFlowInstanceActivation(ctx, req)
				if err != nil {
					t.Fatalf("manager activation preparation: %v", err)
				}
				entry, found, err := workflowlifecycle.LoadStageEntry(plan.Instance.Bookkeeping)
				plannedArm := exactJoinPersistedArm(t, plan.Instance)
				if err != nil || !found || entry.Cause != "construction" || entry.FlowScope != "orders" ||
					entry.InstancePath != identity.InstancePath || entry.EntityID != identity.EntityID ||
					plan.Lifecycle.StageEntry == nil || *plan.Lifecycle.StageEntry != entry ||
					plannedArm.JoinRef().StageEntry() != entry || len(plan.Lifecycle.Schedules) != 1 {
					t.Fatalf("activation omitted exact initial lifecycle: entry=%#v arm=%#v plan=%#v err=%v", entry, plannedArm, plan.Lifecycle, err)
				}
				count := func(table string) int {
					t.Helper()
					var n int
					if err := selected.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", runID).Scan(&n); err != nil {
						t.Fatal(err)
					}
					return n
				}
				for _, table := range []string{"entity_state", "flow_instances", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness", "timers", "entity_mutations"} {
					if count(table) != 0 {
						t.Fatalf("preparation persisted %s", table)
					}
				}
				if scenario.fault {
					drop := a2ActivationJoinScheduleFault(t, ctx, selected)
					failed, err := bus.CommitFlowInstanceActivation(ctx, plan)
					if err == nil || !strings.Contains(err.Error(), "a2_activation_join_sql_fault") || failed.Acknowledged {
						t.Fatalf("activation did not reach real schedule fault: result=%#v err=%v", failed, err)
					}
					for _, table := range []string{"entity_state", "flow_instances", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness", "timers", "entity_mutations"} {
						if count(table) != 0 {
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
				if arm := exactJoinPersistedArm(t, persisted); !reflect.DeepEqual(arm, plannedArm) {
					t.Fatalf("activation changed prepared membership/entry: actual=%#v planned=%#v", arm, plannedArm)
				}
				readiness, found, err := pc.LoadDynamicFlowRuntimeReadiness(ctx, runID, identity.Route())
				if err != nil || !found || readiness.PlanRevision != committed.ReadinessRevision || !readiness.TopologyReadyAt.IsZero() {
					t.Fatalf("commit did not retain unfinalized readiness: found=%v readiness=%#v err=%v", found, readiness, err)
				}
				history := count("entity_mutations")
				replayed, err := bus.CommitFlowInstanceActivation(ctx, plan)
				if err != nil || !replayed.Acknowledged || replayed.Created || count("timers") != 1 || count("entity_mutations") != history ||
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
				if err != nil || !found || readiness.TopologyReadyAt.IsZero() || !bus.HasFlowInstanceRoute(owner) || count("timers") != 1 {
					t.Fatalf("readiness/route did not converge once: found=%v readiness=%#v err=%v", found, readiness, err)
				}
				if scenario.name == "restart_before_arrival" {
					before := load()
					if err := am.Shutdown(); err != nil {
						t.Fatal(err)
					}
					if err := schedules.Stop(ctx); err != nil {
						t.Fatal(err)
					}
					bus, err = newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
						ContractBundle: source, TestLifecycleProbe: probe, Logger: logger,
					}, "platform.join_complete", "platform.join_timeout")
					if err != nil {
						t.Fatal(err)
					}
					schedules, driver = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
					pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{
						Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
					})
					bus.SetInterceptors(pc)
					am = newManager(pc, bus)
					if err := am.FinalizeCommittedFlowInstanceActivation(ctx, replayed); err != nil {
						t.Fatalf("reconstruct exact activation readiness: %v", err)
					}
					if !reflect.DeepEqual(load(), before) || !bus.HasFlowInstanceRoute(owner) || count("timers") != 1 {
						t.Fatal("reconstruction reminted initial entry, membership or deadline")
					}
				}
				var arrival events.Event
				if scenario.count != 0 {
					arrival = eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "orders/order-1/item.completed", "operator", "", []byte(`{"order_id":"order-1","member_id":"a","result":"accepted"}`), 0, runID,
						events.EnvelopeForEntityID(events.EventEnvelope{}, identity.EntityID), eventtest.ConcreteTemplateRoutingSource("orders", identity.InstancePath, identity.EntityID), now)
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
				closed := exactJoinPersistedArm(t, load())
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
				if arm := exactJoinPersistedArm(t, final); !arm.OutcomeFired || arm.OutcomePending ||
					!arm.JoinRef().Equal(plannedArm.JoinRef()) || final.Fields["final_count"] != int64(scenario.count) {
					t.Fatalf("activated continuation lost exact entry/result: arm=%#v fields=%#v", arm, final.Fields)
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
				if err := schedules.Stop(ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func a2ActivationJoinManagerFactory(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, source semanticview.Source) func(*pipeline.PipelineCoordinator, *runtimebus.EventBus) *manager.AgentManager {
	t.Helper()
	process, err := selected.events.(startupownership.Store).AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "a2-activation-join", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Release(context.Background()) })
	set, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: authorActivityTestSourceArtifactFact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: set}); err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: authorActivityTestSourceArtifactFact.BundleHash(),
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
	admission, err := agenttopology.StaticAdmission(set.Revision, authorActivityTestSourceArtifactFact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	return func(pc *pipeline.PipelineCoordinator, bus *runtimebus.EventBus) *manager.AgentManager {
		t.Helper()
		am := manager.NewAgentManagerWithOptions(bus, nil, manager.AgentManagerOptions{
			BaseContext: ctx, SemanticSource: source, SourceArtifactFact: authorActivityTestSourceArtifactFact,
			WorkflowInstances: pc, LifecycleStore: grant, DeliveryStore: selected.events,
			WorkOwner: pipelineExternalTestWorkOwner(t), ReceiverExecution: eventreceiver.NormalExecution(), ExecutionPosture: executionposture.Live,
			PersistenceRoles: manager.PersistenceRoles{FlowActivation: bus, AgentRoutes: bus, RouteInstaller: bus, RouteVerifier: bus, RouteRestorer: bus, CreationPublisher: bus},
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
	}
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
		"schema.yaml":          "name: a2-activation-join\nstages:\n  active: {initial: true}\n",
		"orders/schema.yaml":   "name: orders\nmode: template\ninstance: order_id\nstages:\n  awaiting: {initial: true}\n",
		"orders/entities.yaml": "order_state:\n  order_id: {type: text, indexed: true}\n  final_count: integer\n",
		"orders/events.yaml":   "item.completed:\n  order_id: text\n  member_id: text\n  result: text\n",
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
            writes: [{target_field: final_count, value: "${join.completed}"}]
        on_deadline:
          data_accumulation:
            writes: [{target_field: final_count, value: "${join.completed}"}]
`, count),
	}
}
