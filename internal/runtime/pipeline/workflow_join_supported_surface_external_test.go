package pipeline_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type exactExternalJoinSource struct {
	semanticview.Source
	flowID string
	plans  []runtimecontracts.WorkflowJoinPlan
}

func (s exactExternalJoinSource) WorkflowJoins() []runtimecontracts.WorkflowJoinPlan {
	return append([]runtimecontracts.WorkflowJoinPlan(nil), s.plans...)
}

func (s exactExternalJoinSource) NodeContractSource(nodeID string) (runtimecontracts.ContractItemSource, bool) {
	if strings.TrimSpace(nodeID) == "join-node" {
		layer := "flow"
		if s.flowID == "" {
			layer = "project"
		}
		return runtimecontracts.ContractItemSource{FlowID: s.flowID, Layer: layer}, true
	}
	return s.Source.NodeContractSource(nodeID)
}

type joinProofGenericScheduleWakeups struct{}

func (joinProofGenericScheduleWakeups) ReconcileWakeupWithRecovery(context.Context, string) (bool, error) {
	return false, nil
}

type exactJoinScheduleLogger struct{ t *testing.T }

func (l exactJoinScheduleLogger) GenericScheduleFailure(_ context.Context, action, activationID string, err error) {
	l.t.Helper()
	l.t.Logf("generic schedule %s for %s failed: %v", action, activationID, err)
}

func (exactJoinScheduleLogger) GenericScheduleCatchupWarning(context.Context, string, int) {}

func TestWorkflowJoinDurableEventBusDeliveryClaimPreservesExactDeclarationOnBothStores(t *testing.T) {
	for _, storeCase := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		for _, flowID := range []string{"", "orders"} {
			scope := "root"
			if flowID != "" {
				scope = "flow"
			}
			t.Run(storeCase.name+"/"+scope, func(t *testing.T) {
				selected := storeCase.open(t)
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				source := exactExternalWorkflowJoinSource(t, flowID)
				path := runID
				workflowName := source.WorkflowName()
				instanceID := runID
				if flowID != "" {
					instanceID = uuid.NewString()
					path = flowID + "/" + instanceID
					workflowName = flowID
				}
				eventType := "item.completed"
				subscriptionType := source.WorkflowName() + "/item.completed"
				var additionalDescriptors []string
				if flowID != "" {
					eventType = path + "/item.completed"
					subscriptionType = eventType
					additionalDescriptors = append(additionalDescriptors, eventType)
				}
				if owners := source.RuntimeEventOwners("item.completed"); len(owners) != 1 || owners[0] != "join-node" {
					t.Fatalf("join event owners = %#v", owners)
				}
				module := proposedEffectProofModule{
					source: source,
					workflow: runtimepipeline.NewWorkflowDefinition(workflowName, []runtimepipeline.WorkflowStage{
						{Name: "awaiting"},
						{Name: "ready", Terminal: true},
						{Name: "attention", Terminal: true},
					}, []runtimepipeline.WorkflowTransition{{
						Name: "complete-join", From: []runtimepipeline.WorkflowStateID{"awaiting"}, To: "ready",
						Trigger: "item.completed", Node: "join-node",
					}}),
					nodes: []runtimepipeline.WorkflowNode{{
						ID: "join-node", Subscriptions: []events.EventType{events.EventType(subscriptionType)},
						ExecutionType: runtimecontracts.SystemNodeExecutionType,
						Policies: map[string]runtimepipeline.WorkflowEventPolicy{
							subscriptionType: {Consume: true},
						},
					}},
				}
				probe := runtimelifecycleprobe.New()
				eventBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
					ContractBundle: source, TestLifecycleProbe: probe,
				}, additionalDescriptors...)
				if err != nil {
					t.Fatalf("new join EventBus: %v", err)
				}
				coordinator := newGateRecoveryCoordinator(eventBus, selected, runtimepipeline.PipelineCoordinatorOptions{
					Module: module, Persistence: selected.persistence,
					GenericSchedules: joinProofGenericScheduleWakeups{}, TestLifecycleProbe: probe,
				})
				eventBus.SetInterceptors(coordinator)

				route := runtimeflowidentity.RouteForInstancePath(path)
				entityID := runtimeflowidentity.EntityID(path)
				createdAt := time.Now().UTC()
				if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
					InstanceID: instanceID, StorageRef: path, WorkflowName: workflowName, WorkflowVersion: "1.0.0",
					CurrentState: "awaiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
					Metadata: map[string]any{
						"run_id": runID, "entity_id": entityID, "flow_path": path, "instance_id": route.InstanceID,
						"expected": []any{"a", "b"},
					},
				}, createdAt); err != nil {
					t.Fatalf("materialize exact join owner: %v", err)
				}
				if flowID != "" {
					if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: route}); err != nil {
						t.Fatalf("add flow join route: %v", err)
					}
				}

				eventsByMember := make([]events.Event, 0, 2)
				for _, member := range []string{"a", "b"} {
					wantTargetFlow := flowID
					envelope := events.EnvelopeForFlowInstance(
						events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), workflowName,
					)
					if flowID == "" {
						wantTargetFlow = source.WorkflowName()
					} else {
						envelope = events.EnvelopeForTargetRoute(
							events.EnvelopeForFlowInstance(envelope, path),
							events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: entityID},
						)
					}
					event := eventtest.ExistingRunRootIngress(
						uuid.NewString(), events.EventType(eventType), "operator", "",
						[]byte(`{"member_id":"`+member+`","result":{"value":"`+member+`"}}`),
						0, runID, envelope, time.Now().UTC(),
					)
					plan, err := eventBus.CheckPublishRecipientPlan(ctx, event)
					if err != nil {
						t.Fatalf("plan member %s: %v", member, err)
					}
					if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Recipient.ID() != "join-node" {
						t.Fatalf("member %s delivery plan = %#v", member, plan)
					}
					if target := plan.DeliveryRoutes[0].Target.Route(); target.FlowID != wantTargetFlow || target.FlowInstance != path || target.EntityID != entityID {
						t.Fatalf("member %s target = %#v, want flow=%q path=%q entity=%q", member, target, wantTargetFlow, path, entityID)
					}
					if err := eventBus.PublishAcknowledged(ctx, event); err != nil {
						t.Fatalf("publish member %s: %v", member, err)
					}
					waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
						cancel()
						t.Fatalf("wait member %s quiescence: %v", member, err)
					}
					cancel()
					assertExactJoinDeliveryStatus(t, selected, ctx, event.ID(), "delivered")
					eventsByMember = append(eventsByMember, event)
				}

				instance, found, err := coordinator.Load(ctx, route)
				if err != nil || !found || instance.CurrentState != "ready" {
					t.Fatalf("closed workflow = found:%v state:%q err:%v", found, instance.CurrentState, err)
				}
				carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
				if err != nil {
					t.Fatal(err)
				}
				joins, err := joinruntime.List(carrier.StateBuckets)
				if err != nil || len(joins) != 1 {
					t.Fatalf("join readback = %#v err=%v", joins, err)
				}
				if joins[0].Status != joinruntime.StatusClosed || joins[0].CloseReason != joinruntime.CloseReasonComplete ||
					joins[0].FlowID() != flowID || joins[0].Completed() != 2 || !joins[0].TimerCancelled {
					t.Fatalf("closed join = %#v", joins[0])
				}

				beforeReplayRevision := instance.Revision
				if err := eventBus.PublishAcknowledged(ctx, eventsByMember[1]); err != nil {
					t.Fatalf("replay exact member event: %v", err)
				}
				waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
					cancel()
					t.Fatalf("wait replay quiescence: %v", err)
				}
				cancel()
				afterReplay, found, err := coordinator.Load(ctx, route)
				if err != nil || !found || afterReplay.Revision != beforeReplayRevision {
					t.Fatalf("replay mutated workflow = found:%v before:%d after:%d err:%v", found, beforeReplayRevision, afterReplay.Revision, err)
				}
				assertExactJoinDeliveryCount(t, selected, ctx, eventsByMember[1].ID(), 1)
			})
		}
	}
}

func TestWorkflowJoinScheduleOccurrencePreservesExactDeclarationThroughDurableEventBusOnBothStores(t *testing.T) {
	for _, storeCase := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		for _, flowID := range []string{"", "orders"} {
			scope := "root"
			if flowID != "" {
				scope = "flow"
			}
			t.Run(storeCase.name+"/"+scope, func(t *testing.T) {
				selected := storeCase.open(t)
				store, ok := selected.events.(interface {
					runtimegenericschedule.Store
					runtimebus.PreparedPublishEventReader
				})
				if !ok {
					t.Fatalf("selected store %T lacks generic schedule or event readback ownership", selected.events)
				}
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				source := exactExternalWorkflowJoinSource(t, flowID)
				path := runID
				workflowName := source.WorkflowName()
				instanceID := runID
				if flowID != "" {
					instanceID = uuid.NewString()
					path = flowID + "/" + instanceID
					workflowName = flowID
				}

				module := proposedEffectProofModule{
					source: source,
					workflow: runtimepipeline.NewWorkflowDefinition(workflowName, []runtimepipeline.WorkflowStage{
						{Name: "awaiting"},
						{Name: "ready", Terminal: true},
						{Name: "attention", Terminal: true},
					}, []runtimepipeline.WorkflowTransition{{
						Name: "complete-join", From: []runtimepipeline.WorkflowStateID{"awaiting"}, To: "ready",
						Trigger: "item.completed", Node: "join-node",
					}}),
					nodes: []runtimepipeline.WorkflowNode{{
						ID: "join-node", ExecutionType: runtimecontracts.SystemNodeExecutionType,
						Subscriptions: []events.EventType{"item.completed"},
						Policies: map[string]runtimepipeline.WorkflowEventPolicy{
							"item.completed": {Consume: true},
						},
					}},
				}
				probe := runtimelifecycleprobe.New()
				eventBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
					ContractBundle: source, TestLifecycleProbe: probe,
				}, "platform.join_complete")
				if err != nil {
					t.Fatalf("new schedule occurrence EventBus: %v", err)
				}
				workOwner, ok := worklifetime.OccurrenceFromContext(ctx)
				if !ok {
					t.Fatal("join occurrence proof context lacks work owner")
				}
				scheduler := runtimepipeline.NewSchedulerWithWorkOwner(workOwner)
				lifecycle, err := runtimegenericschedule.NewLifecycle(
					store, scheduler, eventBus, eventBus.EngineDispatcher(), exactJoinScheduleLogger{t: t}, executionposture.Live,
				)
				if err != nil {
					t.Fatalf("new generic schedule lifecycle: %v", err)
				}
				t.Cleanup(func() {
					stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := lifecycle.Stop(stopCtx); err != nil {
						t.Errorf("stop generic schedule lifecycle: %v", err)
					}
				})
				coordinator := newGateRecoveryCoordinator(eventBus, selected, runtimepipeline.PipelineCoordinatorOptions{
					Module: module, Persistence: selected.persistence,
					GenericSchedules: lifecycle, TestLifecycleProbe: probe,
				})
				eventBus.SetInterceptors(coordinator)

				route := runtimeflowidentity.RouteForInstancePath(path)
				entityID := runtimeflowidentity.EntityID(path)
				createdAt := time.Now().UTC()
				if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
					InstanceID: instanceID, StorageRef: path, WorkflowName: workflowName, WorkflowVersion: "1.0.0",
					CurrentState: "awaiting", EnteredStageAt: createdAt, CreatedAt: createdAt,
					Metadata: map[string]any{
						"run_id": runID, "entity_id": entityID, "flow_path": path, "instance_id": route.InstanceID,
						"expected": []any{},
					},
				}, createdAt); err != nil {
					t.Fatalf("materialize immediate join owner: %v", err)
				}
				if flowID != "" {
					if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: route}); err != nil {
						t.Fatalf("add flow join route: %v", err)
					}
				}

				eventID := exactJoinOccurrenceEventID(t, selected, ctx, runID)
				waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
					cancel()
					t.Fatalf("wait occurrence delivery quiescence: %v", err)
				}
				cancel()
				assertExactJoinDeliveryStatus(t, selected, ctx, eventID, "delivered")
				instance := waitForExactJoinState(t, ctx, coordinator, route, "ready")
				carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
				if err != nil {
					t.Fatal(err)
				}
				joins, err := joinruntime.List(carrier.StateBuckets)
				if err != nil || len(joins) != 1 {
					t.Fatalf("join occurrence readback = %#v err=%v", joins, err)
				}
				if joins[0].Status != joinruntime.StatusClosed || joins[0].FlowID() != flowID ||
					joins[0].CloseReason != joinruntime.CloseReasonComplete || !joins[0].OutcomeFired {
					t.Fatalf("fired join occurrence = %#v", joins[0])
				}

				prepared, found, err := store.LoadPreparedPublishEvent(ctx, eventID)
				if err != nil || !found {
					t.Fatalf("load persisted join occurrence = found:%v err:%v", found, err)
				}
				beforeReplayRevision := instance.Revision
				if err := eventBus.PublishAcknowledged(ctx, prepared.Event.Event()); err != nil {
					t.Fatalf("replay persisted join occurrence: %v", err)
				}
				waitCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
				if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
					cancel()
					t.Fatalf("wait occurrence replay quiescence: %v", err)
				}
				cancel()
				afterReplay, found, err := coordinator.Load(ctx, route)
				if err != nil || !found || afterReplay.Revision != beforeReplayRevision {
					t.Fatalf("occurrence replay mutated workflow = found:%v before:%d after:%d err:%v", found, beforeReplayRevision, afterReplay.Revision, err)
				}
				assertExactJoinDeliveryCount(t, selected, ctx, eventID, 1)
			})
		}
	}
}

func exactExternalWorkflowJoinSource(t *testing.T, flowID string) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	fixtureRoot := canonicalrouting.CopyExactJoinEventBusProof(t, flowID)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		fixtureRoot,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load exact join EventBus fixture: %v", err)
	}
	spec := runtimecontracts.JoinSpec{
		ID: "awaiting", Stage: "awaiting",
		Members: runtimecontracts.JoinMembersSpec{
			From: "entity.expected", FromPath: runtimepaths.Parse("entity.expected"),
			By: "payload.member_id", ByPath: runtimepaths.Parse("payload.member_id"),
		},
		Output: "payload.result", OutputPath: runtimepaths.Parse("payload.result"),
		OnCompleteFound: true, OnComplete: runtimecontracts.HandlerRuleEntry{AdvancesTo: "ready"},
		TimeoutFound: true, Timeout: runtimecontracts.JoinTimeoutSpec{
			After: "1h", Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "attention"},
		},
	}
	plans := []runtimecontracts.WorkflowJoinPlan{{
		FlowID: flowID, NodeID: "join-node", HandlerEvent: "item.completed", Spec: spec,
		ResultType: runtimecontracts.CatalogTypeReference{Type: "jsonb"},
	}}
	bundle.Semantics.Joins = plans
	return exactExternalJoinSource{Source: semanticview.Wrap(bundle), flowID: flowID, plans: plans}
}

func assertExactJoinDeliveryStatus(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, eventID, want string) {
	t.Helper()
	query := `SELECT status, CAST(COALESCE(failure, '{}') AS TEXT) FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'join-node'`
	if selected.postgres {
		query = `SELECT status, COALESCE(failure, '{}'::jsonb)::text FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'join-node'`
	}
	var status, failure string
	if err := selected.db.QueryRowContext(ctx, query, eventID).Scan(&status, &failure); err != nil || status != want {
		var runtimeLog string
		logQuery := `SELECT CAST(payload AS TEXT) FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at DESC LIMIT 1`
		_ = selected.db.QueryRowContext(ctx, logQuery).Scan(&runtimeLog)
		t.Fatalf("delivery %s status = %q failure=%s err=%v, want %q; runtime_log=%s", eventID, status, failure, err, want, runtimeLog)
	}
}

func assertExactJoinDeliveryCount(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, eventID string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'join-node'`
	if selected.postgres {
		query = `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'join-node'`
	}
	var count int
	if err := selected.db.QueryRowContext(ctx, query, eventID).Scan(&count); err != nil || count != want {
		t.Fatalf("delivery %s rows = %d err=%v, want %d", eventID, count, err, want)
	}
}

func waitForExactJoinState(
	t *testing.T,
	ctx context.Context,
	coordinator *runtimepipeline.PipelineCoordinator,
	route runtimeflowidentity.Route,
	want string,
) runtimepipeline.WorkflowInstance {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		instance, found, err := coordinator.Load(ctx, route)
		if err == nil && found && instance.CurrentState == want {
			return instance
		}
		time.Sleep(10 * time.Millisecond)
	}
	instance, found, err := coordinator.Load(ctx, route)
	t.Fatalf("workflow did not reach %q = found:%v state:%q err:%v", want, found, instance.CurrentState, err)
	return runtimepipeline.WorkflowInstance{}
}

func exactJoinOccurrenceEventID(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, runID string) string {
	t.Helper()
	query := `SELECT event_id FROM events WHERE run_id = ? AND event_name = 'platform.join_complete'`
	if selected.postgres {
		query = `SELECT event_id::text FROM events WHERE run_id = $1::uuid AND event_name = 'platform.join_complete'`
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var eventID string
		if err := selected.db.QueryRowContext(ctx, query, runID).Scan(&eventID); err == nil {
			return eventID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("durable join occurrence event was not published")
	return ""
}
