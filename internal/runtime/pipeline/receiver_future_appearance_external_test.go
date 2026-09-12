package pipeline_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestReceiverCompositionActivationReuseAndConflictBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, conflict := range []bool{false, true} {
			name := "exact_receiver_reuse"
			if conflict {
				name = "conflicting_entity_type"
			}
			t.Run(backend.name+"/"+name, func(t *testing.T) {
				selected := backend.open(t)
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				source, node := targetedDeclaredKeyExecutionSource(t, "select_or_create")
				module := proposedEffectProofModule{source: source,
					workflow: runtimepipeline.NewWorkflowDefinition("review", []runtimepipeline.WorkflowStage{{Name: "active"}, {Name: "done", Terminal: true}}, nil),
					nodes: []runtimepipeline.WorkflowNode{{Node: node, Subscriptions: []events.EventType{"work.keyed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType,
						Policies: map[string]runtimepipeline.WorkflowEventPolicy{"work.keyed": {Consume: true}}}},
				}
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source})
				if err != nil {
					t.Fatal(err)
				}
				pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: module})
				fact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
				if !ok {
					t.Fatal("missing admitted source")
				}
				// A real live template supplies the subscriber, but its unrelated key
				// must not become the receiver for this zero-match delivery.
				unrelatedPath := "review/" + uuid.NewString()
				unrelatedRoute := runtimeflowidentity.RouteForInstancePath(unrelatedPath)
				unrelatedIdentity := testRunScopedWorkflowInstanceForRun(runID, unrelatedPath)
				unrelatedReadiness := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
					Identity: runtimeflowidentity.Instance{TemplateID: "review", ScopeKey: "review", InstanceID: unrelatedRoute.InstanceID, InstancePath: unrelatedPath, EntityID: runtimeflowidentity.EntityID(unrelatedPath), HasStoredPath: true},
					RunID:    runID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live,
				}
				if _, err := pc.MaterializeInitialEntry(ctx, unrelatedIdentity, runtimepipeline.WorkflowInstance{
					InstanceID: unrelatedRoute.InstanceID, StorageRef: unrelatedPath, EntityID: runtimeflowidentity.EntityID(unrelatedPath),
					WorkflowName: "review", WorkflowVersion: source.WorkflowVersion(), Mode: "template", CurrentState: "active", EntityType: "review_entity",
					Fields: map[string]any{"receiver_id": unrelatedRoute.InstanceID, "account_id": "unrelated-key"}, RuntimeReadiness: &unrelatedReadiness,
				}, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				if err := pc.MarkDynamicFlowRuntimeTopologyReady(ctx, unrelatedReadiness, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				if err := bus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: unrelatedIdentity}); err != nil {
					t.Fatal(err)
				}
				receiverKey := uuid.NewString()
				payload, err := json.Marshal(map[string]any{"receiver_id": receiverKey, "account_id": "appearing-key", "item": "accepted"})
				if err != nil {
					t.Fatal(err)
				}
				evt := targetedDeclaredKeyPublication(t, ctx, bus, source, runID, payload, events.RouteIdentity{}, time.Now().UTC())
				instancePath := "review/" + receiverKey
				route := runtimeflowidentity.RouteForInstancePath(instancePath)
				identity := testRunScopedWorkflowInstanceForRun(runID, instancePath)
				entityID := runtimeflowidentity.EntityID(instancePath)
				readiness := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
					Identity: runtimeflowidentity.Instance{TemplateID: "review", ScopeKey: "review", InstanceID: route.InstanceID, InstancePath: instancePath, EntityID: entityID, HasStoredPath: true},
					RunID:    runID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live,
				}
				instance := runtimepipeline.WorkflowInstance{InstanceID: route.InstanceID, StorageRef: instancePath, EntityID: entityID,
					WorkflowName: "review", WorkflowVersion: source.WorkflowVersion(), Mode: "template", CurrentState: "active", EntityType: "review_entity",
					Fields: map[string]any{"receiver_id": receiverKey, "account_id": "stored-business-key", "owner": "appeared"}, RuntimeReadiness: &readiness}
				// Composition activation establishes the receiver before handler execution.
				// There is no lawful second, payload-derived future receiver to invent.
				if _, err := pc.MaterializeInitialEntry(ctx, identity, instance, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				if err := pc.MarkDynamicFlowRuntimeTopologyReady(ctx, readiness, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				if err := bus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}); err != nil {
					t.Fatal(err)
				}
				evt, err = events.ResolveEnvelope(evt, events.EnvelopeForTargetRoute(evt.NormalizedEnvelope(), events.RouteIdentity{FlowID: "review", FlowInstance: instancePath, EntityID: entityID}))
				if err != nil {
					t.Fatal(err)
				}
				plan, err := bus.CheckPublishRecipientPlan(ctx, evt)
				if err != nil || len(plan.DeliveryRoutes) != 1 || !plan.DeliveryRoutes[0].Target.ExistingEntity() {
					t.Fatalf("composition receiver admission: plan=%#v err=%v", plan, err)
				}
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatal(err)
				}
				prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, evt.ID())
				if err != nil || !found || len(prepared.DeliveryRoutes) != 1 {
					t.Fatalf("committed receiver: found=%t err=%v routes=%#v", found, err, prepared.DeliveryRoutes)
				}
				target := prepared.DeliveryRoutes[0].Target
				if target != plan.DeliveryRoutes[0].Target {
					t.Fatal("publish changed receiver admission")
				}
				initial, found, err := pc.Load(ctx, identity)
				if err != nil || !found || initial.EntityID != target.Route().EntityID || initial.Revision != 1 {
					t.Fatalf("composition activation missing: %#v %t %v", initial, found, err)
				}
				if conflict {
					if _, err := selected.db.ExecContext(ctx, `UPDATE entity_state SET entity_type=$1 WHERE run_id=$2 AND entity_id=$3`, "wrong_entity_type", runID, entityID); err != nil {
						t.Fatal(err)
					}
				}
				// Reconstruct the semantic consumer after publication, retaining the
				// same durable admitted route rather than performing another election.
				pc = newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: module})
				if err := bus.Publish(ctx, evt); err != nil {
					t.Fatalf("exact duplicate after appearance: %v", err)
				}
				delivery, err := events.NewDeliveryEvent(prepared.Event.Event(), prepared.DeliveryRoutes[0])
				if err != nil {
					t.Fatal(err)
				}
				forward, _, outcome, executionErr := pc.InterceptDeliveryRoute(ctx, delivery, prepared.DeliveryRoutes[0])
				if forward || executionErr != nil {
					t.Fatalf("execute committed receiver: forward=%t err=%v", forward, executionErr)
				}
				after, found, err := pc.Load(ctx, identity)
				if err != nil || !found || after.EntityID != target.Route().EntityID || after.Fields["account_id"] != "stored-business-key" || after.Fields["owner"] != "appeared" {
					t.Fatalf("exact appearance changed: %#v found=%t err=%v", after, found, err)
				}
				disposition, disposed := outcome.Disposition()
				if conflict {
					if !disposed || disposition.Kind() != runtimepipelineobligation.DispositionDeadLetter || after.Revision != 1 {
						t.Fatalf("conflicting appearance executed: outcome=%#v revision=%d", outcome, after.Revision)
					}
				} else if disposed || after.Revision != 2 {
					t.Fatalf("composition-selected receiver not executed exactly once: outcome=%#v revision=%d", outcome, after.Revision)
				}
				reloaded, found, err := selected.events.LoadPreparedPublishEvent(ctx, evt.ID())
				if err != nil || !found || len(reloaded.DeliveryRoutes) != 1 || reloaded.DeliveryRoutes[0].Target != target {
					t.Fatalf("appearance rewrote committed receiver: found=%t err=%v routes=%#v", found, err, reloaded.DeliveryRoutes)
				}
				unrelated, found, err := pc.Load(ctx, unrelatedIdentity)
				if err != nil || !found || unrelated.Revision != 1 || unrelated.Fields["account_id"] != "unrelated-key" {
					t.Fatalf("appearance mutated unrelated owner: %#v %t %v", unrelated, found, err)
				}
				var status string
				var outcomes int
				if err := selected.db.QueryRow(`SELECT d.status, (SELECT count(*) FROM event_delivery_outcomes o WHERE o.delivery_id=d.delivery_id) FROM event_deliveries d WHERE d.event_id=$1`, evt.ID()).Scan(&status, &outcomes); err != nil {
					t.Fatal(err)
				}
				wantStatus := "delivered"
				if conflict {
					wantStatus = "dead_letter"
				}
				if status != wantStatus || outcomes != 1 {
					t.Fatalf("appearance settlement: %s/%d want %s/1", status, outcomes, wantStatus)
				}
			})
		}
	}
}
