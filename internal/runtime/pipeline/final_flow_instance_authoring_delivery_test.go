package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestFinalFlowInstanceAuthoringFixturePipelineDispatchLocalizesTemplateInputConnectEvent(t *testing.T) {
	bundle := finalflowinstanceauthoring.LoadBundle(t, finalflowinstanceauthoring.Options{})
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pc, workflowStore := newFinalFlowInstanceAuthoringPipelineCoordinator(t, db, bundle, source)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	instanceID := "ti-account-42"
	flowInstance := finalflowinstanceauthoring.TemplateFlowID + "/" + instanceID
	entityID := FlowInstanceEntityID(flowInstance)
	if err := workflowStore.Create(ctx, WorkflowInstance{
		InstanceID:      instanceID,
		StorageRef:      flowInstance,
		WorkflowName:    finalflowinstanceauthoring.TemplateFlowID,
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
		Metadata: map[string]any{
			"entity_id":   entityID,
			"flow_path":   flowInstance,
			"instance_id": instanceID,
			"account_id":  "acct-42",
		},
	}); err != nil {
		t.Fatalf("seed account_case workflow instance: %v", err)
	}

	target := events.RouteIdentity{
		FlowID:       finalflowinstanceauthoring.TemplateFlowID,
		FlowInstance: flowInstance,
		EntityID:     entityID,
	}
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType(finalflowinstanceauthoring.ProducerFlowID+"/"+finalflowinstanceauthoring.ProducerOutput),
		finalflowinstanceauthoring.ProducerFlowID,
		"",
		json.RawMessage(`{"account_id":"acct-42","score":"91","decision":"approved"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, target),
		time.Now().UTC(),
	)
	seedFinalFlowInstanceAuthoringEvent(t, db, ctx, evt)
	seedFinalFlowInstanceAuthoringNodeDelivery(t, db, ctx, evt.ID(), finalflowinstanceauthoring.TemplateNodeID, target)

	handled, err := pc.dispatchWorkflowNodeEventResult(ctx, evt)
	if err != nil {
		t.Fatalf("dispatchWorkflowNodeEventResult: %v", err)
	}
	if !handled {
		t.Fatal("dispatchWorkflowNodeEventResult handled = false, want account_case handler delivery")
	}
	loaded, ok, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
	}
	if loaded.WorkflowName != finalflowinstanceauthoring.TemplateFlowID || loaded.CurrentState != "reviewed" {
		t.Fatalf("loaded account_case = storage:%q workflow:%q state:%q, want account_case/reviewed", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState)
	}
	if loaded.Metadata["account_id"] != "acct-42" || loaded.Metadata["score"] != "91" || loaded.Metadata["decision"] != "approved" {
		t.Fatalf("loaded account_case metadata = %#v, want account_id/score/decision from routed payload", loaded.Metadata)
	}
	assertFinalFlowInstanceAuthoringDeliveryStatus(t, db, evt.ID(), finalflowinstanceauthoring.TemplateNodeID, "delivered")
}

func newFinalFlowInstanceAuthoringPipelineCoordinator(t *testing.T, db *sql.DB, bundle *runtimecontracts.WorkflowContractBundle, source semanticview.Source) (*PipelineCoordinator, *WorkflowInstanceStore) {
	t.Helper()
	workflow, err := LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	workflowStore := NewWorkflowInstanceStore(db)
	pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{
			bundle:         bundle,
			workflow:       workflow,
			workflowNodes:  nodes,
			guardRegistry:  NewContractGuardRegistry(source),
			actionRegistry: NewContractActionRegistry(source),
		},
		WorkflowStore:           workflowStore,
		EventReceiptsCapability: eventReceiptsCapabilityStub{enabled: true}.resolve,
	})
	return pc, workflowStore
}

func seedFinalFlowInstanceAuthoringEvent(t *testing.T, db *sql.DB, ctx context.Context, evt events.Event) {
	t.Helper()
	targetRaw, err := json.Marshal(evt.TargetRoute())
	if err != nil {
		t.Fatalf("marshal target route: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, entity_id, flow_instance, scope, payload,
			produced_by, produced_by_type, target_route, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3, $4::uuid, $5, 'entity', $6::jsonb,
			$7, 'agent', $8::jsonb, now()
		)
	`, evt.ID(), evt.RunID(), string(evt.Type()), evt.EntityID(), evt.FlowInstance(), string(evt.Payload()), evt.SourceAgent(), string(targetRaw)); err != nil {
		t.Fatalf("seed final flow-instance authoring event: %v", err)
	}
}

func seedFinalFlowInstanceAuthoringNodeDelivery(t *testing.T, db *sql.DB, ctx context.Context, eventID, nodeID string, target events.RouteIdentity) {
	t.Helper()
	targetRaw, err := json.Marshal(target.Normalized())
	if err != nil {
		t.Fatalf("marshal delivery target route: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, delivery_target_route, status, retry_count, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'node', $3, $4::jsonb, 'pending', 0, now()
		)
	`, testPipelineRunID, eventID, nodeID, string(targetRaw)); err != nil {
		t.Fatalf("seed final flow-instance authoring node delivery: %v", err)
	}
}

func assertFinalFlowInstanceAuthoringDeliveryStatus(t *testing.T, db *sql.DB, eventID, nodeID, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(status, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodeID).Scan(&got); err != nil {
		t.Fatalf("load final flow-instance authoring node delivery: %v", err)
	}
	if got != want {
		t.Fatalf("final flow-instance authoring delivery status = %q, want %q", got, want)
	}
}

func assertNoFinalFlowInstanceAuthoringContainedRouteRows(t *testing.T, db *sql.DB, flowInstance string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE delivery_target_route->>'flow_instance' = $1
	`, flowInstance).Scan(&count); err != nil {
		t.Fatalf("count contained route delivery rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("contained flow_instance %q has %d delivery row(s), want none", flowInstance, count)
	}
}

func assertNoFinalFlowInstanceAuthoringContainedWorkflowInstance(t *testing.T, db *sql.DB, store *WorkflowInstanceStore, ctx context.Context, flowInstance string) {
	t.Helper()
	entityID := FlowInstanceEntityID(flowInstance)
	if _, ok, err := store.Load(ctx, entityID); err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
	} else if ok {
		t.Fatalf("contained flow_instance %q materialized with canonical entity id %s", flowInstance, entityID)
	}
	if _, ok, err := store.Load(ctx, flowInstance); err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", flowInstance, err)
	} else if ok {
		t.Fatalf("contained flow_instance %q materialized through storage-ref lookup", flowInstance)
	}

	assertNoFinalFlowInstanceAuthoringRow(t, db, `
		SELECT COUNT(*)
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
	`, testPipelineRunID, entityID)
	assertNoFinalFlowInstanceAuthoringRow(t, db, `
		SELECT COUNT(*)
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND flow_instance = $2
	`, testPipelineRunID, flowInstance)
	assertNoFinalFlowInstanceAuthoringRow(t, db, `
		SELECT COUNT(*)
		FROM flow_instances
		WHERE instance_id = $1
	`, flowInstance)
}

func assertNoFinalFlowInstanceAuthoringRow(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("count final flow-instance authoring rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("final flow-instance authoring absence query returned %d row(s), want none", count)
	}
}
