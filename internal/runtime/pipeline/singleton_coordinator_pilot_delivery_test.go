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
	"github.com/division-sh/swarm/internal/runtime/testfixtures/singletoncoordinatorpilot"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSingletonCoordinatorPilotPipelineDispatchPersistsContainedStateReadback(t *testing.T) {
	bundle := singletoncoordinatorpilot.LoadBundle(t, singletoncoordinatorpilot.Options{})
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pc, workflowStore := newSingletonCoordinatorPilotPipelineCoordinator(t, db, bundle, source)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	entityID := FlowInstanceEntityID(singletoncoordinatorpilot.FlowInstance)
	seedSingletonCoordinatorPilotInstance(t, workflowStore, ctx, bundle, entityID)

	target := events.RouteIdentity{
		FlowID:       singletoncoordinatorpilot.FlowID,
		FlowInstance: singletoncoordinatorpilot.FlowInstance,
		EntityID:     entityID,
	}
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType(singletoncoordinatorpilot.InputEvent),
		singletoncoordinatorpilot.FlowID,
		"",
		json.RawMessage(`{"coordinator_id":"global","lead_id":"lead-42","observation":{"source":"feed","note":"first seen"},"audit":{"ref":"lead-42","action":"observed"},"followup_audit":{"ref":"lead-42","action":"queued"},"corrected_audit":{"ref":"bootstrap","action":"corrected"}}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, target),
		time.Now().UTC(),
	)
	seedSingletonCoordinatorPilotEvent(t, db, ctx, evt)
	seedSingletonCoordinatorPilotNodeDelivery(t, db, ctx, evt.ID(), singletoncoordinatorpilot.NodeID, target)

	handled, err := pc.dispatchWorkflowNodeEventResult(ctx, evt)
	if err != nil {
		t.Fatalf("dispatchWorkflowNodeEventResult: %v", err)
	}
	if !handled {
		t.Fatal("dispatchWorkflowNodeEventResult handled = false, want coordinator handler delivery")
	}
	loaded, ok, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
	}
	if loaded.WorkflowName != singletoncoordinatorpilot.FlowID || loaded.CurrentState != "active" {
		t.Fatalf("loaded singleton coordinator = storage:%q workflow:%q state:%q, want coordinator/active", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState)
	}
	leadIndex, ok := loaded.Metadata["lead_index"].(map[string]any)
	if !ok {
		t.Fatalf("lead_index = %#v, want map", loaded.Metadata["lead_index"])
	}
	lead, ok := leadIndex["lead-42"].(map[string]any)
	if !ok {
		t.Fatalf("lead_index[lead-42] = %#v, want map", leadIndex["lead-42"])
	}
	if lead["status"] != "active" || lead["score"] != float64(1) {
		t.Fatalf("lead_index[lead-42] = %#v, want status active score 1", lead)
	}
	observations, ok := lead["observations"].([]any)
	if !ok || len(observations) != 1 {
		t.Fatalf("lead observations = %#v, want one observation", lead["observations"])
	}
	observation, ok := observations[0].(map[string]any)
	if !ok || observation["source"] != "feed" || observation["note"] != "first seen" {
		t.Fatalf("observation = %#v, want feed/first seen", observations[0])
	}
	auditLog, ok := loaded.Metadata["audit_log"].([]any)
	if !ok || len(auditLog) != 3 {
		t.Fatalf("audit_log = %#v, want three entries", loaded.Metadata["audit_log"])
	}
	firstAudit, ok := auditLog[0].(map[string]any)
	if !ok || firstAudit["ref"] != "bootstrap" || firstAudit["action"] != "corrected" {
		t.Fatalf("audit_log[0] = %#v, want corrected bootstrap entry", auditLog[0])
	}
	secondAudit, ok := auditLog[1].(map[string]any)
	if !ok || secondAudit["ref"] != "lead-42" || secondAudit["action"] != "observed" {
		t.Fatalf("audit_log[1] = %#v, want observed lead-42 entry", auditLog[1])
	}
	thirdAudit, ok := auditLog[2].(map[string]any)
	if !ok || thirdAudit["ref"] != "lead-42" || thirdAudit["action"] != "queued" {
		t.Fatalf("audit_log[2] = %#v, want queued lead-42 entry", auditLog[2])
	}
	assertSingletonCoordinatorPilotDeliveryStatus(t, db, evt.ID(), singletoncoordinatorpilot.NodeID, "delivered")
	assertNoSingletonCoordinatorPilotContainedRouteRows(t, db, "coordinator/lead-42")
	if _, ok, err := workflowStore.Load(ctx, "lead-42"); err != nil {
		t.Fatalf("workflowStore.Load(lead-42): %v", err)
	} else if ok {
		t.Fatal("contained map key lead-42 materialized as a workflow instance")
	}
}

func TestSingletonCoordinatorPilotPipelineRejectsContainedItemDeliveryTarget(t *testing.T) {
	bundle := singletoncoordinatorpilot.LoadBundle(t, singletoncoordinatorpilot.Options{})
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pc, _ := newSingletonCoordinatorPilotPipelineCoordinator(t, db, bundle, source)

	containedTarget := events.RouteIdentity{
		FlowID:       singletoncoordinatorpilot.FlowID,
		FlowInstance: singletoncoordinatorpilot.FlowInstance + "/lead-42",
		EntityID:     uuid.NewString(),
	}
	if pc.workflowNodeMatchesDeliveryTarget(singletoncoordinatorpilot.NodeID, containedTarget) {
		t.Fatalf("contained item target %#v matched singleton coordinator node; contained map entries must not be route recipients", containedTarget)
	}
}

func newSingletonCoordinatorPilotPipelineCoordinator(t *testing.T, db *sql.DB, bundle *runtimecontracts.WorkflowContractBundle, source semanticview.Source) (*PipelineCoordinator, *WorkflowInstanceStore) {
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

func seedSingletonCoordinatorPilotInstance(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, bundle *runtimecontracts.WorkflowContractBundle, entityID string) {
	t.Helper()
	if err := store.Create(ctx, WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      singletoncoordinatorpilot.FlowInstance,
		WorkflowName:    singletoncoordinatorpilot.FlowID,
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "active",
		Metadata: map[string]any{
			"entity_id":      entityID,
			"flow_path":      singletoncoordinatorpilot.FlowInstance,
			"instance_id":    entityID,
			"coordinator_id": "global",
			"lead_index":     map[string]any{},
			"audit_log": []any{
				map[string]any{"ref": "seed", "action": "seed"},
			},
		},
	}); err != nil {
		t.Fatalf("seed singleton coordinator workflow instance: %v", err)
	}
}

func seedSingletonCoordinatorPilotEvent(t *testing.T, db *sql.DB, ctx context.Context, evt events.Event) {
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
		t.Fatalf("seed singleton coordinator pilot event: %v", err)
	}
}

func seedSingletonCoordinatorPilotNodeDelivery(t *testing.T, db *sql.DB, ctx context.Context, eventID, nodeID string, target events.RouteIdentity) {
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
		t.Fatalf("seed singleton coordinator pilot node delivery: %v", err)
	}
}

func assertSingletonCoordinatorPilotDeliveryStatus(t *testing.T, db *sql.DB, eventID, nodeID, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(status, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodeID).Scan(&got); err != nil {
		t.Fatalf("load singleton coordinator pilot node delivery: %v", err)
	}
	if got != want {
		t.Fatalf("singleton coordinator pilot delivery status = %q, want %q", got, want)
	}
}

func assertNoSingletonCoordinatorPilotContainedRouteRows(t *testing.T, db *sql.DB, flowInstance string) {
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
