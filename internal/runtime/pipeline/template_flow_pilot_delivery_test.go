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
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestTemplateFlowPilotPipelineDispatchUpdatesSelectedTemplateInstance(t *testing.T) {
	bundle := templateflowpilot.LoadBundle(t, templateflowpilot.Options{})
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	t.Cleanup(cleanup)
	pc, workflowStore := newTemplateFlowPilotPipelineCoordinator(t, db, bundle, source)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	entityID := uuid.NewString()
	instanceID := "ti-template-flow-pilot"
	flowInstance := "scoring/" + instanceID
	if err := workflowStore.Create(ctx, WorkflowInstance{
		InstanceID:      instanceID,
		StorageRef:      flowInstance,
		WorkflowName:    "scoring",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
		Config: map[string]any{
			"account_id": "acct-1",
		},
		Metadata: map[string]any{
			"entity_id":   entityID,
			"flow_path":   flowInstance,
			"instance_id": instanceID,
		},
	}); err != nil {
		t.Fatalf("seed scoring workflow instance: %v", err)
	}

	target := events.RouteIdentity{FlowID: "scoring", FlowInstance: flowInstance, EntityID: entityID}
	evt := eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("producer/validation.requested"),
		"producer",
		"",
		json.RawMessage(`{"account_id":"acct-1","score":"91","decision":"approved"}`),
		0,
		testPipelineRunID,
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, target),
		time.Now().UTC(),
	)
	seedTemplateFlowPilotPipelineEvent(t, db, ctx, evt)
	seedTemplateFlowPilotPipelineNodeDelivery(t, db, ctx, evt.ID(), "scoring-handler", target)

	handled, err := pc.dispatchWorkflowNodeEventResult(ctx, evt)
	if err != nil {
		t.Fatalf("dispatchWorkflowNodeEventResult: %v", err)
	}
	if !handled {
		t.Fatal("dispatchWorkflowNodeEventResult handled = false, want scoring handler delivery")
	}
	loaded, ok, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
	}
	if loaded.WorkflowName != "scoring" || loaded.CurrentState != "done" {
		t.Fatalf("loaded scoring instance = storage:%q workflow:%q state:%q, want scoring/done", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState)
	}
	if loaded.Metadata["account_id"] != "acct-1" || loaded.Metadata["score"] != "91" || loaded.Metadata["decision"] != "approved" {
		t.Fatalf("loaded scoring metadata = %#v, want account_id/score/decision from routed payload", loaded.Metadata)
	}
	assertTemplateFlowPilotPipelineDeliveryStatus(t, db, evt.ID(), "scoring-handler", "delivered")
}

func newTemplateFlowPilotPipelineCoordinator(t *testing.T, db *sql.DB, bundle *runtimecontracts.WorkflowContractBundle, source semanticview.Source) (*PipelineCoordinator, *WorkflowInstanceStore) {
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

func seedTemplateFlowPilotPipelineEvent(t *testing.T, db *sql.DB, ctx context.Context, evt events.Event) {
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
		t.Fatalf("seed template-flow pilot event: %v", err)
	}
}

func seedTemplateFlowPilotPipelineNodeDelivery(t *testing.T, db *sql.DB, ctx context.Context, eventID, nodeID string, target events.RouteIdentity) {
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
		t.Fatalf("seed template-flow pilot node delivery: %v", err)
	}
}

func assertTemplateFlowPilotPipelineDeliveryStatus(t *testing.T, db *sql.DB, eventID, nodeID, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(status, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodeID).Scan(&got); err != nil {
		t.Fatalf("load template-flow pilot node delivery: %v", err)
	}
	if got != want {
		t.Fatalf("template-flow pilot delivery status = %q, want %q", got, want)
	}
}
