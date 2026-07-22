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
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pc, workflowStore := newTemplateFlowPilotPipelineCoordinator(t, db, bundle, source)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	entityID := uuid.NewString()
	instanceID := "ti-template-flow-pilot"
	flowInstance := "account/" + instanceID
	if err := workflowStore.Create(ctx, WorkflowInstance{
		InstanceID:      instanceID,
		StorageRef:      flowInstance,
		WorkflowName:    "account",
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

	target := events.RouteIdentity{FlowID: "account", FlowInstance: flowInstance, EntityID: entityID}
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("producer/account.ready"),
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
	seedTemplateFlowPilotPipelineNodeDelivery(t, db, ctx, evt.ID(), "account-node", target)
	route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "account-node", Target: target}

	handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
	if err != nil {
		t.Fatalf("dispatchWorkflowNodeEventResult: %v", err)
	}
	if !handled {
		t.Fatal("dispatchWorkflowNodeEventResult handled = false, want account handler delivery")
	}
	loaded, ok, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
	}
	if loaded.WorkflowName != "account" || loaded.CurrentState != "done" {
		t.Fatalf("loaded account instance = storage:%q workflow:%q state:%q, want account/done", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState)
	}
	if loaded.Metadata["account_id"] != "acct-1" || loaded.Metadata["score"] != "91" || loaded.Metadata["decision"] != "approved" {
		t.Fatalf("loaded account metadata = %#v, want account_id/score/decision from routed payload", loaded.Metadata)
	}
	assertTemplateFlowPilotPipelineDeliveryStatus(t, db, evt.ID(), "account-node", "delivered")
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
	deliveryStore := newPipelineTestDeliveryOwnerForDB(t, db)
	pc := NewPipelineCoordinatorWithOptions(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{
			bundle:         bundle,
			workflow:       workflow,
			workflowNodes:  nodes,
			guardRegistry:  NewContractGuardRegistry(source),
			actionRegistry: NewContractActionRegistry(source),
		},
		WorkflowStore: workflowStore,
		DeliveryStore: deliveryStore,
	})
	return pc, workflowStore
}

func seedTemplateFlowPilotPipelineEvent(t *testing.T, db *sql.DB, ctx context.Context, evt events.Event) {
	t.Helper()
	seedPipelineEventRecord(t, ctx, db, evt)
}

func seedTemplateFlowPilotPipelineNodeDelivery(t *testing.T, db *sql.DB, ctx context.Context, eventID, nodeID string, target events.RouteIdentity) {
	t.Helper()
	seedPipelineTestNodeDelivery(t, ctx, db, eventID, nodeID, target)
}

func assertTemplateFlowPilotPipelineDeliveryStatus(t *testing.T, db *sql.DB, eventID, nodeID, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
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
