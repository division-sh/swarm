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
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
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
	if err := workflowStore.create(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      instanceID,
		StorageRef:      flowInstance,
		EntityID:        entityID,
		WorkflowName:    finalflowinstanceauthoring.TemplateFlowID,
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "pending",
		Fields:          map[string]any{"account_id": "acct-42"},
		EntityType:      "account_state",
	})); err != nil {
		t.Fatalf("seed account_case workflow instance: %v", err)
	}

	target := events.RouteIdentity{
		FlowID:       finalflowinstanceauthoring.TemplateFlowID,
		FlowInstance: flowInstance,
		EntityID:     entityID,
	}
	evt := eventtest.RunCreatingRootIngress(
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
	node := pipelineSourceNode(t, source, finalflowinstanceauthoring.TemplateFlowID, finalflowinstanceauthoring.TemplateNodeID)
	seedFinalFlowInstanceAuthoringNodeDelivery(t, db, ctx, evt.ID(), node, target)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)}

	handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
	if err != nil {
		t.Fatalf("dispatchWorkflowNodeEventResult: %v", err)
	}
	if !handled {
		t.Fatal("dispatchWorkflowNodeEventResult handled = false, want account_case handler delivery")
	}
	loaded, ok, err := workflowStore.Load(ctx, testWorkflowInstanceRoute(flowInstance))
	if err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
	}
	if loaded.WorkflowName != finalflowinstanceauthoring.TemplateFlowID || loaded.CurrentState != "reviewed" {
		t.Fatalf("loaded account_case = storage:%q workflow:%q state:%q, want account_case/reviewed", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState)
	}
	if loaded.Fields["account_id"] != "acct-42" || loaded.Fields["score"] != "91" || loaded.Fields["decision"] != "approved" {
		t.Fatalf("loaded account_case fields = %#v, want account_id/score/decision from routed payload", loaded.Fields)
	}
	assertFinalFlowInstanceAuthoringDeliveryStatus(t, db, evt.ID(), node.Key(), "delivered")
}

func newFinalFlowInstanceAuthoringPipelineCoordinator(t *testing.T, db *sql.DB, bundle *runtimecontracts.WorkflowContractBundle, source semanticview.Source) (*PipelineCoordinator, *workflowInstanceStore) {
	t.Helper()
	workflow, err := LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	workflowStore := newPostgresWorkflowInstanceStoreForTest(db)
	deliveryStore := newPipelineTestDeliveryOwnerForDB(t, db)
	bus := &recordingPipelineBus{}
	bus.configurePipelineTestDeliveryOwner(deliveryStore)
	pc := newDurablePipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{
		Module: &previewWorkflowModule{
			bundle:         bundle,
			workflow:       workflow,
			workflowNodes:  nodes,
			guardRegistry:  NewContractGuardRegistry(source),
			actionRegistry: NewContractActionRegistry(source),
		},
		Persistence:         workflowPersistenceForTest(workflowStore),
		DeliveryStore:       deliveryStore,
		DeliveryRuntime:     bus,
		PipelineObligations: unavailablePipelineTestObligationOwner{},
	})
	return pc, workflowStore
}

func seedFinalFlowInstanceAuthoringEvent(t *testing.T, db *sql.DB, ctx context.Context, evt events.Event) {
	t.Helper()
	seedPipelineEventRecord(t, ctx, db, evt)
}

func seedFinalFlowInstanceAuthoringNodeDelivery(t *testing.T, db *sql.DB, ctx context.Context, eventID string, node runtimeidentity.ExecutableNode, target events.RouteIdentity) {
	t.Helper()
	seedPipelineTestNodeDelivery(t, ctx, db, eventID, node, target)
}

func assertFinalFlowInstanceAuthoringDeliveryStatus(t *testing.T, db *sql.DB, eventID, nodeID, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
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
