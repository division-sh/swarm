package pipeline

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
	"github.com/google/uuid"
)

func VerifyNativeFinalFlowInstanceAuthoringFixturePipelineDispatchLocalizesTemplateInputConnectEventForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	bundle := finalflowinstanceauthoring.LoadBundle(t, finalflowinstanceauthoring.Options{})
	source := semanticview.Wrap(bundle)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			workflowStore := pc.workflowStore
			instanceID := "acct-42"
			flowInstance := finalflowinstanceauthoring.TemplateFlowID + "/" + instanceID
			entityID := FlowInstanceEntityID(flowInstance)
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
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
			evt := eventtest.ExistingRunRootIngress(
				uuid.NewString(),
				events.EventType(finalflowinstanceauthoring.ProducerFlowID+"/"+finalflowinstanceauthoring.ProducerOutput),
				finalflowinstanceauthoring.ProducerFlowID,
				"",
				json.RawMessage(`{"account_id":"acct-42","score":"91","decision":"approved"}`),
				0,
				runtimecorrelation.RunIDFromContext(ctx),
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, target),
				time.Now().UTC(),
			)
			node := pipelineSourceNode(t, source, finalflowinstanceauthoring.TemplateFlowID, finalflowinstanceauthoring.TemplateNodeID)
			route := workflowNodeStampedConnectRoute(t, source, finalflowinstanceauthoring.TemplateFlowID, finalflowinstanceauthoring.TemplateInputPin, finalflowinstanceauthoring.TemplateNodeID)
			route.Target = events.MustExistingEntityTarget(target)
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatal(err)
			}

			handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
			if err != nil {
				t.Fatalf("dispatchWorkflowNodeEventResult: %v", err)
			}
			if !handled {
				t.Fatal("dispatchWorkflowNodeEventResult handled = false, want account_case handler delivery")
			}
			loaded, ok, err := workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, flowInstance))
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
			id, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := fixture.Store.Snapshot(ctx, id)
			if err != nil || snapshot.Status != runtimedelivery.StatusDelivered {
				t.Fatalf("exact pilot delivery did not settle: %+v/%v", snapshot, err)
			}
			if count, err := fixture.DeliveryCount(ctx, evt.ID(), node.Key()); err != nil || count != 1 {
				t.Fatalf("exact pilot obligations=%d/%v, want one", count, err)
			}
		})
	}
}
