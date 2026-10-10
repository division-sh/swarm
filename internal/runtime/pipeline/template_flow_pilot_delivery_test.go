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
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
	"github.com/google/uuid"
)

func VerifyNativeTemplateFlowPilotPipelineDispatchUpdatesSelectedTemplateInstanceForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	bundle := templateflowpilot.LoadBundle(t, templateflowpilot.Options{})
	source := semanticview.Wrap(bundle)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			workflowStore := pc.workflowStore
			entityID := uuid.NewString()
			instanceID := "acct-1"
			flowInstance := "account/" + instanceID
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
				InstanceID:      instanceID,
				StorageRef:      flowInstance,
				EntityID:        entityID,
				WorkflowName:    "account",
				WorkflowVersion: bundle.WorkflowVersion(),
				CurrentState:    "pending",
				Fields:          map[string]any{"account_id": "acct-1"},
				EntityType:      "account_state",
			})); err != nil {
				t.Fatalf("seed scoring workflow instance: %v", err)
			}

			target := events.RouteIdentity{FlowID: "account", FlowInstance: flowInstance, EntityID: entityID}
			evt := eventtest.ExistingRunRootIngress(
				uuid.NewString(),
				events.EventType("producer/account.ready"),
				"producer",
				"",
				json.RawMessage(`{"account_id":"acct-1","score":"91","decision":"approved"}`),
				0,
				runtimecorrelation.RunIDFromContext(ctx),
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, target),
				time.Now().UTC(),
			)
			node := pipelineSourceNode(t, source, "account", "account-node")
			route := workflowNodeStampedConnectRoute(t, source, "account", "account.ready", "account-node")
			route.Target = events.MustExistingEntityTarget(target)
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatal(err)
			}

			handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
			if err != nil {
				t.Fatalf("dispatchWorkflowNodeEventResult: %v", err)
			}
			if !handled {
				t.Fatal("dispatchWorkflowNodeEventResult handled = false, want account handler delivery")
			}
			loaded, ok, err := workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, flowInstance))
			if err != nil {
				t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
			}
			if !ok {
				t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
			}
			if loaded.WorkflowName != "account" || loaded.CurrentState != "done" {
				t.Fatalf("loaded account instance = storage:%q workflow:%q state:%q, want account/done", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState)
			}
			if loaded.Fields["account_id"] != "acct-1" || loaded.Fields["score"] != "91" || loaded.Fields["decision"] != "approved" {
				t.Fatalf("loaded account fields = %#v, want account_id/score/decision from routed payload", loaded.Fields)
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
