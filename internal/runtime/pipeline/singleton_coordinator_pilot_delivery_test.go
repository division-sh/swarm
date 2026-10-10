package pipeline

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/singletoncoordinatorpilot"
	"github.com/google/uuid"
)

func VerifyNativeSingletonCoordinatorPilotPipelineDispatchPersistsContainedStateReadbackForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	bundle := singletoncoordinatorpilot.LoadBundle(t, singletoncoordinatorpilot.Options{})
	source := semanticview.Wrap(bundle)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			workflowStore := pc.workflowStore
			entityID := FlowInstanceEntityID(singletoncoordinatorpilot.FlowInstance)
			constructNativeSingletonCoordinatorPilotForTest(t, fixture, ctx, bundle, entityID)

			target := events.RouteIdentity{
				FlowID:       singletoncoordinatorpilot.FlowID,
				FlowInstance: singletoncoordinatorpilot.FlowInstance,
				EntityID:     entityID,
			}
			evt := eventtest.ExistingRunRootIngress(
				uuid.NewString(),
				events.EventType(singletoncoordinatorpilot.InputEvent),
				singletoncoordinatorpilot.FlowID,
				"",
				json.RawMessage(`{"coordinator_id":"global","lead_id":"lead-42","observation":{"source":"feed","note":"first seen"},"audit":{"ref":"lead-42","action":"observed"},"followup_audit":{"ref":"lead-42","action":"queued"},"corrected_audit":{"ref":"bootstrap","action":"corrected"}}`),
				0,
				runtimecorrelation.RunIDFromContext(ctx),
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, target),
				time.Now().UTC(),
			)
			node := pipelineSourceNode(t, source, singletoncoordinatorpilot.FlowID, singletoncoordinatorpilot.NodeID)
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)}
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatal(err)
			}

			handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
			if err != nil {
				t.Fatalf("dispatchWorkflowNodeEventResult: %v", err)
			}
			if !handled {
				t.Fatal("dispatchWorkflowNodeEventResult handled = false, want coordinator handler delivery")
			}
			loaded, ok, err := workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, singletoncoordinatorpilot.FlowInstance))
			if err != nil {
				t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
			}
			if !ok {
				t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
			}
			if loaded.WorkflowName != singletoncoordinatorpilot.FlowID || loaded.CurrentState != "pending" {
				t.Fatalf("loaded singleton coordinator = storage:%q workflow:%q state:%q, want coordinator/pending", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState)
			}
			leadIndex, ok := loaded.Fields["lead_index"].(map[string]any)
			if !ok {
				t.Fatalf("lead_index = %#v, want map", loaded.Fields["lead_index"])
			}
			lead, ok := leadIndex["lead-42"].(map[string]any)
			if !ok {
				t.Fatalf("lead_index[lead-42] = %#v, want map", leadIndex["lead-42"])
			}
			if lead["status"] != "active" || lead["score"] != int64(1) {
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
			auditLog, ok := loaded.Fields["audit_log"].([]any)
			if !ok || len(auditLog) != 3 {
				t.Fatalf("audit_log = %#v, want three entries", loaded.Fields["audit_log"])
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
			id, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := fixture.Store.Snapshot(ctx, id)
			if err != nil || snapshot.Status != runtimedelivery.StatusDelivered {
				t.Fatalf("exact singleton delivery did not settle: %+v/%v", snapshot, err)
			}
			contained := "coordinator/lead-42"
			if _, found, err := workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, contained)); err != nil || found {
				t.Fatalf("contained item became a workflow projection: %t/%v", found, err)
			}
			footprint, err := fixture.ContainedFootprint(ctx, runtimecorrelation.RunIDFromContext(ctx), contained, FlowInstanceEntityID(contained))
			if err != nil || footprint != (PipelineContainedFootprintForTest{}) {
				t.Fatalf("contained item gained physical delivery/header/entity rows: %+v/%v", footprint, err)
			}
		})
	}
}

func VerifyNativeSingletonCoordinatorPilotPipelineRejectsContainedItemDeliveryTargetForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	bundle := singletoncoordinatorpilot.LoadBundle(t, singletoncoordinatorpilot.Options{})
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			_, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)

			containedTarget := events.RouteIdentity{
				FlowID:       singletoncoordinatorpilot.FlowID,
				FlowInstance: singletoncoordinatorpilot.FlowInstance + "/lead-42",
				EntityID:     uuid.NewString(),
			}
			if pc.workflowNodeMatchesDeliveryTarget(pipelineNode(t, singletoncoordinatorpilot.FlowID, singletoncoordinatorpilot.NodeID), runtimecorrelation.RunIDFromContext(ctx), containedTarget) {
				t.Fatalf("contained item target %#v matched singleton coordinator node; contained map entries must not be route recipients", containedTarget)
			}
		})
	}
}

func constructNativeSingletonCoordinatorPilotForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context, bundle *runtimecontracts.WorkflowContractBundle, entityID string) {
	t.Helper()
	graph, ok := bundle.WorkflowStageTopology(singletoncoordinatorpilot.FlowID)
	if !ok {
		t.Fatal("singleton coordinator fixture has no compiled stage catalog")
	}
	initial, err := graph.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	constructed := constructorUnitIdentity(t, semanticview.Wrap(bundle), runtimecorrelation.RunIDFromContext(ctx), singletoncoordinatorpilot.FlowID)
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:   singletoncoordinatorpilot.FlowInstance,
		StorageRef:   singletoncoordinatorpilot.FlowInstance,
		EntityID:     entityID,
		ParentFlowID: constructed.ParentRoute.FlowID, ParentFlowInstance: constructed.ParentRoute.FlowInstance, ParentEntityID: constructed.ParentEntityID,
		WorkflowName:    singletoncoordinatorpilot.FlowID,
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    initial.ID(),
		Fields: map[string]any{
			"coordinator_id": "global",
			"lead_index":     map[string]any{},
			"audit_log": []any{
				map[string]any{"ref": "seed", "action": "seed"},
			},
		},
		EntityType: "coordinator_state",
	})); err != nil {
		t.Fatalf("seed singleton coordinator workflow instance: %v", err)
	}
}
