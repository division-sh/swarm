package runtimepersistence

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// Fork consumers need a real original declaration, unlike isolated SQL-owner
// fixtures. The capsule is explicit; claim, intent and settlement use real owners.
func seedDeclaredForkFanOutFixture(t *testing.T, backend string, fixture authorActivityReceiptFixture, cardinality int, at time.Time) (context.Context, fanOutOwnerFixture) {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, canonicalrouting.CopyForkFanOutCarrier(t, false, false), contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	runID := uuid.NewString()
	ctx := correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, runID, source), runID)
	node := mustPersistenceRootNode("fan-out-source")
	plans := source.FanOutPlansForHandler(node, "items.ready")
	if len(plans) != 1 {
		t.Fatal("fixture requires exactly one compiled fan-out")
	}
	items := make([]string, cardinality)
	for i := range items {
		items[i] = fmt.Sprintf("item-%03d", i)
	}
	trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "items.ready", "fan-out-test", "", []byte(forkTestJSON(t, map[string]any{"items": items})), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})}
	selected := fixture.store.(storeTestDurableEventBusStore)
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, trigger, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claim, err := claimDeliveryFixture(ctx, selected, trigger, route)
	if err != nil {
		t.Fatal(err)
	}
	seedWorkflowTargetStateForTransition(t, backend, fixture.db, runID, runID, runID, "pending", 1, at)
	if _, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET entity_type='root' WHERE run_id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	request := fanoutobligation.IntentRequest{
		Key: fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: claim.Claim.DeliveryID(), ElementRef: plans[0].Ref.ElementRef}, PlanRef: plans[0].Ref,
		Source: fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: trigger.ID(), Field: "items"}, Cardinality: cardinality,
		Capsule: fanoutobligation.Capsule{NodeKey: node.Key(), ExecutionFlowID: ".", Route: flowidentity.StoredRoute(".", runID, runID), EntityID: runID,
			HandlerEventKey: "items.ready", CurrentState: "review", ProducerSource: trigger.RoutingSource(), DeliveryRoute: &route, Lineage: events.LineageFromEvent(trigger)},
	}
	record := stateOnlyWorkflowEngineMutationRecord(t, runID, ".", runID, runID, "pending", 1, at)
	record.CurrentState, record.EntityType, record.Mode = "review", "root", "static"
	if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{
		State: record, FanOutIntent: &request,
		DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claim.Claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
	}); err != nil {
		t.Fatal(err)
	}
	return ctx, fanOutOwnerFixture{runID: runID, eventID: trigger.ID(), deliveryID: claim.Claim.DeliveryID(), flowPath: plans[0].Ref.ElementRef.FlowPath,
		semanticPath: plans[0].Ref.ElementRef.SemanticPath, createdAt: at, bundleHash: bundle.SourceArtifact.BundleHash(), artifact: bundle.SourceArtifact}
}
