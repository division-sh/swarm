package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// Fork consumers need a real original declaration, unlike isolated SQL-owner
// fixtures. The capsule is explicit; claim, intent and settlement use real owners.
func seedDeclaredForkFanOutFixture(t *testing.T, backend string, fixture authorActivityReceiptFixture, cardinality int, at time.Time) (context.Context, fanOutOwnerFixture) {
	t.Helper()
	ctx, source, _ := seedDeclaredForkFanOutFixtureWithBarrier(t, backend, fixture, cardinality, at, false)
	return ctx, source
}

func seedDeclaredForkFanOutFixtureWithBarrier(t *testing.T, backend string, fixture authorActivityReceiptFixture, cardinality int, at time.Time, withBarrier bool) (context.Context, fanOutOwnerFixture, timeridentity.TimerHandle) {
	ctx, source, handle, _ := seedDeclaredForkFanOutGenerationFixture(t, backend, fixture, cardinality, at, withBarrier, false)
	return ctx, source, handle
}

func seedDeclaredForkFanOutGenerationFixture(t *testing.T, backend string, fixture authorActivityReceiptFixture, cardinality int, at time.Time, withBarrier, withGeneration bool) (context.Context, fanOutOwnerFixture, timeridentity.TimerHandle, func(bool)) {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, canonicalrouting.CopyForkFanOutCarrier(t, withGeneration, withBarrier), contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
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
	generation := attemptgeneration.Generation{}
	var activation loopruntime.Activation
	payload := map[string]any{"items": items}
	if withGeneration {
		activation, err = loopruntime.New(runID, runID, ".", "revision", "revision_id", uuid.NewString(), "review", 3, at)
		if err != nil {
			t.Fatal(err)
		}
		generation = activation.Generation()
		payload["revision_id"] = generation.RevisionID
	}
	trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "items.ready", "fan-out-test", "", []byte(forkTestJSON(t, payload)), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at)
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
			HandlerEventKey: "items.ready", CurrentState: "review", ProducerSource: trigger.RoutingSource(), Receiver: &fanoutobligation.ExecutionReceiver{Node: node, Target: route.Target}, Lineage: events.LineageFromEvent(trigger)},
	}
	record := stateOnlyWorkflowEngineMutationRecord(t, runID, ".", runID, runID, "pending", 1, at)
	record.CurrentState, record.EntityType, record.Mode = "review", "root", "static"
	record.EnteredStageAt, record.UpdatedAt = at, at
	if withGeneration {
		request.Capsule.Loop = activation.Context()
		buckets := map[string]map[string]any{}
		if err := loopruntime.Store(buckets, activation); err != nil {
			t.Fatal(err)
		}
		record.Accumulator = json.RawMessage(forkTestJSON(t, buckets))
	}
	var barrier *fanoutbarrier.Registration
	var handle timeridentity.TimerHandle
	if withBarrier {
		joinPlan, ok := semanticview.WorkflowJoinPlanForHandler(source, node, "items.ready")
		if !ok {
			t.Fatal("fixture requires the compiled fan-out join")
		}
		declaration, err := request.PlanRef.ElementRef.DeclarationIdentity()
		if err != nil {
			t.Fatal(err)
		}
		join, err := timeridentity.NewFanOutDeliveryJoinRef(node, "items.ready", joinPlan.Spec.ID, declaration, request.PlanRef.BundleHash, request.PlanRef.SemanticDigest)
		if err != nil {
			t.Fatal(err)
		}
		join, err = join.BindFanOutIntent(claim.Claim.DeliveryID(), generation)
		if err != nil {
			t.Fatal(err)
		}
		handle, err = timeridentity.JoinCompleteHandle(join)
		if err != nil {
			t.Fatal(err)
		}
		barrier = &fanoutbarrier.Registration{IntentKey: request.Key, PlanRef: request.PlanRef, Handle: handle,
			Route: request.Capsule.Route, EntityID: runID, RoutingSource: trigger.RoutingSource(), ExecutionMode: trigger.ExecutionMode(), CreatedAt: at}
	}
	if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{
		State: record, FanOutIntent: &request, FanOutBarrier: barrier,
		DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claim.Claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
	}); err != nil {
		t.Fatal(err)
	}
	// Advance the persisted owner only after barrier closure/terminal evidence has
	// been written, leaving the registration's old attempt intact at revision R.
	advance := func(closeLoop bool) {
		var revision int64
		if err := fixture.db.QueryRowContext(ctx, `SELECT revision FROM entity_state WHERE run_id=$1 AND entity_id=$1`, runID).Scan(&revision); err != nil {
			t.Fatal(err)
		}
		state := "review"
		if closeLoop {
			state = "done"
			err = activation.Close(state, uuid.NewString(), at.Add(2*time.Second))
		} else {
			_, err = activation.Repeat(state, uuid.NewString(), at.Add(2*time.Second))
		}
		if err != nil {
			t.Fatal(err)
		}
		buckets := map[string]map[string]any{}
		if err := loopruntime.Store(buckets, activation); err != nil {
			t.Fatal(err)
		}
		next := stateOnlyWorkflowEngineMutationRecord(t, runID, ".", runID, runID, "review", revision, at.Add(2*time.Second))
		next.CurrentState, next.EntityType, next.Mode = state, "root", "static"
		next.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
		next.EnteredStageAt, next.UpdatedAt = at.Add(2*time.Second), at.Add(2*time.Second)
		next.Accumulator = json.RawMessage(forkTestJSON(t, buckets))
		if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: next}); err != nil {
			t.Fatal(err)
		}
	}
	return ctx, fanOutOwnerFixture{runID: runID, eventID: trigger.ID(), deliveryID: claim.Claim.DeliveryID(), flowPath: plans[0].Ref.ElementRef.FlowPath,
		semanticPath: plans[0].Ref.ElementRef.SemanticPath, createdAt: at, bundleHash: bundle.SourceArtifact.BundleHash(), artifact: bundle.SourceArtifact}, handle, advance
}
