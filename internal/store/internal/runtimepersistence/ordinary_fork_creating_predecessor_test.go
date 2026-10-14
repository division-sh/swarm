package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/google/uuid"
)

func TestOrdinaryForkCreatingPredecessorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scenario := range []string{"historical", "later_native", "corrupt_header", "missing_cut", "dual_evidence"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				proveOrdinaryForkCreatingPredecessor(t, backend, scenario)
			})
		}
	}
}

type ordinaryForkPredecessorFixture struct {
	receiverConfigActivationFixture
	ctx               context.Context
	owner             flowidentity.RunScopedFlowInstance
	entityID          string
	sourceOwner       flowidentity.RunScopedFlowInstance
	sourceEntity      string
	cutRevision       int64
	earlierRevision   int64
	otherSourceOwner  flowidentity.RunScopedFlowInstance
	otherSourceEntity string
}

func newOrdinaryForkPredecessorFixture(t *testing.T, backend string) ordinaryForkPredecessorFixture {
	t.Helper()
	f := newReceiverConfigActivationFixtureWithAgents(t, backend, false)
	f.constructKeylessRoot(t)
	plan := constructHistoricalSourceFixture(t, f.ctx, f.store, f.request("original", "ti-original", "source"))
	runID := correlation.RunIDFromContext(f.ctx)
	other := constructHistoricalSourceFixture(t, f.ctx, f.store, f.request("other", "ti-other", "unrelated"))
	earlier := ordinaryForkPredecessorCheckpoint(t, f, backend)
	point := eventtest.ExistingRunRootIngress(uuid.NewString(), "task.started", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	if err := insertCanonicalEventRecordFixture(f.ctx, f.store, point); err != nil {
		t.Fatal(err)
	}
	captureFanOutBarrierForkRevision(t, f.ctx, f.db, runID, backend == "postgres")
	child, err := f.store.(runForkSelectedLifecycleStore).MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: point.ID()})
	if err != nil {
		t.Fatal(err)
	}
	activation, err := f.store.(runForkSelectedLifecycleStore).ActivateRunFork(f.ctx, runfork.RunForkActivateRequest{
		ForkRunID: child.ForkRunID, AllowSourceFreeze: true, OriginalLoopCarriage: originalCarriageForRun(t, f.store, runID),
		HistoricalReplayExecutionAdmitter: runforkexecution.HistoricalReplayExecutionAdmitter{},
	})
	if err != nil || !activation.Activated || !activation.SourceFrozen || activation.SourceRunID != runID {
		t.Fatalf("activate ordinary historical receiver: %+v %v", activation, err)
	}
	projected, err := runfork.ProjectEntityOwnership(runID, child.ForkRunID, plan.Identity.EntityID, plan.Identity.InstancePath)
	if err != nil {
		t.Fatal(err)
	}
	owner := flowidentity.RunScopedFlowInstance{RunID: child.ForkRunID, Route: flowidentity.StoredRoute(plan.Identity.TemplateID, plan.Identity.InstanceID, projected.Fork.FlowInstance)}
	ctx := correlation.WithRunID(f.ctx, child.ForkRunID)
	if _, err := f.store.(pipeline.FlowConstructionPublicationReader).LoadFlowConstructionPublication(ctx, owner, projected.Fork.EntityID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("historical receiver unexpectedly has native construction: %v", err)
	}
	return ordinaryForkPredecessorFixture{receiverConfigActivationFixture: f, ctx: ctx, owner: owner, entityID: projected.Fork.EntityID,
		sourceOwner: flowidentity.RunScopedFlowInstance{RunID: runID, Route: plan.Identity.Route()}, sourceEntity: plan.Identity.EntityID, cutRevision: child.ForkPoint.Revision,
		earlierRevision: earlier, otherSourceOwner: flowidentity.RunScopedFlowInstance{RunID: runID, Route: other.Identity.Route()}, otherSourceEntity: other.Identity.EntityID}
}

func ordinaryForkPredecessorCheckpoint(t *testing.T, f receiverConfigActivationFixture, backend string) int64 {
	t.Helper()
	runID := correlation.RunIDFromContext(f.ctx)
	point := eventtest.ExistingRunRootIngress(uuid.NewString(), "task.started", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	if err := insertCanonicalEventRecordFixture(f.ctx, f.store, point); err != nil {
		t.Fatal(err)
	}
	captureFanOutBarrierForkRevision(t, f.ctx, f.db, runID, backend == "postgres")
	plan, err := f.store.(forkWorkflowOwnershipStore).PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: point.ID()})
	if err != nil || plan.ForkPoint.Revision <= 0 {
		t.Fatalf("prior real history cut: %+v %v", plan, err)
	}
	return plan.ForkPoint.Revision
}

func proveOrdinaryForkCreatingPredecessor(t *testing.T, backend, scenario string) {
	t.Helper()
	f := newOrdinaryForkPredecessorFixture(t, backend)
	if scenario == "later_native" {
		f.owner.Route = flowidentity.StoredRoute("review", "ti-later", "review/ti-later")
		f.entityID = uuid.NewString()
		commitPreparedWorkflowAggregateFixture(t, f.ctx, f.store, f.owner.RunID, pipeline.WorkflowInstance{
			InstanceID: "ti-later", StorageRef: f.owner.Route.InstancePath, EntityID: f.entityID,
			EntityType: "review_item", WorkflowName: "review", WorkflowVersion: "1", Mode: "template",
			InstanceKey: "later", ParentFlowID: ".", ParentFlowInstance: f.owner.RunID, ParentEntityID: f.owner.RunID,
			StageDefined: true, CurrentState: "pending", Fields: map[string]any{"request_id": "later", "label": "native"},
			CreatedAt: time.Now().UTC(), EnteredStageAt: time.Now().UTC(),
		}, time.Now().UTC())
		evidence, err := f.store.(pipeline.FlowConstructionPublicationReader).LoadFlowConstructionPublication(f.ctx, f.owner, f.entityID)
		if err != nil || evidence.Identity.InstancePath != f.owner.Route.InstancePath || evidence.CreatingInput.EventID != "" {
			t.Fatalf("post-cut construction lacks exact native evidence: %+v %v", evidence, err)
		}
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceNode("review", "ordered-node")), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: f.owner.Route.ScopeKey, FlowInstance: f.owner.Route.InstancePath, EntityID: f.entityID,
	})}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "task.started", "fixture", "", []byte(`{}`), 0, f.owner.RunID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	if err := acknowledgePipelineEventFixture(f.ctx, f.store, event.ID()); err != nil {
		t.Fatal(err)
	}
	applyOrdinaryForkPredecessorFault(t, f, scenario)
	want := deliverylifecycle.ClaimAcquired
	if scenario != "historical" && scenario != "later_native" {
		want = deliverylifecycle.ClaimInvariantInvalid
	}
	proveOrdinaryForkClaimEntrances(t, f, event, route, want, scenario)
}

func applyOrdinaryForkPredecessorFault(t *testing.T, f ordinaryForkPredecessorFixture, scenario string) {
	t.Helper()
	switch scenario {
	case "corrupt_header":
		changed, err := SetWorkflowProjectionConflictingInstanceIDForTest(f.ctx, f.store, f.owner.RunID, f.owner.Route.InstancePath)
		if err != nil || changed != 1 {
			t.Fatalf("corrupt exact historical receiver: changed=%d err=%v", changed, err)
		}
	case "missing_cut":
		before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
		if err := CorruptReceiverConstructionHistoryCutForTest(f.ctx, f.store, f.owner, f.entityID, 1<<62); err != nil {
			t.Fatal(err)
		}
		proveOrdinaryForkOriginRevisionDelta(t, before, forkReceiverFaultSnapshot(t, f.ctx, f.store), f.owner.RunID)
	case "dual_evidence":
		if err := CopyReceiverConstructionReceiptForTest(f.ctx, f.store, f.sourceOwner, f.owner, f.sourceEntity, f.entityID); err != nil {
			t.Fatal(err)
		}
	}
}

func proveOrdinaryForkClaimEntrances(t *testing.T, f ordinaryForkPredecessorFixture, event events.Event, route events.DeliveryRoute, want deliverylifecycle.ClaimDisposition, scenario string) {
	t.Helper()
	store := f.store.(deliverylifecycle.Store)
	pending := loadDeliverySnapshotFixture(t, f.ctx, store, event.ID(), route)
	before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
	single, err := store.ObserveDeliveryContinuation(f.ctx, pending.Authority, pending.DeliveryID)
	if err != nil || single.Disposition != want || single.Wake != (deliverylifecycle.ContinuationWake{}) {
		t.Fatalf("ordinary-fork single observation: %+v %v", single, err)
	}
	batch, err := store.ObserveDeliveryContinuations(f.ctx, pending.Authority, []string{pending.DeliveryID})
	if err != nil || len(batch) != 1 || batch[0].Disposition != want || batch[0].Wake != (deliverylifecycle.ContinuationWake{}) {
		t.Fatalf("ordinary-fork batch observation: %+v %v", batch, err)
	}
	page, err := store.ScanDeliveryContinuations(f.ctx, pending.Authority, deliverylifecycle.ContinuationCursor{}, 64)
	if err != nil || len(page.Items) != 1 || page.Items[0].DeliveryID != pending.DeliveryID || page.Items[0].Disposition != want {
		t.Fatalf("ordinary-fork scan: %+v %v", page, err)
	}
	after, err := store.Snapshot(f.ctx, pending.DeliveryID)
	if err != nil || !reflect.DeepEqual(pending, after) || after.ClaimVersion != 0 || !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
		t.Fatalf("ordinary-fork observations changed pending/source evidence: %+v %v", after, err)
	}
	claim, err := store.ClaimDelivery(f.ctx, pending.Authority, event, route)
	if err != nil || claim.Disposition != want {
		t.Fatalf("ordinary-fork direct claim: %+v %v", claim, err)
	}
	if scenario == "dual_evidence" && (claim.Invariant == nil || !strings.Contains(claim.Invariant.Error(), "contradictory historical and native construction evidence")) {
		t.Fatalf("dual evidence did not reach the specific history/native guard: %+v", claim)
	}
	if scenario == "missing_cut" && (!errors.Is(claim.Invariant, sql.ErrNoRows) || !strings.Contains(claim.Invariant.Error(), "read admitted historical instance cut")) {
		t.Fatalf("missing cut did not reach fixed-snapshot refusal: %+v", claim)
	}
	if want == deliverylifecycle.ClaimInvariantInvalid {
		after, err = store.Snapshot(f.ctx, pending.DeliveryID)
		if claim.Invariant == nil || err != nil || !reflect.DeepEqual(pending, after) || !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
			t.Fatalf("ordinary-fork corrupt evidence changed receiver: %+v %v", claim, err)
		}
	}
}
