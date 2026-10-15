package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestA2JoinHeaderOnlyReceiverRefusesClaimBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveA2JoinHeaderOnlyReceiverRefusal(t, backend)
		})
	}
}

func proveA2JoinHeaderOnlyReceiverRefusal(t *testing.T, backend string) {
	t.Helper()
	selected, db, _ := newStopCommitStore(t, backend)
	runID, entityID := uuid.NewString(), uuid.NewString()
	flowID, path := "native-join", "native-join/one"
	ctx, cancel := context.WithTimeout(correlation.WithRunID(storeTestWorkContext(t, testAuthorActivityContext()), runID), time.Minute)
	defer cancel()
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	seedWorkflowTargetStateForTransition(t, backend, db, runID, entityID, path, at)
	seedWorkflowHeaderProjectionFixture(t, ctx, db, runID, entityID, path, flowID, "review_item", "active", "{}", at)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceNode(flowID, "join-node")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: entityID})}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "item.completed", "fixture", "", []byte(`{"member":"two","value":2}`), 0, runID,
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), path), at)
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	if err := acknowledgePipelineEventFixture(ctx, selected, event.ID()); err != nil {
		t.Fatal(err)
	}
	before := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
	for table, want := range map[string]int{"flow_instances": 1, "entity_state": 1, "workflow_instance_initial_materializations": 0, "flow_instance_runtime_readiness": 0} {
		if got := len(before[table]); got != want {
			t.Fatalf("header-only inventory %s=%d, want %d", table, got, want)
		}
	}
	store := selected.(deliverylifecycle.Store)
	pending := loadDeliverySnapshotFixture(t, ctx, store, event.ID(), route)
	claim, err := store.ClaimDelivery(ctx, pending.Authority, event, route)
	if err != nil || claim.Disposition != deliverylifecycle.ClaimInvariantInvalid || !errors.Is(claim.Invariant, sql.ErrNoRows) {
		t.Fatalf("header-only target acquired construction: result=%+v err=%v", claim, err)
	}
	t.Logf("header=1 fields=1 construction=0 readiness=0: ClaimResult.Invariant=%v", claim.Invariant)
	single, err := store.ObserveDeliveryContinuation(ctx, pending.Authority, pending.DeliveryID)
	if err != nil || single.Disposition != deliverylifecycle.ClaimInvariantInvalid || !errors.Is(single.Invariant, sql.ErrNoRows) {
		t.Fatalf("header-only single observation: result=%+v err=%v", single, err)
	}
	batch, err := store.ObserveDeliveryContinuations(ctx, pending.Authority, []string{pending.DeliveryID})
	if err != nil || len(batch) != 1 || batch[0].Disposition != deliverylifecycle.ClaimInvariantInvalid || !errors.Is(batch[0].Invariant, sql.ErrNoRows) {
		t.Fatalf("header-only batch observation: result=%+v err=%v", batch, err)
	}
	page, err := store.ScanDeliveryContinuations(ctx, pending.Authority, deliverylifecycle.ContinuationCursor{}, 64)
	if err != nil || len(page.Items) != 1 || page.Items[0].DeliveryID != pending.DeliveryID || page.Items[0].Disposition != deliverylifecycle.ClaimInvariantInvalid || !errors.Is(page.Items[0].Invariant, sql.ErrNoRows) {
		t.Fatalf("header-only scan: result=%+v err=%v", page, err)
	}
	after, err := store.Snapshot(ctx, pending.DeliveryID)
	if err != nil || !reflect.DeepEqual(pending, after) || after.Status != deliverylifecycle.StatusPending || after.ClaimVersion != 0 || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
		t.Fatalf("header-only refusal mutated evidence: before=%+v after=%+v err=%v", pending, after, err)
	}
}
