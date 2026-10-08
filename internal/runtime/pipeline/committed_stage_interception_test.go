package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestPipelineInterceptionRetainsCommittedStageBothStores(t *testing.T) {
	bundle := compiledAdapterSource(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCompiledAdapterFixture(t, backend, bundle, ".", "ready", true)
			event := f.event("direct")
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(f.node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: f.flow, FlowInstance: f.path, EntityID: f.entityID})}
			ctx, err := persistWorkflowJoinPublicationForTest(t, f.pc, f.ctx, event, route, true)
			if err != nil {
				t.Fatal(err)
			}
			owner := installedWorkflowJoinDeliveryOwnerForTest(t, f.pc)
			id, err := runtimedelivery.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := owner.Snapshot(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			acquisition, err := owner.ClaimDelivery(ctx, snapshot.Authority, event, snapshot.Route)
			claimed, acquired := acquisition.Acquired()
			if err != nil || !acquired {
				t.Fatalf("claim handler occurrence: disposition=%s err=%v", acquisition.Disposition, err)
			}
			ctx = runtimedelivery.WithClaim(ctx, claimed.Claim)
			handled, outcome, err := f.pc.executeNodeHandlerPlanResultWithEmissionPlan(ctx, f.node, event, nil)
			if err != nil || !handled || !outcome.Committed {
				t.Fatalf("handler interception: handled=%t result=%+v err=%v", handled, outcome, err)
			}
			receipts := outcome.StageReceipts()
			after, found := f.load()
			if !found || len(receipts) != 1 || receipts[0].EventID() != event.ID() || receipts[0].Stage().EntityID != f.entityID || receipts[0].Stage().Instance.RunID != event.RunID() || receipts[0].Stage().Stage != "done" || receipts[0].Stage().Revision != after.Revision {
				t.Fatalf("exact acknowledged stage was dropped: receipts=%+v state=%+v", receipts, after)
			}
		})
	}
}
