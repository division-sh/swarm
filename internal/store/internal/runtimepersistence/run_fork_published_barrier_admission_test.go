package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestIssue642PublishedBarrierFixedCutAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := owner.(storeTestDurableEventBusStore)
			planner := owner.(interface {
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
			})
			at := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
			ctx, fixture, handle := seedDeclaredForkFanOutFixtureWithBarrier(t, backend, authorActivityReceiptFixture{db: db, store: owner.(authorActivityReceiptStore)}, 0, at, true)
			advanceFanOutBarriersForTest(t, ctx, selected, fixture.runID, at.Add(time.Second))
			activationID := mustFanOutBarrierScheduleActivationID(t, ctx, db, fixture)
			activation, found, err := owner.(genericschedule.Store).LoadGenericScheduleActivation(ctx, activationID)
			if err != nil || !found {
				t.Fatalf("read source barrier activation: found=%v err=%v", found, err)
			}
			payload, err := canonicaljson.Encode(activation.Command.Payload)
			if err != nil {
				t.Fatal(err)
			}
			eventID := genericschedule.OccurrenceEventID(activationID, activation.CurrentDueAt)
			occurrence := eventtest.RuntimeControlWithRoutingSource(eventID, events.EventType(activation.Command.EventType), genericschedule.OccurrenceProducerID(), activation.Command.TaskID,
				payload, 0, fixture.runID, "", events.EventEnvelope{EntityID: fixture.runID}, activation.Command.RoutingSource, activation.CurrentDueAt)
			ref, _ := handle.JoinRef()
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()), Target: events.MustExistingEntityTarget(events.RouteIdentity{
				FlowID: ".", FlowInstance: fixture.runID, EntityID: fixture.runID,
			})}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, occurrence, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			markFanOutBarrierScheduleFired(t, ctx, db, activationID, eventID, activation.CurrentDueAt.Add(time.Second))
			cut := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.published-barrier-cut", "operator", "", []byte(`{}`), 0,
				fixture.runID, events.EventEnvelope{}, eventtest.RootRoutingSource(fixture.runID), at.Add(3*time.Second))
			captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, postgres)
			if err := insertCanonicalEventRecordFixture(ctx, owner, cut); err != nil {
				t.Fatal(err)
			}
			captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, postgres)
			request := runfork.RunForkPlanRequest{SourceRunID: fixture.runID, At: cut.ID()}
			before := snapshotForkHistoricalExecutionTables(t, db, postgres)
			plan, err := planner.PlanRunFork(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			assertPublishedBarrierCut(t, plan, fixture, activationID, eventID)
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, postgres)) {
				t.Fatal("published barrier admission changed database state")
			}
			// The cut still owes this exact occurrence after current source work
			// settles. A live source lookup must not erase the historical obligation.
			settleFanOutBarrierRouteSuccess(t, ctx, selected, occurrence, route)
			captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, postgres)
			before = snapshotForkHistoricalExecutionTables(t, db, postgres)
			repeated, err := planner.PlanRunFork(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			assertPublishedBarrierCut(t, repeated, fixture, activationID, eventID)
			if !reflect.DeepEqual(plan.FanOutObligations, repeated.FanOutObligations) || plan.ForkPoint != repeated.ForkPoint {
				t.Fatal("current source settlement changed the retained barrier cut")
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, postgres)) {
				t.Fatal("repeated published barrier admission changed database state")
			}
		})
	}
}

func assertPublishedBarrierCut(t *testing.T, plan runfork.RunForkPlan, fixture fanOutOwnerFixture, activationID, eventID string) {
	t.Helper()
	if len(plan.FanOutObligations) != 1 {
		t.Fatalf("published barrier lost exact intent: %+v", plan.FanOutObligations)
	}
	barrier := plan.FanOutObligations[0].Barrier
	if barrier == nil || barrier.Status != fanoutbarrier.StatusClosedPending || barrier.ScheduleActivationID != activationID ||
		barrier.Registration.IntentKey.RunID != fixture.runID || barrier.Registration.IntentKey.TriggeringDeliveryID != fixture.deliveryID ||
		barrier.Summary == nil || *barrier.Summary != (fanoutbarrier.Summary{Total: 0}) {
		t.Fatalf("published barrier cut changed exact ownership or summary: %+v", barrier)
	}
	found := 0
	ref, _ := barrier.Registration.Handle.JoinRef()
	for _, pending := range plan.PendingWork {
		if pending.EventID == eventID {
			found++
			if pending.Status != "pending" || pending.SubscriberID != ref.Node().Key() {
				t.Fatalf("retained occurrence changed exact pending delivery: %+v", pending)
			}
		}
	}
	if found != 1 {
		t.Fatalf("published barrier retained %d pending deliveries, want one", found)
	}
}
