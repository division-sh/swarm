package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestForkTerminalBarrierHistoryCorruptionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := owner.(storeTestDurableEventBusStore)
			forkOwner := owner.(interface {
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
			})
			for _, variant := range []string{"claim", "handler", "outcome_reason", "missing_dead_letter", "occurrence_payload"} {
				t.Run(variant, func(t *testing.T) {
					at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					ctx, fixture, handle := seedDeclaredForkFanOutFixtureWithBarrier(t, backend, authorActivityReceiptFixture{db: db, store: owner.(authorActivityReceiptStore)}, 0, at, true)
					advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, at.Add(time.Second))
					activationID := mustFanOutBarrierScheduleActivationID(t, ctx, db, fixture)
					activation, found, err := owner.(genericschedule.Store).LoadGenericScheduleActivation(ctx, activationID)
					if err != nil || !found {
						t.Fatalf("load activation: found=%v err=%v", found, err)
					}
					payload, err := canonicaljson.Encode(activation.Command.Payload)
					if err != nil {
						t.Fatal(err)
					}
					occurrenceID := genericschedule.OccurrenceEventID(activationID, activation.CurrentDueAt)
					occurrence := eventtest.RuntimeControlWithRoutingSource(occurrenceID, events.EventType(activation.Command.EventType), genericschedule.OccurrenceProducerID(), activation.Command.TaskID, payload, 0, fixture.runID, "",
						events.EventEnvelope{EntityID: fixture.runID}, activation.Command.RoutingSource, activation.CurrentDueAt)
					ref, _ := handle.JoinRef()
					route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: fixture.runID, EntityID: fixture.runID})}
					if err := commitSemanticEventFixtureWithRoutes(ctx, selected, occurrence, []events.DeliveryRoute{route}); err != nil {
						t.Fatal(err)
					}
					markFanOutBarrierScheduleFired(t, ctx, db, activationID, occurrenceID, activation.CurrentDueAt.Add(time.Second))
					settleFanOutBarrierRouteDeadLetter(t, ctx, selected, occurrence, route)
					// Use the real terminal owner, not a status-only fixture.
					advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, time.Now().UTC())
					query := ""
					switch variant {
					case "claim":
						query = `UPDATE event_deliveries SET claim_version=claim_version+1 WHERE event_id=$1`
					case "handler":
						query = `UPDATE dead_letters SET handler_node='wrong-handler' WHERE original_event_id=$1`
					case "outcome_reason":
						query = `UPDATE event_delivery_outcomes SET reason_code='wrong-reason' WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE event_id=$1)`
					case "missing_dead_letter":
						query = `DELETE FROM dead_letters WHERE original_event_id=$1`
					case "occurrence_payload":
						query = `UPDATE events SET payload_bytes=$2 WHERE event_id=$1`
					}
					args := []any{occurrenceID}
					if variant == "occurrence_payload" {
						args = append(args, []byte(`{}`))
					}
					if _, err := db.ExecContext(ctx, query, args...); err != nil {
						t.Fatalf("install hostile %s: %v", variant, err)
					}
					captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, postgres)
					point := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.terminal.hostile", "operator", "", []byte(`{}`), 0, fixture.runID, events.EventEnvelope{}, eventtest.RootRoutingSource(fixture.runID), time.Now().UTC())
					if err := insertCanonicalEventRecordFixture(ctx, owner, point); err != nil {
						t.Fatal(err)
					}
					captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, postgres)
					before := snapshotForkHistoricalExecutionTables(t, db, postgres)
					got, err := forkOwner.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: point.ID(), OriginalLoopCarriage: originalCarriageForRun(t, owner, fixture.runID)})
					if err == nil || got.ForkRunID != "" || !strings.Contains(err.Error(), "terminal barrier") {
						t.Fatalf("wrong rejection: materialized=%+v err=%v", got, err)
					}
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, postgres)) {
						t.Fatal("hostile terminal history changed database")
					}
				})
			}
		})
	}
}
