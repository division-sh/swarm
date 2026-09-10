package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestForkBarrierScheduleHostileAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := owner.(storeTestDurableEventBusStore)
			forkOwner := owner.(interface {
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
			})
			for _, variant := range []string{"missing_activation", "wrong_entity", "wrong_owner", "wrong_payload", "terminal_active", "extra_schedule"} {
				t.Run(variant, func(t *testing.T) {
					at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					ctx, fixture, _ := seedDeclaredForkFanOutFixtureWithBarrier(t, backend, authorActivityReceiptFixture{db: db, store: owner.(authorActivityReceiptStore)}, 0, at, true)
					advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, at.Add(time.Second))
					id := mustFanOutBarrierScheduleActivationID(t, ctx, db, fixture)
					var query string
					switch variant {
					case "missing_activation":
						query = `DELETE FROM timers WHERE timer_id=$1`
					case "wrong_entity":
						query = `UPDATE timers SET entity_id='00000000-0000-4000-8000-000000000099' WHERE timer_id=$1`
					case "wrong_owner":
						query = `UPDATE timers SET owner_agent='wrong-owner' WHERE timer_id=$1`
					case "wrong_payload":
						query = `UPDATE timers SET fire_payload='{}' WHERE timer_id=$1`
					case "terminal_active":
						query = `UPDATE fan_out_obligation_barriers SET status='fired' WHERE schedule_activation_id=$1`
					case "extra_schedule":
						command := testRootGenericScheduleCommand(t, fixture.runID, fixture.runID, "unrelated-timer", genericschedule.AbsoluteDue(at))
						if _, err := owner.(genericschedule.Store).AdmitGenericSchedule(ctx, command); err != nil {
							t.Fatal(err)
						}
					}
					if query != "" {
						if _, err := db.ExecContext(ctx, query, id); err != nil {
							t.Fatalf("install explicit historical contradiction: %v", err)
						}
					}
					point := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType("fork.barrier.hostile"), "operator", "", []byte(`{}`), 0, fixture.runID, events.EventEnvelope{}, eventtest.RootRoutingSource(fixture.runID), at.Add(2*time.Second))
					captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, postgres)
					if err := insertCanonicalEventRecordFixture(ctx, owner, point); err != nil {
						t.Fatal(err)
					}
					captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, postgres)
					before := snapshotForkHistoricalExecutionTables(t, db, postgres)
					got, err := forkOwner.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: point.ID(), OriginalLoopCarriage: originalCarriageForRun(t, owner, fixture.runID)})
					if err == nil || got.ForkRunID != "" {
						t.Fatalf("hostile barrier materialized: %+v err=%v", got, err)
					}
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, postgres)) {
						t.Fatal("rejected historical barrier changed database")
					}
				})
			}
		})
	}
}
