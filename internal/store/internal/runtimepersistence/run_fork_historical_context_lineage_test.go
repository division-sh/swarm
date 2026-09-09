package runtimepersistence

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

// H09 is a store materialization/history proof, not live fork activation. Each
// child's selector is a canonical writer checkpoint in its materialized run.
func TestRunForkHistoricalFanOutAncestorLineageBothStores(t *testing.T) {
	historicalFanOutAncestorLineage(t, true)
}

func TestRunForkHistoricalFanOutAncestorLineageWithoutRetryBothStores(t *testing.T) {
	historicalFanOutAncestorLineage(t, false)
}

func historicalFanOutAncestorLineage(t *testing.T, proveRetry bool) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, sourceKind := range []string{"event_payload_field", "entity_field_revision"} {
				t.Run(sourceKind, func(t *testing.T) {
					owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
					store := owner.(runForkSelectedLifecycleStore)
					at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					ctx, source := seedDeclaredForkFanOutFixture(t, backend, authorActivityReceiptFixture{db: db, store: owner.(authorActivityReceiptStore)}, 1, at)
					if sourceKind == "entity_field_revision" {
						// The declared carrier executes at the root. Retain its real root
						// owner rather than substituting the unrelated root/one fixture.
						entityID, mutationID := source.runID, uuid.NewString()
						if _, err := db.ExecContext(ctx, `UPDATE entity_state SET fields=$2 WHERE run_id=$1 AND entity_id=$1`, source.runID, `{"items":["source-item"]}`); err != nil {
							t.Fatal(err)
						}
						if _, err := db.ExecContext(ctx, `INSERT INTO entity_mutations (mutation_id,run_id,entity_id,domain,path,old_value,new_value,writer_type,writer_id,created_at) VALUES ($1,$2,$2,'authored_field','items','null',$3,'platform','fan-out-history',$4)`, mutationID, source.runID, `["source-item"]`, at); err != nil {
							t.Fatal(err)
						}
						bindFanOutEntityRevision(t, ctx, db, postgres, source, source.runID, entityID, mutationID, at)
					}
					captureFanOutBarrierForkRevision(t, ctx, db, source.runID, postgres)
					outcomeEvent := eventtest.ExistingRunRootIngress(uuid.NewString(), "history.fanout_finished", "history-proof", "", []byte(`{}`), 0, source.runID, events.EventEnvelope{}, at.Add(time.Second))
					if err := commitSemanticPipelineProcessedEventFixture(ctx, owner, outcomeEvent); err != nil {
						t.Fatal(err)
					}
					seedFanOutBarrierOutcomes(t, ctx, db, source, []string{outcomeEvent.ID()}, false, at.Add(time.Second))
					captureFanOutBarrierForkRevision(t, ctx, db, source.runID, postgres)
					point := historicalLineageCheckpoint(t, db, source.runID, postgres, at.Add(2*time.Second))
					plan, err := store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: source.runID, At: point})
					if err != nil || len(plan.FanOutObligations) != 1 {
						t.Fatalf("canonical source fan-out plan: %#v err=%v", plan.FanOutObligations, err)
					}
					original := plan.FanOutObligations[0]
					var localOutcome string
					if err := db.QueryRow(`SELECT event_id FROM fan_out_outcomes WHERE run_id=$1 AND ordinal=0`, source.runID).Scan(&localOutcome); err != nil || localOutcome != outcomeEvent.ID() {
						t.Fatalf("live source outcome event=%q err=%v", localOutcome, err)
					}
					// The planner prepares a terminal local outcome for inheritance:
					// SourceEventID is historical provenance, not a child event mint.
					if original.Intent.Source.Kind != fanoutobligation.SourceKind(sourceKind) || original.Intent.Request.Capsule.Lineage.RunID != source.runID || len(original.Outcomes) != 1 || original.Outcomes[0].EventID != "" || original.Outcomes[0].SourceEventID != outcomeEvent.ID() || original.Outcomes[0].InheritedDisposition != "no_route" {
						t.Fatalf("source evidence prerequisite: %#v", original)
					}
					if sourceKind == "entity_field_revision" && original.Intent.Source.RunID != source.runID {
						t.Fatalf("source revision ownership = %#v", original.Intent.Source)
					}
					rootRows := historicalLineageOwnedRows(t, db, postgres, source.runID)
					parentRun := source.runID
					for generation := 1; generation <= 2; generation++ {
						request := runfork.RunForkMaterializeRequest{SourceRunID: parentRun, At: point, OriginalLoopCarriage: originalCarriageForRun(t, owner, parentRun)}
						child, err := store.MaterializeRunFork(ctx, request)
						if err != nil || child.MaterializedFanOutCount != 1 || child.ForkRunID == parentRun || child.ForkRunID == source.runID {
							t.Fatalf("generation %d materialization: %#v err=%v", generation, child, err)
						}
						if proveRetry {
							beforeRetry := historicalContextDatabaseRows(t, db, postgres)
							retry, err := store.MaterializeRunFork(ctx, request)
							historicalContextRequireUnchanged(t, beforeRetry, historicalContextDatabaseRows(t, db, postgres))
							if err != nil || retry.ForkRunID != child.ForkRunID {
								t.Fatalf("generation %d exact materialization replay: %#v err=%v", generation, retry, err)
							}
						}
						point = historicalLineageCheckpoint(t, db, child.ForkRunID, postgres, at.Add(time.Duration(2+generation)*time.Second))
						beforePlan := historicalContextDatabaseRows(t, db, postgres)
						inherited, err := store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: child.ForkRunID, At: point})
						historicalContextRequireUnchanged(t, beforePlan, historicalContextDatabaseRows(t, db, postgres))
						if err != nil || len(inherited.FanOutObligations) != 1 {
							t.Fatalf("generation %d inherited historical plan: %#v err=%v", generation, inherited.FanOutObligations, err)
						}
						got := inherited.FanOutObligations[0]
						if got.Intent.Request.Key.RunID != child.ForkRunID || got.Intent.Request.Key.TriggeringDeliveryID != source.deliveryID || !reflect.DeepEqual(got.Intent.Source, original.Intent.Source) {
							t.Fatalf("generation %d remapped ancestor provenance into owning child: %#v; original=%#v", generation, got.Intent, original.Intent)
						}
						wantCapsule := original.Intent.Request.Capsule
						if reflect.TypeOf(wantCapsule).NumField() != 19 || wantCapsule.DeliveryRoute == nil || !wantCapsule.DeliveryRoute.Target.ExistingEntity() {
							t.Fatal("capsule census or declared root receiver changed; reclassify every field")
						}
						wantCapsule.Route = flowidentity.StoredRoute(".", child.ForkRunID, child.ForkRunID)
						wantCapsule.EntityID = child.ForkRunID
						wantCapsule.ProducerSource = eventtest.RootRoutingSource(child.ForkRunID)
						wantDelivery := *wantCapsule.DeliveryRoute
						wantDelivery.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: child.ForkRunID, EntityID: child.ForkRunID})
						wantCapsule.DeliveryRoute = &wantDelivery
						if !reflect.DeepEqual(got.Intent.Request.Capsule, wantCapsule) {
							t.Fatalf("generation %d changed frozen capsule fields or lost child execution ownership: got=%#v want=%#v", generation, got.Intent.Request.Capsule, wantCapsule)
						}
						if len(got.Outcomes) != 1 || got.Outcomes[0].EventID != "" || got.Outcomes[0].SourceEventID != outcomeEvent.ID() || got.Outcomes[0].InheritedDisposition == "" {
							t.Fatalf("generation %d inherited outcome became a fabricated local event: %#v", generation, got.Outcomes)
						}
						historicalContextRequireZero(t, db, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND delivery_id=$2`, child.ForkRunID, source.deliveryID)
						historicalContextRequireZero(t, db, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, child.ForkRunID, outcomeEvent.ID())
						if sourceKind == "event_payload_field" {
							historicalContextRequireZero(t, db, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, child.ForkRunID, original.Intent.Source.EventID)
						}
						historicalContextRequireUnchanged(t, rootRows, historicalLineageOwnedRows(t, db, postgres, source.runID))
						t.Logf("generation=%d child=%s retains ancestor=%s trigger=%s source_kind=%s; planning writes nothing; exact_retry_checked=%v", generation, child.ForkRunID, source.runID, source.deliveryID, sourceKind, proveRetry)
						parentRun = child.ForkRunID
					}
				})
			}
		})
	}
}

func historicalLineageCheckpoint(t *testing.T, db *sql.DB, runID string, postgres bool, at time.Time) string {
	t.Helper()
	ctx, eventID := testAuthorActivityContext(), uuid.NewString()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	seedRunForkRevisionMatrixEvent(t, ctx, tx, runID, eventID, at, postgres)
	f := historicalContextFixture{postgres: postgres, source: runForkRevisionMatrixFixture{runID: runID}}
	f.finalize(t, tx)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return eventID
}

func historicalLineageOwnedRows(t *testing.T, db *sql.DB, postgres bool, runID string) map[string][]string {
	t.Helper()
	all := historicalContextDatabaseRows(t, db, postgres)
	eventRows, err := db.Query(`SELECT event_id FROM events WHERE run_id=$1`, runID)
	if err != nil {
		t.Fatal(err)
	}
	eventIDs := make(map[string]bool)
	for eventRows.Next() {
		var id string
		if err := eventRows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		eventIDs[id] = true
	}
	if err := eventRows.Err(); err != nil {
		t.Fatal(err)
	}
	eventRows.Close()
	out := make(map[string][]string, len(all))
	for table, rows := range all {
		var columns []string
		if err := json.Unmarshal([]byte(rows[0]), &columns); err != nil {
			t.Fatal(err)
		}
		index := -1
		eventOwned := table == "event_receipts" || table == "dead_letters"
		for i, column := range columns {
			if column == "run_id" || (table == "event_receipts" && column == "event_id") || (table == "dead_letters" && column == "original_event_id") {
				index = i
				break
			}
		}
		if index < 0 {
			continue
		}
		out[table] = []string{rows[0]}
		for _, row := range rows[1:] {
			var fields []any
			if err := json.Unmarshal([]byte(row), &fields); err != nil {
				t.Fatal(err)
			}
			id, _ := fields[index].(string)
			if (!eventOwned && id == runID) || (eventOwned && eventIDs[id]) {
				out[table] = append(out[table], row)
			}
		}
	}
	return out
}
