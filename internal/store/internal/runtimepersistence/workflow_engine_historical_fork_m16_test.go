package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestWorkflowEngineHistoricalForkM16BothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, preRequest := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
			owner := fixture.store.(forkWorkflowOwnershipStore)
			preFork, err := owner.MaterializeRunForkForSelectedContractExecution(ctx, preRequest)
			if err != nil {
				t.Fatal(err)
			}
			runID, eventID := preRequest.SourceRunID, preRequest.At
			ctx = correlation.WithRunID(ctx, runID)
			prePlan, err := owner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID})
			if err != nil {
				t.Fatal(err)
			}
			if preFork.ForkPoint != prePlan.ForkPoint || prePlan.EventCountAtFork != 2 || len(prePlan.Entities) != 1 || prePlan.Entities[0].CurrentState != "waiting" || prePlan.Entities[0].Fields["marker"] != "source-owned" {
				t.Fatalf("pre-handler cut lost the source state/publication prefix: fork=%+v plan=%+v", preFork.ForkPoint, prePlan)
			}
			m16RequireHistoricalEventSet(t, prePlan, eventID, false, "")
			m16RequireWork(t, prePlan, eventID, runfork.RunForkPendingClassificationPending)
			m16RequireEntity(t, fixture.db, preFork.ForkRunID, preFork.ForkRunID, "waiting", 1, "marker", "source-owned")
			m16RequireCompanion(t, fixture.db, preFork.ForkRunID, 1)
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("controller")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})}
			input := eventtest.ExistingRunRootIngress(eventID, "start.requested", "test", "", []byte(`{"token":"ownership"}`), 0, runID, events.EventEnvelope{}, time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC))
			claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), input, route)
			if err != nil {
				t.Fatal(err)
			}
			state := stateOnlyWorkflowEngineMutationRecord(t, runID, ".", runID, runID, "waiting", 7, time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC))
			state.EntityType, state.Mode = "root", "static"
			if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: state, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claimed.Claim, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection(), SideEffects: []string{"handler_completed"}}}); err != nil {
				t.Fatal(err)
			}
			// This processed event pins the post-handler cut. Composed handler
			// publication atomicity is proved by TestReceiverConfigEngineAtomicFaultMatrixBothStores.
			postEvent := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.requested", "test", "", []byte(`{"token":"after"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, postEvent); err != nil {
				t.Fatal(err)
			}
			postRequest := prepareSelectedStoreMaterializationForTest(t, ctx, fixture.store, runID, postEvent.ID(), runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts})
			postPlan, err := owner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: postEvent.ID()})
			if err != nil {
				t.Fatal(err)
			}
			postFork, err := owner.MaterializeRunForkForSelectedContractExecution(ctx, postRequest)
			if err != nil {
				t.Fatal(err)
			}
			if preFork.ForkRunID == postFork.ForkRunID || preFork.ForkPoint.Revision >= postFork.ForkPoint.Revision || postFork.ForkPoint != postPlan.ForkPoint || postPlan.EventCountAtFork != 3 || len(postPlan.Entities) != 1 || postPlan.Entities[0].CurrentState != "done" || postPlan.Entities[0].Fields["handled"] != true {
				t.Fatalf("distinct ordered historical cuts: pre=%+v post=%+v", preFork, postFork)
			}
			m16RequireHistoricalEventSet(t, postPlan, eventID, true, postEvent.ID())
			m16RequireHistoricalEventSet(t, prePlan, eventID, false, postEvent.ID())
			m16RequireWork(t, postPlan, eventID, runfork.RunForkPendingClassificationDeliveredCompleted)
			m16RequireWork(t, postPlan, postEvent.ID(), runfork.RunForkPendingClassificationDeliveredCompleted)
			m16RequireEntity(t, fixture.db, postFork.ForkRunID, postFork.ForkRunID, "done", 1, "handled", true)
			// The completed frontier has no receiver to initialize; unlike the
			// pending pre-handler cut it must not invent a live companion.
			m16RequireCompanion(t, fixture.db, postFork.ForkRunID, 0)
			m16RequireCompanion(t, fixture.db, runID, 1)
			m16RequireEntity(t, fixture.db, preFork.ForkRunID, preFork.ForkRunID, "waiting", 1, "marker", "source-owned")
			m16RequireEntity(t, fixture.db, runID, runID, "done", 8, "handled", true)
			var sourceDeliveries, sourcePublication, childDeliveries, childPublications int
			m16QueryCount(t, fixture.db, &sourceDeliveries, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2 AND status='delivered'`, runID, eventID)
			m16QueryCount(t, fixture.db, &sourcePublication, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline' AND outcome='success'`, postEvent.ID())
			for _, child := range []string{preFork.ForkRunID, postFork.ForkRunID} {
				var deliveries, publications int
				m16QueryCount(t, fixture.db, &deliveries, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, child)
				m16QueryCount(t, fixture.db, &publications, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, child, postEvent.ID())
				childDeliveries += deliveries
				childPublications += publications
			}
			if sourceDeliveries != 1 || sourcePublication != 1 || childDeliveries != 0 || childPublications != 0 {
				t.Fatalf("delivery/publication effects duplicated or lost: source delivery=%d receipt=%d fork deliveries=%d fork publications=%d", sourceDeliveries, sourcePublication, childDeliveries, childPublications)
			}
			revisionFixture := authorActivityReceiptFixture{store: fixture.store.(authorActivityReceiptStore), db: fixture.db}
			for _, id := range []string{runID, preFork.ForkRunID, postFork.ForkRunID} {
				requireCompleteRunForkRevision(t, ctx, revisionFixture, id)
			}
			beforeReplay := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
			for _, cut := range []struct {
				request runfork.RunForkPlanRequest
				id      string
			}{{runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID}, preFork.ForkRunID}, {runfork.RunForkPlanRequest{SourceRunID: runID, At: postEvent.ID()}, postFork.ForkRunID}} {
				plan, err := owner.PlanRunFork(ctx, cut.request)
				if err != nil || plan.ForkPoint.Revision == 0 {
					t.Fatalf("fixed cut changed after materialization: %+v %v", plan.ForkPoint, err)
				}
				request := preRequest
				if cut.id == postFork.ForkRunID {
					request = postRequest
				}
				again, err := owner.MaterializeRunForkForSelectedContractExecution(ctx, request)
				if err != nil || again.ForkRunID != cut.id || again.ForkPoint != plan.ForkPoint {
					t.Fatalf("exact fork replay wrote a different cut: %+v %v", again, err)
				}
			}
			if afterReplay := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(beforeReplay, afterReplay) {
				t.Fatal("exact selected-fork replay changed source or child history/effects")
			}
		})
	}
}

func m16RequireHistoricalEventSet(t *testing.T, plan runfork.RunForkPlan, input string, wantOutput bool, output string) {
	t.Helper()
	ids, ok := plan.HistoricalEventIDs(plan.ForkPoint.Revision)
	if !ok || len(ids) != plan.EventCountAtFork {
		t.Fatalf("fixed event history missing: events=%v plan=%+v", ids, plan.ForkPoint)
	}
	seenInput, seenOutput := false, false
	for _, id := range ids {
		seenInput = seenInput || id == input
		seenOutput = seenOutput || output != "" && id == output
	}
	if !seenInput || seenOutput != wantOutput {
		t.Fatalf("fixed publication membership: ids=%v input=%s output=%s wanted=%t", ids, input, output, wantOutput)
	}
}

func m16RequireWork(t *testing.T, plan runfork.RunForkPlan, eventID, classification string) {
	t.Helper()
	count := 0
	for _, work := range plan.PendingWork {
		if work.EventID == eventID && work.Classification == classification {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("event %s has %d fixed %s work facts, want one: %+v", eventID, count, classification, plan.PendingWork)
	}
}

func m16RequireEntity(t *testing.T, db *sql.DB, runID, entityID, wantState string, wantRevision int, key string, want any) {
	t.Helper()
	var state string
	var raw []byte
	var revision int
	if err := db.QueryRow(`SELECT current_state,fields,revision FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&state, &raw, &revision); err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if state != wantState || fields[key] != want || revision != wantRevision {
		t.Fatalf("run %s entity state=%q fields=%v revision=%d, want %q/%d %s=%v", runID, state, fields, revision, wantState, wantRevision, key, want)
	}
}

func m16RequireCompanion(t *testing.T, db *sql.DB, runID string, want int) {
	t.Helper()
	var count int
	m16QueryCount(t, db, &count, `SELECT COUNT(*) FROM flow_instances WHERE CAST(run_id AS TEXT)=$1 AND instance_path=$1 AND flow_template='.' AND status='active'`, runID)
	if count != want {
		var path, flow, status string
		err := db.QueryRow(`SELECT instance_path,flow_template,status FROM flow_instances WHERE run_id=$1 LIMIT 1`, runID).Scan(&path, &flow, &status)
		t.Fatalf("run %s has %d exact root companions, want %d; first=%q/%q/%q err=%v", runID, count, want, path, flow, status, err)
	}
}

func m16QueryCount(t *testing.T, db *sql.DB, count *int, query string, args ...any) {
	t.Helper()
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(count); err != nil {
		t.Fatal(err)
	}
}
