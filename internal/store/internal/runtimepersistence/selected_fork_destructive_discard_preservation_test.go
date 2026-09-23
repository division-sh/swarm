package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// This pins the two deliberately different discard outcomes: whole-run deletion
// without completion evidence, and a retained run with a historical tombstone.
func TestSelectedForkDestructiveDiscardPendingDeliveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, _ := selectedForkDiscardTestStore(t, backend)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			requirePausedRunForTest(t, ctx, store, runID, time.Now().UTC())
			eventID, deliveryID := seedSelectedForkDiscardPendingDelivery(t, ctx, store, db, runID)
			before := selectedForkDiscardDurableCounts(t, ctx, db, runID, eventID, deliveryID)
			if before.Runs != 1 || before.Events != 1 || before.Deliveries != 1 || before.Revisions == 0 || before.Facts == 0 || before.Occurrences == 0 {
				t.Fatalf("unretained pending-delivery fixture incomplete: %#v", before)
			}
			beforeHead := selectedForkDiscardAuthorOrderHead(t, ctx, db)

			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := store.DiscardMaterializedSelectedContractExecutionFork(cancelled, runID); err == nil {
				t.Fatal("pre-cancelled discard succeeded")
			}
			if after := selectedForkDiscardDurableCounts(t, ctx, db, runID, eventID, deliveryID); after != before {
				t.Fatalf("pre-cancelled discard mutated durable state: before=%#v after=%#v", before, after)
			}

			if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, runID); err != nil {
				t.Fatalf("discard unretained run with pending delivery: %v", err)
			}
			want := selectedForkDiscardCounts{}
			if after := selectedForkDiscardDurableCounts(t, ctx, db, runID, eventID, deliveryID); after != want {
				t.Fatalf("whole-run discard left durable state: %#v", after)
			}
			afterHead := selectedForkDiscardAuthorOrderHead(t, ctx, db)
			if afterHead <= beforeHead {
				t.Fatalf("whole-run discard rewound or failed to advance global author order: before=%d after=%d", beforeHead, afterHead)
			}
			freshRunID := uuid.NewString()
			requirePausedRunForTest(t, ctx, store, freshRunID, time.Now().UTC())
			var nextSequence int64
			if err := db.QueryRowContext(ctx, `SELECT MIN(sequence) FROM author_activity_occurrences WHERE run_id=$1`, freshRunID).Scan(&nextSequence); err != nil || nextSequence <= afterHead {
				t.Fatalf("next run reused discarded author order: sequence=%d err=%v, discarded head=%d", nextSequence, err, afterHead)
			}
			if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, runID); err != nil {
				t.Fatalf("repeat discard of absent parent: %v", err)
			}
			if after := selectedForkDiscardDurableCounts(t, ctx, db, runID, eventID, deliveryID); after != want {
				t.Fatalf("repeat discard recreated durable state: %#v", after)
			}
			neverExisted := uuid.NewString()
			if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, neverExisted); err != nil {
				t.Fatalf("discard never-existing parent: %v", err)
			}
			if after := selectedForkDiscardDurableCounts(t, ctx, db, neverExisted, eventID, deliveryID); after != want {
				t.Fatalf("discard of never-existing parent created durable state: %#v", after)
			}
		})
	}
}

func selectedForkDiscardAuthorOrderHead(t *testing.T, ctx context.Context, db *sql.DB) int64 {
	t.Helper()
	var head int64
	if err := db.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id=1`).Scan(&head); err != nil {
		t.Fatalf("load global author activity order: %v", err)
	}
	return head
}

func TestSelectedForkRetainedDiscardPendingDeliveryTombstonesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedCompletionFixture(t, store, db, sqlite)
			ctx := testAuthorActivityContext()
			issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatalf("issue retained execution: %v", err)
			}
			authority, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "discard-proof", time.Minute)
			if err != nil {
				t.Fatalf("claim retained execution: %v", err)
			}
			authority.Target = selectedAgentTurnTarget(fixture.forkRun)
			completionCtx := runtimeeffects.WithLogicalOperationIdentity(
				runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), newCompletionControllerForTest(store)),
				"selected:discard-both-stores",
			)
			completionCtx = managedSelectedExecutionStoreTestContext(t, completionCtx, authority)
			completionCtx = withManagedCompletionTestSurface(t, completionCtx, authority, "openai_compatible")
			handle, err := beginManagedCompletionForTest(t, completionCtx, "openai_compatible", []byte("discard-preservation"))
			if err != nil {
				t.Fatalf("begin retained completion: %v", err)
			}
			if err := handle.MarkLaunched(completionCtx); err != nil {
				t.Fatalf("launch retained completion: %v", err)
			}
			if err := handle.MarkResponseObserved(completionCtx, map[string]any{"response_fingerprint": "discard-preservation"}); err != nil {
				t.Fatalf("observe retained completion: %v", err)
			}
			settleSelectedCompletionForTest(t, completionCtx, handle, authority.Target, time.Now().UTC())
			if err := store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
				t.Fatalf("quiesce retained execution: %v", err)
			}
			if err := store.CloseRunForkSelectedContractRuntimeExecution(ctx, authority.ID); err != nil {
				t.Fatalf("close retained execution: %v", err)
			}

			eventID, deliveryID := seedSelectedForkDiscardPendingDelivery(t, ctx, store, db, fixture.forkRun)
			before := selectedForkDiscardDurableCounts(t, ctx, db, fixture.forkRun, eventID, deliveryID)
			if before.Runs != 1 || before.Events != 1 || before.Deliveries != 1 || before.Executions != 1 || before.Bindings != 1 || before.Attempts != 1 {
				t.Fatalf("retained pending-delivery fixture incomplete: %#v", before)
			}
			var beforeRevision int64
			if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1`, fixture.forkRun).Scan(&beforeRevision); err != nil {
				t.Fatalf("load pre-discard revision: %v", err)
			}
			if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err != nil {
				t.Fatalf("discard retained run: %v", err)
			}
			after := selectedForkDiscardDurableCounts(t, ctx, db, fixture.forkRun, eventID, deliveryID)
			if after.Runs != 1 || after.Events != 0 || after.Deliveries != 0 || after.Executions != 1 || after.Bindings != 1 || after.Attempts != 1 {
				t.Fatalf("retained discard lost evidence or kept mutable rows: %#v", after)
			}
			var status, executionState string
			if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1`, fixture.forkRun).Scan(&status); err != nil || status != "cancelled" {
				t.Fatalf("retained run status=%q err=%v, want cancelled", status, err)
			}
			if err := db.QueryRowContext(ctx, `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, issued.ExecutionID).Scan(&executionState); err != nil || executionState != "closed" {
				t.Fatalf("retained execution state=%q err=%v, want closed", executionState, err)
			}
			var afterRevision int64
			if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1`, fixture.forkRun).Scan(&afterRevision); err != nil || afterRevision != beforeRevision+1 {
				t.Fatalf("discard revision=%d err=%v, want %d", afterRevision, err, beforeRevision+1)
			}
			for _, fact := range []struct{ family, key string }{
				{string(runforkrevision.FamilyEvents), eventID},
				{string(runforkrevision.FamilyEventDeliveries), deliveryID},
			} {
				var tombstones int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2 AND family=$3 AND fact_key=$4 AND NOT present`, fixture.forkRun, afterRevision, fact.family, fact.key).Scan(&tombstones); err != nil || tombstones != 1 {
					t.Fatalf("%s/%s tombstones=%d err=%v, want 1", fact.family, fact.key, tombstones, err)
				}
			}
			validationTx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatalf("begin retained-history validation: %v", err)
			}
			if sqlite {
				err = runforkrevision.ValidateCompleteSQLite(ctx, validationTx, fixture.forkRun)
			} else {
				err = runforkrevision.ValidateCompletePostgres(ctx, validationTx, fixture.forkRun)
			}
			_ = validationTx.Rollback()
			if err != nil {
				t.Fatalf("retained discard left incomplete history: %v", err)
			}
			if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err == nil {
				t.Fatal("repeat retained discard unexpectedly accepted terminal parent")
			}
			if repeated := selectedForkDiscardDurableCounts(t, ctx, db, fixture.forkRun, eventID, deliveryID); !reflect.DeepEqual(repeated, after) {
				t.Fatalf("repeat retained discard changed durable state: before=%#v after=%#v", after, repeated)
			}
		})
	}
}

func TestSelectedForkDestructiveDiscardRollbackPendingDeliveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, sqlite := selectedForkDiscardTestStore(t, backend)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			requirePausedRunForTest(t, ctx, store, runID, time.Now().UTC())
			eventID, deliveryID := seedSelectedForkDiscardPendingDelivery(t, ctx, store, db, runID)
			before := selectedForkDiscardDurableCounts(t, ctx, db, runID, eventID, deliveryID)
			installSelectedForkDiscardFailure(t, ctx, db, sqlite)
			if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, runID); err == nil || !strings.Contains(err.Error(), "injected selected discard failure") {
				t.Fatalf("discard failure=%v, want injected rollback", err)
			}
			if after := selectedForkDiscardDurableCounts(t, ctx, db, runID, eventID, deliveryID); after != before {
				t.Fatalf("failed whole-run discard partially deleted state: before=%#v after=%#v", before, after)
			}
			if sqlite {
				if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_selected_discard`); err != nil {
					t.Fatalf("drop SQLite failure trigger: %v", err)
				}
			} else {
				if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_selected_discard ON events`); err != nil {
					t.Fatalf("drop PostgreSQL failure trigger: %v", err)
				}
			}
			if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, runID); err != nil {
				t.Fatalf("retry whole-run discard after rollback: %v", err)
			}
			if after := selectedForkDiscardDurableCounts(t, ctx, db, runID, eventID, deliveryID); after != (selectedForkDiscardCounts{}) {
				t.Fatalf("successful retry left durable state: %#v", after)
			}
		})
	}
}

func TestSelectedForkDestructiveDiscardRefusesDependentForkBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, _ := selectedForkDiscardTestStore(t, backend)
			materializer, ok := store.(interface {
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
			})
			if !ok {
				t.Fatal("selected store does not expose fork materialization")
			}
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			requirePausedRunForTest(t, ctx, store, runID, time.Now().UTC())
			eventStore, ok := store.(semanticEventFixtureStore)
			if !ok {
				t.Fatal("selected store does not expose semantic event commit")
			}
			forkPointID := uuid.NewString()
			forkPoint := eventtest.PersistedProjection(forkPointID, "selected.fork_point", "selected-test", "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixture(ctx, eventStore, forkPoint); err != nil {
				t.Fatalf("commit forkable source event: %v", err)
			}
			child, err := materializer.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: forkPointID})
			if err != nil {
				t.Fatalf("materialize dependent fork: %v", err)
			}
			eventID, deliveryID := seedSelectedForkDiscardPendingDelivery(t, ctx, store, db, runID)
			before := selectedForkDiscardDurableCounts(t, ctx, db, runID, eventID, deliveryID)
			if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, runID); err == nil || !strings.Contains(err.Error(), child.ForkRunID) {
				t.Fatalf("dependent-fork discard=%v, want refusal naming %s", err, child.ForkRunID)
			}
			if after := selectedForkDiscardDurableCounts(t, ctx, db, runID, eventID, deliveryID); after != before {
				t.Fatalf("dependent-fork refusal mutated source state: before=%#v after=%#v", before, after)
			}
			var childRows int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id=$1`, child.ForkRunID).Scan(&childRows); err != nil || childRows != 1 {
				t.Fatalf("dependent fork rows=%d err=%v, want 1", childRows, err)
			}
		})
	}
}

type selectedForkDiscardCounts struct {
	Runs, Events, Deliveries, Executions, Bindings, Attempts, Revisions, Facts, Heads, Occurrences int
}

func selectedForkDiscardDurableCounts(t *testing.T, ctx context.Context, db *sql.DB, runID, eventID, deliveryID string) selectedForkDiscardCounts {
	t.Helper()
	var counts selectedForkDiscardCounts
	for _, item := range []struct {
		query string
		args  []any
		dest  *int
	}{
		{`SELECT COUNT(*) FROM runs WHERE run_id=$1`, []any{runID}, &counts.Runs},
		{`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, []any{runID, eventID}, &counts.Events},
		{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND delivery_id=$2`, []any{runID, deliveryID}, &counts.Deliveries},
		{`SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, []any{runID}, &counts.Executions},
		{`SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1`, []any{runID}, &counts.Bindings},
		{`SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON a.operation_id=o.operation_id JOIN run_fork_selected_contract_runtime_executions e ON e.execution_id=o.selected_execution_id WHERE e.fork_run_id=$1`, []any{runID}, &counts.Attempts},
		{`SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`, []any{runID}, &counts.Revisions},
		{`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1`, []any{runID}, &counts.Facts},
		{`SELECT COUNT(*) FROM run_fork_revision_heads WHERE run_id=$1`, []any{runID}, &counts.Heads},
		{`SELECT COUNT(*) FROM author_activity_occurrences WHERE run_id=$1`, []any{runID}, &counts.Occurrences},
	} {
		if err := db.QueryRowContext(ctx, item.query, item.args...).Scan(item.dest); err != nil {
			t.Fatalf("count discard state with %q: %v", item.query, err)
		}
	}
	return counts
}

func seedSelectedForkDiscardPendingDelivery(t *testing.T, ctx context.Context, store selectedForkDiscardStore, db *sql.DB, runID string) (string, string) {
	t.Helper()
	eventID := uuid.NewString()
	event := eventtest.PersistedProjection(eventID, "selected.discard_pending", "selected-test", "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("discard-pending")),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "discard-flow", FlowInstance: "discard-flow/one"}),
	}
	if err := commitSemanticEventFixtureWithRoutes(ctx, store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("commit pending discard delivery: %v", err)
	}
	var deliveryID, status string
	if err := db.QueryRowContext(ctx, `SELECT delivery_id,status FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, runID, eventID).Scan(&deliveryID, &status); err != nil {
		t.Fatalf("load pending discard delivery: %v", err)
	}
	if status != "pending" {
		t.Fatalf("discard fixture delivery status=%q, want pending", status)
	}
	return eventID, deliveryID
}
