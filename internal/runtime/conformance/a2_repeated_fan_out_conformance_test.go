package conformance

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/google/uuid"
)

// Lifecycle uses the existing conformance manager; provider execution is not
// part of this proof. Publication, claims, node execution and schedules are real.
func TestA2RepeatedFanOutTriggerIsolationOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{FanOutDeliveryBarrier: true})
			probe := newNestedServingProbe(t)
			rt, db := newNestedServingRuntime(t, backend, source, nil, probe)
			runID := uuid.NewString()
			ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact), runID)
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("repeated fan-out counters: %+v", probe.snapshot())
					dumpNotifyAllChildrenRuntimeState(t, context.Background(), rt.selected, db)
				}
			})
			if err := rt.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "a2-repeated-fan-out", rt.sourceArtifactFact)); err != nil {
				t.Fatal(err)
			}
			publishNotifyAllChildrenRunCreatingEvent(t, ctx, rt, source, runID, "portfolio.opened", map[string]any{"portfolio_id": "portfolio-main"})
			accounts := []string{"acct-c", "acct-a", "acct-b"}
			publishNotifyAllChildrenEvent(t, ctx, rt, source, runID, "portfolio.accounts.register.requested", map[string]any{
				"portfolio_id": "portfolio-main", "account_ids": accounts,
			})

			probe.armPostCommitHold(t)
			first := publishNotifyAllChildrenEventAsync(t, ctx, rt, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main", "command": "first-snapshot",
			})
			held := waitNestedServingPostCommit(t, probe)
			if held.ParentEvent != first || held.Publications != len(accounts) {
				t.Fatalf("first exact held publication = %+v", held)
			}
			reader := nestedPublicReader(t, rt.selected)
			seed := publishNotifyAllChildrenEventAsync(t, ctx, rt, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main", "account_ids": []string{"acct-b"},
			})
			waitA2RepeatedTriggerDelivery(t, ctx, reader, seed)
			second := publishNotifyAllChildrenEventAsync(t, ctx, rt, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main", "command": "second-snapshot",
			})
			secondDelivery := waitA2RepeatedTriggerDelivery(t, ctx, reader, second)
			secondKey := held.Key
			secondKey.TriggeringDeliveryID = secondDelivery
			if secondKey == held.Key {
				t.Fatal("distinct trigger deliveries reused one intent")
			}
			firstMutation := assertA2RepeatedIntent(t, ctx, db, held.Key, len(accounts))
			secondMutation := assertA2RepeatedIntent(t, ctx, db, secondKey, 1)
			if firstMutation == "" || secondMutation == "" || firstMutation == secondMutation {
				t.Fatalf("overlapping intents did not freeze distinct source revisions: %q / %q", firstMutation, secondMutation)
			}
			var secondOutcomes int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2`, runID, secondDelivery).Scan(&secondOutcomes); err != nil || secondOutcomes != 0 {
				t.Fatalf("queued second intent escaped the held capacity-one turn: outcomes=%d err=%v", secondOutcomes, err)
			}
			probe.releaseHeld()
			waitNotifyAllChildrenRuntime(t, rt, runID)

			publishNotifyAllChildrenEvent(t, ctx, rt, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main", "account_ids": []string{},
			})
			empty := publishNotifyAllChildrenEvent(t, ctx, rt, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main", "command": "empty-snapshot",
			})
			emptyKey := held.Key
			emptyKey.TriggeringDeliveryID = waitA2RepeatedTriggerDelivery(t, ctx, reader, empty)
			if emptyKey == held.Key || emptyKey == secondKey {
				t.Fatal("empty trigger reused another delivery's intent")
			}
			assertA2RepeatedIntent(t, ctx, db, emptyKey, 0)
			cases := []struct {
				eventID, command string
				key              fanoutobligation.IntentKey
				accounts         []string
			}{
				{first, "first-snapshot", held.Key, accounts},
				{second, "second-snapshot", secondKey, []string{"acct-b"}},
				{empty, "empty-snapshot", emptyKey, nil},
			}
			var callbackIDs []string
			for _, tc := range cases {
				items := loadNotifyAllChildrenItemEvents(t, ctx, rt.selected, db, runID, tc.eventID)
				assertNotifyAllChildrenItemSequence(t, items, tc.accounts)
				assertNotifyAllChildrenContiguousItemOrdinals(t, items)
				callbackIDs = append(callbackIDs, tc.eventID)
				for _, item := range items {
					view := assertNestedDeliveredEvent(t, ctx, reader, item.ID, 2)
					if view.Payload["account_id"] != item.AccountID || view.Payload["command"] != tc.command || view.SourceEventID != tc.eventID {
						t.Fatalf("trigger %s borrowed another intent's payload/lineage: %+v", tc.eventID, view)
					}
					callbackIDs = append(callbackIDs, item.ID)
				}
				assertNestedExactBarrier(t, ctx, db, tc.key, "fired", fanoutbarrier.Summary{Total: len(tc.accounts), Succeeded: len(tc.accounts)})
			}
			completions := assertA2RepeatedCompletions(t, ctx, db, rt, runID, map[string]int{
				held.Key.TriggeringDeliveryID: len(accounts), secondDelivery: 1, emptyKey.TriggeringDeliveryID: 0,
			})
			callbackIDs = append(callbackIDs, completions...)
			assertNestedServingDrained(t, probe)
			before := a2RepeatedFanOutSnapshot(t, ctx, db, runID)
			beforeSummary, err := rt.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || beforeSummary.Intents != 4 || beforeSummary.Cardinality != 7 || beforeSummary.Settled != 7 || beforeSummary.BarrierTerminal != 3 || beforeSummary.BlocksCompletion() {
				t.Fatalf("repeated-trigger public summary = %+v err=%v", beforeSummary, err)
			}

			// Replay committed handoffs through their durable delivery path, not
			// another publication or an authored attempt with a fabricated claim.
			for round := 0; round < 2; round++ {
				for _, id := range callbackIDs {
					view, err := reader.LoadOperatorEvent(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					evt, err := view.EventSnapshot()
					if err != nil {
						t.Fatal(err)
					}
					if err := rt.bus.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{{Event: evt}}); err != nil {
						t.Fatalf("replay committed callback %s: %v", id, err)
					}
				}
				rt.bus.SignalDeliveryContinuations()
				waitNotifyAllChildrenRuntime(t, rt, runID)
			}
			afterSummary, err := rt.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || !reflect.DeepEqual(beforeSummary, afterSummary) || before != a2RepeatedFanOutSnapshot(t, ctx, db, runID) {
				t.Fatalf("duplicate committed callbacks changed state, history, intent or settlement: before=%+v after=%+v err=%v", beforeSummary, afterSummary, err)
			}
			assertA2RepeatedCompletions(t, ctx, db, rt, runID, map[string]int{
				held.Key.TriggeringDeliveryID: len(accounts), secondDelivery: 1, emptyKey.TriggeringDeliveryID: 0,
			})
		})
	}
}

func waitA2RepeatedTriggerDelivery(t *testing.T, ctx context.Context, reader nestedOperatorReader, eventID string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		view, err := reader.LoadOperatorEvent(ctx, eventID)
		if err != nil || len(view.Deliveries) != 1 || view.NoDelivery != nil {
			t.Fatalf("repeated trigger %s exact delivery = %+v err=%v", eventID, view, err)
		}
		if view.Deliveries[0].Terminal {
			assertNestedDeliveredEvent(t, ctx, reader, eventID, 1)
			return view.Deliveries[0].DeliveryID
		}
		if time.Now().After(deadline) {
			t.Fatalf("repeated trigger %s did not settle: %+v", eventID, view)
		}
		time.Sleep(time.Millisecond)
	}
}

func assertA2RepeatedIntent(t *testing.T, ctx context.Context, db *sql.DB, key fanoutobligation.IntentKey, cardinality int) string {
	t.Helper()
	var got int
	var mutation string
	if err := db.QueryRowContext(ctx, `SELECT cardinality,source_mutation_id FROM fan_out_intents
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`,
		key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&got, &mutation); err != nil || got != cardinality {
		t.Fatalf("exact repeated-trigger intent %+v cardinality=%d want=%d err=%v", key, got, cardinality, err)
	}
	return mutation
}

func assertA2RepeatedCompletions(t *testing.T, ctx context.Context, db *sql.DB, rt notifyAllChildrenRuntime, runID string, expected map[string]int) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='portfolio/portfolio.notify.completed' ORDER BY event_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(ids) != len(expected) {
		t.Fatalf("repeated fan-out completion count=%d want=%d err=%v", len(ids), len(expected), err)
	}
	reader := nestedPublicReader(t, rt.selected)
	seen := map[string]bool{}
	var callbacks []string
	for _, id := range ids {
		view := assertNestedDeliveredEvent(t, ctx, reader, id, 1)
		if view.Deliveries[0].SubscriberID != conformanceNode(t, "portfolio", "portfolio-coordinator").Key() || view.SourceEventID == "" {
			t.Fatalf("completion lacks exact ordinary consumer/causal occurrence: %+v", view)
		}
		occurrence := assertNestedDeliveredEvent(t, ctx, reader, view.SourceEventID, 1)
		_, ref, ok := timeridentity.ParseJoinHandle(occurrence.Payload)
		fanOut, exact := ref.FanOutDelivery()
		want, known := expected[fanOut.TriggeringDeliveryID()]
		if !ok || !exact || !known || seen[fanOut.TriggeringDeliveryID()] || occurrence.EventName != "platform.join_complete" {
			t.Fatalf("completion borrowed or repeated another intent: occurrence=%+v ref=%+v", occurrence, ref)
		}
		seen[fanOut.TriggeringDeliveryID()] = true
		for field, value := range map[string]int{"total": want, "succeeded": want, "dead_lettered": 0, "no_route": 0, "semantic_rejected": 0, "canceled": 0} {
			if view.Payload[field] != float64(value) {
				t.Fatalf("exact intent %s completion %s=%#v want=%d", fanOut.TriggeringDeliveryID(), field, view.Payload[field], value)
			}
		}
		callbacks = append(callbacks, occurrence.EventID, id)
	}
	return callbacks
}

func a2RepeatedFanOutSnapshot(t *testing.T, ctx context.Context, db *sql.DB, runID string) [8]int64 {
	t.Helper()
	var got [8]int64
	if err := db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM events WHERE run_id=$1),
		(SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1),
		(SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1),
		(SELECT COALESCE(SUM(revision),0) FROM entity_state WHERE run_id=$1),
		(SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1))`, runID).Scan(&got[0], &got[1], &got[2], &got[3], &got[4], &got[5], &got[6], &got[7]); err != nil {
		t.Fatal(err)
	}
	return got
}
