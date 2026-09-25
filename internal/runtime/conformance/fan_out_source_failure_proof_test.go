package conformance

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func TestFanOutServingPayloadEntityAndResourceSnapshotsPreserveOrdinalsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSemanticProofFixture(t, backend)
			release := f.pauseAtEmptyScan(t)
			defer release()
			payloadRows, entityRows := semanticProofRows(35), semanticProofRows(33)
			payloadID := f.submit(t, semanticProofPayloadEvent, payloadRows, 81)
			f.waitIntent(t, payloadID, 0, "open")
			entityID := f.submit(t, semanticProofEntityEvent, entityRows, 62)
			f.waitIntent(t, entityID, 0, "open")
			resourceRows := semanticProofRows(34)
			for i := range resourceRows {
				resourceRows[i]["account_id"] = fmt.Sprintf("resource-%02d", i)
				resourceRows[i]["portfolio_id"] = f.runID
				resourceRows[i]["eligible"] = true
				resourceRows[i]["ordinal"] = i
				resourceRows[i]["source_count"] = len(resourceRows)
				resourceRows[i]["snapshot_threshold"] = 93
			}
			resourceSource := f.installPinnedResourceSource(t, resourceRows)
			// Trigger payload deliberately differs from the pinned resource rows.
			resourceID := f.submit(t, semanticProofResourceEvent, semanticProofRows(34), 93)
			f.waitIntent(t, resourceID, 0, "open")
			overwriteID := f.submit(t, semanticProofOverwriteEvent, semanticProofRows(1), 10)
			semanticProofWait(t, func() (bool, error) {
				var delivered int
				err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2 AND status='delivered'`, f.runID, overwriteID).Scan(&delivered)
				return delivered == 1, err
			})
			var current []byte
			if err := f.db.QueryRowContext(f.ctx, `SELECT fields FROM entity_state WHERE run_id=$1 AND flow_instance='portfolio'`, f.runID).Scan(&current); err != nil {
				t.Fatal(err)
			}
			var state struct {
				Threshold int               `json:"threshold"`
				Rows      []json.RawMessage `json:"account_ids"`
			}
			if err := json.Unmarshal(current, &state); err != nil || state.Threshold != 10 || len(state.Rows) != 1 {
				t.Fatalf("real overwrite did not supersede both capsules: %s %v", current, err)
			}
			var total, progressed int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*),SUM(cursor) FROM fan_out_intents WHERE run_id=$1`, f.runID).Scan(&total, &progressed); err != nil || total != 3 || progressed != 0 {
				t.Fatalf("sources were not simultaneously pending: intents=%d cursor=%d err=%v", total, progressed, err)
			}
			release()
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, f.runID, 10*time.Second)
			f.assertOutcomes(t, payloadID, payloadRows, 81, nil)
			f.assertOutcomes(t, entityID, entityRows, 62, nil)
			f.assertOutcomes(t, resourceID, resourceRows, 93, nil)
			f.probe.mu.Lock()
			payloadIntent, entityIntent := f.probe.intents[payloadID], f.probe.intents[entityID]
			resourceIntent := f.probe.intents[resourceID]
			f.probe.mu.Unlock()
			if resourceIntent.Source != resourceSource {
				t.Fatalf("resource source escaped its exact pinned version: got=%+v want=%+v", resourceIntent.Source, resourceSource)
			}
			var pin string
			if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path=$2 AND event_name=$3`, f.runID, resourceSource.Declaration.FlowPath, resourceSource.Declaration.EventName).Scan(&pin); err != nil || pin != string(resourceSource.VersionID) {
				t.Fatalf("serving changed immutable resource pin: %s %v", pin, err)
			}
			node := identitytest.FlowNode(t, "portfolio", "portfolio-coordinator")
			payloadPlan := f.source.FanOutPlansForHandler(node, semanticProofPayloadEvent)
			entityPlan := f.source.FanOutPlansForHandler(node, semanticProofEntityEvent)
			resourcePlan := f.source.FanOutPlansForHandler(node, semanticProofResourceEvent)
			if len(payloadPlan) != 1 || len(entityPlan) != 1 || len(resourcePlan) != 1 {
				t.Fatal("proof requires exact admitted producer plans")
			}
			if resourceIntent.Request.PlanRef != resourcePlan[0].Ref || resourcePlan[0].ResourceSource == nil {
				t.Fatalf("resource fixture changed its compiled evaluator owner: %+v", resourceIntent.Request.PlanRef)
			}
			if payloadIntent.Source.Kind != fanoutobligation.SourceEventPayloadField || payloadIntent.Source.EventID != payloadID || payloadIntent.Request.PlanRef != payloadPlan[0].Ref {
				t.Fatalf("payload/rules owner changed: %+v", payloadIntent)
			}
			if entityIntent.Source.Kind != fanoutobligation.SourceEntityField || entityIntent.Source.RunID != f.runID || entityIntent.Source.MutationID == "" || entityIntent.Request.PlanRef != entityPlan[0].Ref {
				t.Fatalf("entity/on_complete owner changed: %+v", entityIntent)
			}
			var mutationEvent string
			if err := f.db.QueryRowContext(f.ctx, `SELECT caused_by_event FROM entity_mutations WHERE run_id=$1 AND mutation_id=$2 AND entity_id=$3 AND domain='authored_field' AND path='account_ids'`, f.runID, entityIntent.Source.MutationID, entityIntent.Source.EntityID).Scan(&mutationEvent); err != nil || mutationEvent != entityID {
				t.Fatalf("entity source not bound to its own handler mutation: event=%s err=%v", mutationEvent, err)
			}
			if _, copied := entityIntent.Request.Capsule.Entity["account_ids"]; copied {
				t.Fatal("entity collection copied into capsule instead of exact mutation source")
			}
		})
	}
}

func TestFanOutServingMixedAndCommitFailureIsolationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSemanticProofFixture(t, backend, true)
			release := f.pauseAtEmptyScan(t)
			defer release()
			type batch struct {
				name, trigger string
				rows          []map[string]any
				policy        semanticProofPolicy
				rejected      map[int]bool
			}
			batches := []batch{
				{name: "unknown", rows: semanticProofRows(4), policy: semanticProofPolicy{failure: "unknown"}},
				{name: "authorization", rows: semanticProofRows(4), policy: semanticProofPolicy{failure: "authorization"}},
				{name: "retry", rows: semanticProofRows(4), policy: semanticProofPolicy{failure: "retry"}},
				{name: "aggregate", rows: semanticProofRows(8), policy: semanticProofPolicy{maxCommit: 2}},
				{name: "mixed", rows: semanticProofRows(4), rejected: map[int]bool{1: true, 2: true}},
				{name: "healthy", rows: semanticProofRows(3)},
			}
			batches[4].rows[1]["external_id"] = ""
			batches[4].rows[2]["gem_score"] = "not-a-number"
			for i := range batches {
				batches[i].trigger = f.submit(t, semanticProofPayloadEvent, batches[i].rows, 75)
				f.waitIntent(t, batches[i].trigger, 0, "open")
				f.probe.mu.Lock()
				f.probe.policies[batches[i].trigger] = batches[i].policy
				f.probe.mu.Unlock()
			}
			release()
			for _, batch := range batches {
				if batch.name == "unknown" || batch.name == "authorization" {
					f.waitIntent(t, batch.trigger, 0, "blocked")
					var count, leased int
					var raw string
					if err := f.db.QueryRowContext(f.ctx, `SELECT (SELECT COUNT(*) FROM fan_out_outcomes o WHERE o.run_id=i.run_id AND o.triggering_delivery_id=i.triggering_delivery_id),CASE WHEN i.claim_owner IS NULL THEN 0 ELSE 1 END,i.blocked_reason FROM fan_out_intents i JOIN event_deliveries d ON d.delivery_id=i.triggering_delivery_id WHERE i.run_id=$1 AND d.event_id=$2`, f.runID, batch.trigger).Scan(&count, &leased, &raw); err != nil {
						t.Fatal(err)
					}
					failure, err := runtimefailures.UnmarshalEnvelope([]byte(raw))
					want := runtimefailures.ClassInternalFailure
					if batch.name == "authorization" {
						want = runtimefailures.ClassAuthorizationDenied
					}
					if err != nil || failure.Class != want || count != 0 || leased != 0 {
						t.Fatalf("%s escaped exact blocked/no-progress disposition: failure=%+v outcomes=%d leased=%d err=%v", batch.name, failure, count, leased, err)
					}
					continue
				}
				f.waitIntent(t, batch.trigger, len(batch.rows), "closed")
				f.assertOutcomes(t, batch.trigger, batch.rows, 75, batch.rejected)
			}
			semanticProofWait(t, func() (bool, error) {
				summary, err := f.selected.FanOutRunSummary(f.ctx, f.runID, time.Now().UTC())
				return summary.Intents == 6 && summary.Blocked == 2 && summary.Cardinality == 27 && summary.Cursor == 19 && summary.Committed == 17 && summary.SemanticRejected == 2 && summary.Owed == 8 && summary.Unsettled == 0, err
			})
			f.probe.mu.Lock()
			unknown := append([]semanticProofAttempt(nil), f.probe.attempts[batches[0].trigger]...)
			auth := append([]semanticProofAttempt(nil), f.probe.attempts[batches[1].trigger]...)
			retry := append([]semanticProofAttempt(nil), f.probe.attempts[batches[2].trigger]...)
			aggregate := append([]semanticProofAttempt(nil), f.probe.attempts[batches[3].trigger]...)
			sqlFaults := append([]semanticProofAttempt(nil), f.probe.sqlFaults[batches[3].trigger]...)
			retryIntent := f.probe.intents[batches[2].trigger]
			_, retryReleased := f.probe.retries[batches[2].trigger]
			blockCount := len(f.probe.blocks)
			f.probe.mu.Unlock()
			if len(unknown) != 1 || len(auth) != 1 || !unknown[0].injected || !auth[0].injected || blockCount != 2 {
				t.Fatalf("permanent failures were retried or crossed intents: unknown=%+v auth=%+v blocks=%d", unknown, auth, blockCount)
			}
			if len(retry) != 2 || !retry[0].injected || retry[1].injected || retry[0].start != 0 || retry[1].start != 0 || retryIntent.NextChunkSize != 16 || !retryReleased {
				t.Fatalf("retry did not yield without semantic progress: attempts=%+v intent=%+v released=%t", retry, retryIntent, retryReleased)
			}
			if len(aggregate) < 3 || aggregate[0] != (semanticProofAttempt{0, 8, true}) || aggregate[1] != (semanticProofAttempt{0, 4, true}) || aggregate[2] != (semanticProofAttempt{0, 2, false}) {
				t.Fatalf("safe aggregate did not commit lowest prefix first: %+v", aggregate)
			}
			var injected []semanticProofAttempt
			for _, attempt := range aggregate {
				if attempt.injected {
					injected = append(injected, attempt)
				}
			}
			if !reflect.DeepEqual(sqlFaults, injected) {
				t.Fatalf("safe aggregate attempts did not all fail through canonical SQL rollback: got=%+v want=%+v", sqlFaults, injected)
			}
			// Remaining open work belongs only to the two blocked suffixes; healthy
			// siblings have durable effects without clearing or canceling failures.
			var leaked int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND claim_owner IS NOT NULL`, f.runID).Scan(&leaked); err != nil || leaked != 0 {
				t.Fatalf("claims leaked after isolated dispositions: %d %v", leaked, err)
			}
		})
	}
}

func TestFanOutPinnedResourceChunkRollbackAndRetryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSemanticProofFixture(t, backend, true)
			release := f.pauseAtEmptyScan(t)
			defer release()
			rows := semanticProofRows(4)
			for index := range rows {
				rows[index]["account_id"] = fmt.Sprintf("rollback-resource-%02d", index)
				rows[index]["portfolio_id"] = f.runID
				rows[index]["eligible"] = true
				rows[index]["ordinal"] = index
				rows[index]["source_count"] = len(rows)
				rows[index]["snapshot_threshold"] = 75
			}
			source := f.installPinnedResourceSource(t, rows)
			trigger := f.submit(t, semanticProofResourceEvent, semanticProofRows(1), 75)
			f.waitIntent(t, trigger, 0, "open")
			f.probe.mu.Lock()
			f.probe.policies[trigger] = semanticProofPolicy{maxCommit: 2}
			f.probe.mu.Unlock()
			release()
			f.waitIntent(t, trigger, len(rows), "closed")
			f.assertOutcomes(t, trigger, rows, 75, nil)
			f.probe.mu.Lock()
			attempts := append([]semanticProofAttempt(nil), f.probe.attempts[trigger]...)
			faults := append([]semanticProofAttempt(nil), f.probe.sqlFaults[trigger]...)
			intent := f.probe.intents[trigger]
			f.probe.mu.Unlock()
			if intent.Source != source || len(attempts) < 3 || attempts[0] != (semanticProofAttempt{0, 4, true}) || attempts[1] != (semanticProofAttempt{0, 2, false}) || len(faults) == 0 || faults[0] != attempts[0] {
				t.Fatalf("resource rollback/retry changed exact source or publication sequence: source=%+v attempts=%+v faults=%+v", intent.Source, attempts, faults)
			}
		})
	}
}
