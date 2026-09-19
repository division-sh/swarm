package runtimepersistence

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

// This is the selected owner's legal terminal-disposition boundary, not a new
// bus interceptor outcome or a fan-out event renamed to mailbox.card_decided.
func TestB08GroupDecisionQuarantinePreservesHealthyMemberBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newB10GroupFaultFixture(t, backend)
			failure := failures.Normalize(failures.New(failures.ClassSchemaInvalid, "b08_invalid_decision", "b08-proof", "validate_decision", nil), "b08-proof", "validate_decision")
			f.members[1].Disposition = pipelineobligation.Quarantined("b08_invalid_decision", &failure)
			revisions := countP16RunRevisions(t, f.db, f.seed.runID)
			collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restore)
			out, err := f.group.Settle(f.ctx, f.members)
			if err != nil || len(out.Results) != 2 {
				t.Fatalf("real mixed quarantine segment: %+v err=%v", out, err)
			}
			for i, member := range f.members {
				result := out.Results[i]
				if result.Claim != member.Claim || !result.Outcome.Committed() || result.Outcome.DeliveryHandoffCommitted() != (i == 0) {
					t.Fatalf("member %d acknowledgement/handoff: %+v", i, result)
				}
				var status, outcome, reason string
				var count, handed int
				if err := f.db.QueryRowContext(f.ctx, `SELECT status FROM decision_card_route_obligations WHERE event_id=$1`, member.Claim.EventID()).Scan(&status); err != nil {
					t.Fatal(err)
				}
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*),MAX(outcome),MAX(reason_code) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, member.Claim.EventID()).Scan(&count, &outcome, &reason); err != nil {
					t.Fatal(err)
				}
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND continuation_handoff_at IS NOT NULL`, member.Claim.EventID()).Scan(&handed); err != nil {
					t.Fatal(err)
				}
				wantStatus, wantOutcome, wantHanded := "completed", "success", 1
				if i == 1 {
					wantStatus, wantOutcome, wantHanded = "quarantined", "dead_letter", 0
					var raw []byte
					if err := f.db.QueryRowContext(f.ctx, `SELECT failure FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, member.Claim.EventID()).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					got, err := failures.UnmarshalEnvelope(raw)
					if err != nil || !reflect.DeepEqual(got, failure) {
						t.Fatalf("quarantine changed typed failure: %+v err=%v", got, err)
					}
				}
				if count != 1 || status != wantStatus || outcome != wantOutcome || reason != member.Disposition.ReasonCode() || handed != wantHanded {
					t.Fatalf("member %d facts: receipt=%d/%s/%s route=%s handed=%d", i, count, outcome, reason, status, handed)
				}
			}
			counts := collector.Snapshot()
			if counts.ByOperation[transactiontest.PipelineSettlement].WriteCommits != 1 || counts.ByOperation[transactiontest.PipelineSettlement].Revision.Finalizations != 1 || counts.Active != 0 || countP16RunRevisions(t, f.db, f.seed.runID) != revisions+1 {
				t.Fatalf("mixed dispositions escaped one real segment/revision: %+v", counts)
			}
			if f.sink.reserves != 1 || f.sink.submits != 1 || f.sink.cancels != 0 {
				t.Fatalf("mixed segment candidate ownership: %+v", f.sink)
			}
			f.requireObservation(t, pipelineobligation.PublicationSettlementSatisfied)
			before := f.snapshot(t)
			if out, err := f.group.Settle(f.ctx, f.members); !errors.Is(err, pipelineobligation.ErrStaleClaim) || len(out.Results) != 0 {
				t.Fatalf("repeated quarantine segment: %+v err=%v", out, err)
			}
			f.requireSnapshot(t, before)
		})
	}
}

func TestB08GroupRoutedFailurePreservesExactOutcomesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"terminal_middle", "dead_letter_first"} {
			t.Run(backend+"/"+kind, func(t *testing.T) {
				f, _, coordinator := newMixedExecutionFixture(t, backend, nil)
				setup := b08SetupDeliveryRows(t, f)
				if len(setup) != 1 {
					t.Fatalf("expected one separate setup delivery: %v", setup)
				}
				failed, visited, delivered := 2, 3, 2
				deadLetter := kind == "dead_letter_first"
				if deadLetter {
					failed, visited, delivered = 0, 4, 5
				}
				fault := failures.New(failures.ClassComputeFailure, "b08_routed_failure", "b08-proof", "dispatch", nil)
				f.bus.SetInterceptors(mixedDispatchFailure{eventID: f.events[failed].ID(), deadLetter: deadLetter, failure: fault}, coordinator)
				f.prepare(t)
				f.seal(t)
				committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
				if err != nil {
					t.Fatal(err)
				}
				membership := b08DeliveryMembership(t, f)
				b08RequirePreparedRoutes(t, f)
				if !reflect.DeepEqual(setup, b08SetupDeliveryRows(t, f)) {
					t.Fatal("group publication changed the separate setup delivery")
				}
				t.Logf("separate setup event=%s delivery=%s; exact committed group delivery identities=%+v", f.seed.eventID, f.seed.deliveryID, membership)
				if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
					t.Fatal(err)
				}
				observed := &mixedExecutionGroup{PublicationGroup: f.group}
				err = f.bus.DispatchFanOutPublications(f.ctx, observed, committed.Publications)
				if (!deadLetter && !errors.Is(err, fault)) || (deadLetter && err != nil) || len(observed.requests) != visited {
					t.Fatalf("routed failure dispatch: members=%d err=%v", len(observed.requests), err)
				}
				for i, request := range observed.requests {
					wantKind, wantOutcome, wantReason := pipelineobligation.DispositionAcknowledged, "success", "pipeline_persisted"
					if i == failed {
						wantKind, wantOutcome, wantReason = pipelineobligation.DispositionTerminal, "dead_letter", "pipeline_outbox_dispatch_failed"
						class, detail := failures.ClassInternalFailure, "event_interceptor_failed"
						if deadLetter {
							wantKind, wantReason, class, detail = pipelineobligation.DispositionDeadLetter, "b08_routed_failure", failures.ClassComputeFailure, "b08_routed_failure"
						}
						mixedAssertFailure(t, f, f.events[i].ID(), class, detail)
					}
					if request.Claim != f.claims[i] || request.Disposition.Kind() != wantKind || request.Disposition.ReasonCode() != wantReason {
						t.Fatalf("member %d changed disposition: %+v", i, request)
					}
					mixedAssertReceipt(t, f, f.events[i].ID(), wantOutcome, wantReason)
				}
				results := 0
				for _, out := range observed.results {
					for _, result := range out.Results {
						results++
						if !result.Outcome.Committed() || result.Outcome.DeliveryHandoffCommitted() != (result.Claim != f.claims[failed]) {
							t.Fatalf("failed routed member borrowed successful handoff: %+v", result)
						}
					}
				}
				if results != visited {
					t.Fatalf("acknowledged %d members, want %d", results, visited)
				}
				b08AwaitMixedDeliveryCounts(t, f, delivered)
				if after := b08DeliveryMembership(t, f); !reflect.DeepEqual(membership, after) {
					t.Fatalf("dispatch changed exact delivery membership: before=%+v after=%+v", membership, after)
				}
				b08RequirePreparedRoutes(t, f)
				if after := b08SetupDeliveryRows(t, f); !reflect.DeepEqual(setup, after) {
					t.Fatalf("dispatch changed setup delivery rows: before=%v after=%v", setup, after)
				}
				var routes, handed, completed int
				wantRoutes := 4
				if deadLetter {
					wantRoutes = 1
				}
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN continuation_handoff_at IS NOT NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END),0) FROM event_deliveries WHERE event_id=$1`, f.events[failed].ID()).Scan(&routes, &handed, &completed); err != nil || routes != wantRoutes || handed != 0 || completed != 0 {
					t.Fatalf("failed routed event lost routes or fabricated success: routes=%d handoffs=%d delivered=%d err=%v", routes, handed, completed, err)
				}
				if !deadLetter {
					var receipts int
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, f.events[3].ID()).Scan(&receipts); err != nil || receipts != 0 {
						t.Fatalf("unvisited suffix acknowledged: count=%d err=%v", receipts, err)
					}
					if err := f.group.Close(f.ctx); err != nil {
						t.Fatal(err)
					}
					work, err := f.store().ClaimEvent(f.ctx, f.events[3].ID(), pipelineobligation.PurposeRecovery)
					if err != nil {
						t.Fatalf("unvisited suffix lost canonical recovery: %v", err)
					}
					if err := f.store().Release(f.ctx, work.Claim); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

type b08DeliveryIdentity struct {
	EventID, DeliveryID, RouteIdentity, SubscriberType, SubscriberID string
}

func b08DeliveryMembership(t *testing.T, f *groupProofFixture) []b08DeliveryIdentity {
	t.Helper()
	wantCounts := []int{1, 1, 4, 0}
	if len(f.events) != len(wantCounts) || len(f.plans) != len(wantCounts) {
		t.Fatalf("exact four-event group changed: events=%d plans=%d", len(f.events), len(f.plans))
	}
	counts := map[string]int{}
	for i, event := range f.events {
		if event.ID() == f.seed.eventID {
			t.Fatal("group event borrowed setup event identity")
		}
		if _, exists := counts[event.ID()]; exists {
			t.Fatalf("duplicate group event identity: %s", event.ID())
		}
		counts[event.ID()] = wantCounts[i]
	}
	counts[f.seed.eventID] = 1
	rows, err := f.db.QueryContext(f.ctx, `SELECT event_id,delivery_id,route_identity,subscriber_type,subscriber_id FROM event_deliveries WHERE run_id=$1 ORDER BY event_id,delivery_id`, f.seed.runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []b08DeliveryIdentity
	for rows.Next() {
		var row b08DeliveryIdentity
		if err := rows.Scan(&row.EventID, &row.DeliveryID, &row.RouteIdentity, &row.SubscriberType, &row.SubscriberID); err != nil {
			t.Fatal(err)
		}
		remaining, exists := counts[row.EventID]
		if !exists || remaining <= 0 {
			t.Fatalf("unexpected or excess delivery for exact event membership: %+v", row)
		}
		counts[row.EventID]--
		if row.EventID == f.seed.eventID {
			if row.DeliveryID != f.seed.deliveryID {
				t.Fatalf("setup event changed delivery identity: %+v", row)
			}
			continue
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for eventID, remaining := range counts {
		if remaining != 0 {
			t.Fatalf("event %s missing %d exact routes; observed=%+v", eventID, remaining, out)
		}
	}
	return out
}

func b08RequirePreparedRoutes(t *testing.T, f *groupProofFixture) {
	t.Helper()
	for i, plan := range f.plans {
		want := plan.(bus.EnginePublicationPlan).PublicationCommand().Commit.DeliveryRoutes
		mixedAssertRecipients(t, i, want)
		read, found, err := f.raw.(storeTestDurableEventBusStore).LoadPreparedPublishEvent(f.ctx, f.events[i].ID())
		if err != nil || !found || !reflect.DeepEqual(mixedRouteKeys(t, read.DeliveryRoutes), mixedRouteKeys(t, want)) {
			t.Fatalf("group event %s changed prepared recipients: found=%v routes=%+v err=%v", f.events[i].ID(), found, read.DeliveryRoutes, err)
		}
	}
}

func b08SetupDeliveryRows(t *testing.T, f *groupProofFixture) []string {
	t.Helper()
	rows, err := f.db.QueryContext(f.ctx, `SELECT * FROM event_deliveries WHERE run_id=$1 AND event_id=$2 ORDER BY delivery_id`, f.seed.runID, f.seed.eventID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		values, targets := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(raw))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func b08AwaitMixedDeliveryCounts(t *testing.T, f *groupProofFixture, wantDelivered int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var total, delivered, trigger int
		if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, f.seed.runID, f.seed.eventID).Scan(&trigger); err != nil || trigger != 1 {
			t.Fatalf("fixture trigger delivery membership: count=%d err=%v", trigger, err)
		}
		// Count only the four exact group IDs, not all deliveries in the run.
		if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END),0) FROM event_deliveries WHERE run_id=$1 AND event_id IN ($2,$3,$4,$5)`, f.seed.runID, f.events[0].ID(), f.events[1].ID(), f.events[2].ID(), f.events[3].ID()).Scan(&total, &delivered); err != nil {
			t.Fatal(err)
		}
		// Failed routes remain exact durable work, never erased to make the
		// count of all routes equal the count of successful deliveries.
		if total != 6 || delivered > wantDelivered {
			t.Fatalf("mixed failure changed route membership: total=%d delivered=%d want=6/%d", total, delivered, wantDelivered)
		}
		if delivered == wantDelivered {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("healthy deliveries incomplete: total=%d delivered=%d want=6/%d", total, delivered, wantDelivered)
		}
	}
}
