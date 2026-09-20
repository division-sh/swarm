package runtimepersistence

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func TestPublicationGroupBatchConsumersFreshReadsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 3)
			f.prepare(t)
			f.seal(t)
			f.commit(t)
			requests := f.members()
			// Observation preserves caller order, not map order or ordinal order.
			requests[0], requests[2] = requests[2], requests[0]
			measure := func(name string, operation func()) {
				t.Helper()
				outcomes, events := 0, 0
				f.probe.set(func(phase, query string) error {
					if phase == "before_query" {
						q := strings.Join(strings.Fields(query), " ")
						if strings.Contains(q, "FROM fan_out_outcomes WHERE") {
							outcomes++
						}
						if strings.Contains(q, "SELECT e.event_class,") {
							events++
						}
					}
					return nil
				})
				defer f.probe.set(nil)
				operation()
				if outcomes != 1 || events != 1 {
					t.Fatalf("%s physical validation reads: outcomes=%d events=%d", name, outcomes, events)
				}
				t.Logf("%s: 3 members, outcome_queries=%d canonical_event_queries=%d", name, outcomes, events)
			}
			observe := func(state pipelineobligation.PublicationSettlementState) pipelineobligation.PublicationSettlementSnapshot {
				t.Helper()
				out, err := f.group.ReadPublicationSettlement(f.ctx, requests)
				if err != nil || len(out.Rows) != len(requests) || out.ObservedAt.IsZero() {
					t.Fatalf("observation=%+v err=%v", out, err)
				}
				for i, row := range out.Rows {
					if row.Claim != requests[i].Claim || row.State != state {
						t.Fatalf("row %d=%+v want claim=%v state=%v", i, row, requests[i].Claim, state)
					}
				}
				return out
			}
			var before pipelineobligation.PublicationSettlementSnapshot
			measure("read_pending", func() { before = observe(pipelineobligation.PublicationSettlementPending) })
			// A valid canonical mutation must be reread, not replaced by the
			// prior observation or the group's immutable publication evidence.
			id := requests[1].Claim.EventID()
			if _, err := f.db.ExecContext(f.ctx, `UPDATE events SET chain_depth=chain_depth+1 WHERE event_id=$1`, id); err != nil {
				t.Fatal(err)
			}
			if err := f.group.ValidateCommittedMembership(f.claims); err != nil {
				t.Fatalf("immutable membership unexpectedly became a fresh database read: %v", err)
			}
			preserved := f.snapshot(t)
			changed, err := f.group.ReadPublicationSettlement(f.ctx, requests)
			if err == nil || !strings.Contains(err.Error(), "committed event changed") || !reflect.DeepEqual(changed, pipelineobligation.PublicationSettlementSnapshot{}) {
				t.Fatalf("second read hid mutation or leaked a prefix: %+v %v", changed, err)
			}
			out, err := f.group.Settle(f.ctx, requests)
			if err == nil || !strings.Contains(err.Error(), "committed event changed") || len(out.Results) != 0 {
				t.Fatalf("settlement admitted changed member: %+v %v", out, err)
			}
			if after := f.snapshot(t); !reflect.DeepEqual(preserved, after) {
				t.Fatal("refused read/settlement changed durable rows")
			}
			if _, err := f.db.ExecContext(f.ctx, `UPDATE events SET chain_depth=chain_depth-1 WHERE event_id=$1`, id); err != nil {
				t.Fatal(err)
			}
			measure("settle", func() {
				out, err := f.group.Settle(f.ctx, requests)
				if err != nil || len(out.Results) != 3 {
					t.Fatalf("settlement=%+v err=%v", out, err)
				}
				for _, result := range out.Results {
					if !result.Outcome.Committed() || !result.Outcome.DeliveryHandoffCommitted() {
						t.Fatalf("lost native settlement/handoff: %+v", result)
					}
				}
			})
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := f.group.ValidateCommittedMembership(f.claims); err != nil {
				t.Fatalf("closed group's immutable membership lost: %v", err)
			}
			measure("read_satisfied_after_close", func() { observe(pipelineobligation.PublicationSettlementSatisfied) })
			if _, err := f.db.ExecContext(f.ctx, `UPDATE event_receipts SET reason_code='different' WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, id); err != nil {
				t.Fatal(err)
			}
			measure("read_mutated_receipt", func() {
				out, err := f.group.ReadPublicationSettlement(f.ctx, requests)
				if err != nil || len(out.Rows) != 3 || out.Rows[1].State != pipelineobligation.PublicationSettlementConflict || out.Rows[0].State != pipelineobligation.PublicationSettlementSatisfied || out.Rows[2].State != pipelineobligation.PublicationSettlementSatisfied {
					t.Fatalf("receipt mutation was cached: %+v %v", out, err)
				}
			})
			for _, row := range before.Rows {
				if row.State != pipelineobligation.PublicationSettlementPending {
					t.Fatalf("later calls rewrote earlier snapshot: %+v", before)
				}
			}
		})
	}
}
