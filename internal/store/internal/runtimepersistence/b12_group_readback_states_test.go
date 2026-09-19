package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func TestB12GroupReadbackConflictDominatesMissingHandoffBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newB10GroupFaultFixture(t, backend)
			out, err := f.group.Settle(f.ctx, f.members)
			if err != nil || len(out.Results) != len(f.members) {
				t.Fatalf("initial real settlement: %+v %v", out, err)
			}
			// Contradictory persisted route plus missing handoff must not be
			// softened into a merely incomplete publication observation.
			id := f.members[0].Claim.EventID()
			if _, err := f.db.Exec(`UPDATE decision_card_route_obligations SET status='quarantined',quarantined_at=completed_at,completed_at=NULL WHERE event_id=$1`, id); err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.Exec(`UPDATE event_deliveries SET continuation_handoff_at=NULL WHERE event_id=$1`, id); err != nil {
				t.Fatal(err)
			}
			before := f.snapshot(t)
			observed, err := f.group.ReadPublicationSettlement(f.ctx, f.members)
			if err != nil || len(observed.Rows) != len(f.members) || observed.Rows[0].State != pipelineobligation.PublicationSettlementConflict || !strings.Contains(observed.Rows[0].Reason, "decision route disposition differs") {
				t.Fatalf("contradiction hidden by incomplete handoff: %+v err=%v", observed, err)
			}
			f.requireSnapshot(t, before)
		})
	}
}

func TestB12GroupReadbackMixedRecoveryDoesNotMintAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newB10GroupFaultFixture(t, backend)
			f.requireObservation(t, pipelineobligation.PublicationSettlementPending)
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			owner := f.raw.(pipelineObligationParityStore).PipelineObligations()
			// Pending decision routes use the decision consumer's exact admission,
			// not generic pipeline recovery, which correctly excludes them.
			first, err := owner.ClaimEvent(f.ctx, f.members[0].Claim.EventID(), pipelineobligation.PurposeDecisionRoute)
			if err != nil {
				t.Fatal(err)
			}
			ack, err := owner.Settle(f.ctx, first.Claim, f.members[0].Disposition)
			if err != nil || !ack.Committed() {
				t.Fatalf("independent predecessor recovery: %+v err=%v", ack, err)
			}
			next, err := owner.ClaimEvent(f.ctx, f.members[1].Claim.EventID(), pipelineobligation.PurposeDecisionRoute)
			if err != nil {
				t.Fatal(err)
			}
			before := f.snapshot(t)
			submits := f.sink.submits
			for repeat := 0; repeat < 2; repeat++ {
				observed, err := f.group.ReadPublicationSettlement(f.ctx, f.members)
				if err != nil || len(observed.Rows) != len(f.members) || observed.Rows[0].State != pipelineobligation.PublicationSettlementSatisfied || observed.Rows[1].State != pipelineobligation.PublicationSettlementPending || observed.ObservedAt.IsZero() {
					t.Fatalf("mixed independent recovery observation: %+v err=%v", observed, err)
				}
				f.requireSnapshot(t, before)
			}
			if f.sink.submits != submits {
				t.Fatal("readback submitted completion work")
			}
			out, err := f.group.Settle(f.ctx, f.members)
			if err == nil || len(out.Results) != 0 {
				t.Fatalf("observation revived closed group claims: %+v err=%v", out, err)
			}
			ack, err = owner.Settle(f.ctx, next.Claim, f.members[1].Disposition)
			if err != nil || !ack.Committed() {
				t.Fatalf("readback retired independent successor authority: %+v err=%v", ack, err)
			}
		})
	}
}

func TestB12GroupUnavailableReadbackReturnsNoPartialRowsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newB10GroupFaultFixture(t, backend)
			before := f.snapshot(t)
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			observed, err := f.group.ReadPublicationSettlement(ctx, f.members)
			if !errors.Is(err, context.Canceled) || len(observed.Rows) != 0 || !observed.ObservedAt.IsZero() {
				t.Fatalf("unavailable observation manufactured evidence: %+v err=%v", observed, err)
			}
			f.requireSnapshot(t, before)
			f.requireObservation(t, pipelineobligation.PublicationSettlementPending)
		})
	}
}

func TestB12GroupCorruptLaterMemberDiscardsObservedPrefixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newB10GroupFaultFixture(t, backend)
			out, err := f.group.Settle(f.ctx, f.members)
			if err != nil || len(out.Results) != len(f.members) {
				t.Fatalf("initial settlement: %+v %v", out, err)
			}
			// The first row is independently readable, but the later ordinal
			// contradicts the sealed event identity. No prefix is usable evidence.
			if _, err := f.db.Exec(`UPDATE fan_out_outcomes SET event_id=$1 WHERE event_id=$2`, f.seed.eventID, f.members[1].Claim.EventID()); err != nil {
				t.Fatal(err)
			}
			before := f.snapshot(t)
			observed, err := f.group.ReadPublicationSettlement(f.ctx, f.members)
			if err == nil || !strings.Contains(err.Error(), "durable ordinal identity conflicts") || len(observed.Rows) != 0 || !observed.ObservedAt.IsZero() {
				t.Fatalf("corrupt later member leaked prefix evidence: %+v err=%v", observed, err)
			}
			f.requireSnapshot(t, before)
		})
	}
}
