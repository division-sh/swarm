package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestServedPipelineHandoffObservationPreservesEveryORArmBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, cut := range []string{"complete", "pending", "in_progress", "missing_handoff", "missing_receipt", "foreign_subscriber", "foreign_event"} {
			t.Run(backend.name+"/"+cut, func(t *testing.T) {
				fixture, ctx := backend.open(t), testAuthorActivityContext()
				claim := seedDeliveryRecoveryClaim(t, fixture, ctx)
				sibling := seedDeliveryRecoveryClaim(t, fixture, ctx)
				if cut != "in_progress" {
					if _, err := fixture.store.SettleSuccess(ctx, claim.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := fixture.db.ExecContext(ctx, `UPDATE event_deliveries SET continuation_handoff_at=$1 WHERE delivery_id=$2`, time.Now().UTC(), claim.Claim.DeliveryID()); err != nil {
					t.Fatal(err)
				}
				eventID, subscriber := claim.Snapshot.EventID, "pipeline"
				if cut == "foreign_event" {
					eventID = sibling.Snapshot.EventID
				}
				if cut == "foreign_subscriber" {
					subscriber = "not-pipeline"
				}
				if cut != "missing_receipt" {
					if _, err := fixture.db.ExecContext(ctx, `INSERT INTO event_receipts (receipt_id,event_id,subscriber_type,subscriber_id,outcome,side_effects,processed_at)
						VALUES ($1,$2,'platform',$3,'success','{}',$4)`, uuid.NewString(), eventID, subscriber, time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
				}
				switch cut {
				case "pending":
					if _, err := fixture.db.ExecContext(ctx, `UPDATE event_deliveries SET status='pending',next_eligible_at=$1,settled_at=NULL,
						current_attempt_version=NULL,current_attempt_open=NULL WHERE delivery_id=$2`, time.Now().UTC(), claim.Claim.DeliveryID()); err != nil {
						t.Fatal(err)
					}
				case "missing_handoff":
					if _, err := fixture.db.ExecContext(ctx, `UPDATE event_deliveries SET continuation_handoff_at=NULL WHERE delivery_id=$1`, claim.Claim.DeliveryID()); err != nil {
						t.Fatal(err)
					}
				}
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				want := 1
				if cut == "complete" {
					want = 0
				}
				got, err := ReadServedIncompletePipelineHandoffCountForTest(ctx, fixture.store, claim.Claim.RunID())
				if err != nil || got != want {
					t.Fatalf("exact OR/scoped handoff cut=%s got=%d want=%d err=%v", cut, got, want, err)
				}
				if siblingCount, err := ReadServedIncompletePipelineHandoffCountForTest(ctx, fixture.store, sibling.Claim.RunID()); err != nil || siblingCount != 1 {
					t.Fatalf("sibling active work disappeared: count=%d err=%v", siblingCount, err)
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("handoff observation changed storage: %v", err)
				}
			})
		}
	}
}

func TestServedPipelineHandoffObservationRefusesUnownedCancelledClosedAndMissingBothStores(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadServedIncompletePipelineHandoffCountForTest(ctx, owner, uuid.NewString()); err == nil || got != 0 {
			t.Fatalf("unowned count became quiescent evidence: %d %v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		for _, cut := range []string{"invalid_identity", "cancelled", "closed", "missing_deliveries", "missing_receipts"} {
			t.Run(backend.name+"/"+cut, func(t *testing.T) {
				fixture := backend.open(t)
				runID, readCtx := uuid.NewString(), ctx
				switch cut {
				case "invalid_identity":
					runID = "not-an-id"
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					readCtx = cancelled
				case "closed":
					if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "missing_deliveries", "missing_receipts":
					query := `ALTER TABLE event_deliveries RENAME TO unavailable_handoff_deliveries`
					if cut == "missing_receipts" {
						query = `ALTER TABLE event_receipts RENAME TO unavailable_handoff_receipts`
					}
					if _, err := fixture.db.ExecContext(ctx, query); err != nil {
						t.Fatal(err)
					}
				}
				got, err := ReadServedIncompletePipelineHandoffCountForTest(readCtx, fixture.store, runID)
				if err == nil || got != 0 || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("failed %s count became quiescent evidence: %d %v", cut, got, err)
				}
			})
		}
	}
}
