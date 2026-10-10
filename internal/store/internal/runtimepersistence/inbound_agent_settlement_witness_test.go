package runtimepersistence

import (
	"testing"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestInboundAgentSettlementWitnessRejectsRetiredReceiptCounterBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			claimed := seedDeliveryRecoveryClaim(t, fixture, ctx)
			eventID := claimed.Snapshot.EventID
			if n, err := ReadAgentSettledAttemptCountForTest(ctx, fixture.store, eventID, "agent-a"); err != nil || n != 0 {
				t.Fatalf("unsettled claim counted as completion: %d,%v", n, err)
			}
			if _, err := fixture.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			if n, err := ReadAgentSettledAttemptCountForTest(ctx, fixture.store, eventID, "agent-a"); err != nil || n != 1 {
				t.Fatalf("real agent settlement escaped witness: %d,%v", n, err)
			}
			// Deliberately reproduce the retired assertion. This negative evidence
			// is not a selected-store read port or a compatibility producer.
			var retired int
			if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='agent' AND subscriber_id=$2`, eventID, "agent-a").Scan(&retired); err != nil {
				t.Fatal(err)
			}
			if retired != 0 {
				t.Fatalf("retired agent receipt counter unexpectedly observed executable settlement: %d", retired)
			}
		})
	}
}
