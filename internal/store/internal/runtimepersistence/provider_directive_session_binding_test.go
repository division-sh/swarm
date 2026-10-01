package runtimepersistence

import (
	"testing"
	"time"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/google/uuid"
)

func TestProviderDirectiveSessionBindingStaleOriginParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, base completionSettlementFixture) {
		for _, scenario := range []string{"reconciled_expiry", "executed", "foreign_operation"} {
			t.Run(scenario, func(t *testing.T) {
				fixture := freshProviderDrainFixture(t, base.sqlite)
				store := requireProviderDirectiveStore(t, fixture)
				origin, operation, event := admitProviderDirectiveOrigin(t, fixture, store, scenario)
				before := providerDirectiveDeliveryCount(t, fixture)
				switch scenario {
				case "reconciled_expiry":
					query := `UPDATE agent_directive_operations SET execution_lease_expires_at=? WHERE operation_id=?`
					if !fixture.sqlite {
						query = `UPDATE agent_directive_operations SET execution_lease_expires_at=$1 WHERE operation_id=$2::uuid`
					}
					if _, err := fixture.db.Exec(query, time.Now().UTC().Add(-time.Hour), operation.OperationID); err != nil {
						t.Fatal(err)
					}
					// Expiry is recovery evidence; reconciliation withdraws the exact executing origin.
					reconciled, ok, err := store.ReconcileDirectiveOperation(testAuthorActivityContext(), operation.OperationID, time.Now().UTC(), time.Hour)
					if err != nil || !ok || reconciled.State != runtimeagentcontrol.DirectiveOperationIndeterminate {
						t.Fatalf("reconcile expired directive: state=%q found=%v err=%v", reconciled.State, ok, err)
					}
				case "executed":
					if _, err := store.RecordDirectiveExecuted(testAuthorActivityContext(), operation.OperationID, origin.ExecutionOwnerID, directiveOperationResponseForTest(operation), time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
				case "foreign_operation":
					origin.OperationID = uuid.NewString()
				}
				ctx := providerDirectiveContext(t, fixture, origin, event, scenario)
				if _, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte(scenario)); err == nil {
					t.Fatal("stale directive admitted before provider launch")
				}
				requireProviderAttemptCount(t, fixture, 0)
				requireProviderDirectiveDeliveryCount(t, fixture, before)
			})
		}
	})
}
