package runtimepersistence

import (
	"context"
	"testing"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestDeliveryAuthorityActivationOutcomeAcknowledgesOnlyCommittedAttemptBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var store runtimedelivery.Store
			if backend == "sqlite" {
				store = newBootstrappedSQLiteRuntimeStoreForTest(t)
			} else {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store = admitTestPostgresStore(t, db)
			}
			ctx := testAuthorActivityContext()
			commit, err := store.ActivateDeliveryAuthorityOutcome(ctx, runtimedelivery.ExecutionAuthority{})
			if commit.Acknowledged || err == nil {
				t.Fatalf("invalid authority activation = %+v, %v", commit, err)
			}
			authority, err := runtimedelivery.NewNormalExecutionAuthority(sourceartifactfixture.Fact(), uuid.NewString(), 1)
			if err != nil {
				t.Fatal(err)
			}
			commit, err = store.ActivateDeliveryAuthorityOutcome(context.Background(), authority)
			if !commit.Acknowledged || err != nil {
				t.Fatalf("valid authority activation = %+v, %v", commit, err)
			}
		})
	}
}
