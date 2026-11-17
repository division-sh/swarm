package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestSelectedProviderAuthorityReadersKeepSharedFencePostgres(t *testing.T) {
	selected, db, _ := selectedForkDiscardTestStore(t, "postgres")
	store := selected.(*PostgresStore)
	fixture := newSelectedProviderCompletionFixture(t, selected, db, false)
	ctx := originalStoreFixtureContext(t, testAuthorActivityContextForBundle(fixture.request.DeclarationPlan.BundleHash), selected)
	issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "selected-reader-lock", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	admitSelectedProviderFixture(t, ctx, fixture, issued, authority)
	authority.Target = selectedProviderTarget(fixture)
	claims := make([]deliverylifecycle.Claim, 2)
	for index := range claims {
		claimCtx := selectedProviderClaimContext(t, ctx, fixture, authority)
		claim, ok := deliverylifecycle.ClaimFromContext(claimCtx)
		if !ok {
			t.Fatal("selected provider fixture omitted its delivery claim")
		}
		claims[index] = claim
	}

	owned, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ready := make(chan error, len(claims))
	done := make(chan error, len(claims))
	release := make(chan struct{})
	for _, claim := range claims {
		go func() {
			done <- store.backend.RunTransaction(owned, func(txctx context.Context, tx *sql.Tx) error {
				_, err := store.deliveryPostgresOwner.ProviderOriginSnapshotTx(txctx, tx, claim)
				ready <- err
				if err != nil {
					return err
				}
				select {
				case <-release:
				case <-txctx.Done():
					return txctx.Err()
				}
				return store.effectPostgresOwner.RequireExternalEffectAuthorityTx(txctx, tx, authority, true)
			})
		}()
	}
	var setupErr error
	for range claims {
		if err := <-ready; err != nil && setupErr == nil {
			setupErr = err
		}
	}
	close(release)
	if setupErr != nil {
		t.Fatalf("prepare concurrent selected readers: %v", setupErr)
	}
	for range claims {
		if err := <-done; err != nil {
			t.Errorf("concurrent selected authority reader: %v", err)
		}
	}
}
