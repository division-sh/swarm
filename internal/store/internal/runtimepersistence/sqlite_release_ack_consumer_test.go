package runtimepersistence

import (
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestSQLiteReleaseOutcomeRetainsCommittedLeaseAndHandoffError(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	fixture := newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	ctx = originalStoreFixtureContext(t, ctx, store)
	identity := fixture.authority.Target.AgentIdentity
	lease := &runtimesessions.Lease{SessionID: fixture.sessionID, GrantID: fixture.grantID, Identity: identity, LockOwner: fixture.leaseHolder}
	released, err := store.lLMSQLiteOwner.ReleaseOutcome(ctx, lease)
	if !released.Acknowledged || err != nil {
		t.Fatalf("released=%+v error=%v", released, err)
	}
	var holder string
	if err := fixture.db.QueryRowContext(ctx, `SELECT COALESCE(lease_holder, '') FROM agent_sessions WHERE session_id=?`, fixture.sessionID).Scan(&holder); err != nil {
		t.Fatal(err)
	}
	if holder != "" {
		t.Fatalf("acknowledged release left lease holder %q", holder)
	}
	released, err = store.lLMSQLiteOwner.ReleaseOutcome(ctx, lease)
	if released.Acknowledged || err == nil {
		t.Fatalf("repeat released=%+v error=%v", released, err)
	}
}
