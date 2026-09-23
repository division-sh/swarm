package runtimepersistence

import (
	"errors"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestSQLiteReleaseOutcomeRetainsCommittedLeaseAndHandoffError(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	fixture := newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true)
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	identity := fixture.authority.Target.AgentIdentity
	var bundleHash string
	if err := fixture.db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=?`, identity.RunID).Scan(&bundleHash); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected release postcommit handoff failure")
	submits := 0
	sink := &completionHandoffEvidenceProbeSink{submit: func(candidate runtimerunlifecycle.Candidate) error {
		submits++
		return injected
	}}
	registration, err := store.RegisterCompletionCandidateSink(ctx, runtimerunlifecycle.CandidateScope{BundleHash: bundleHash}, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Release()
	lease := &runtimesessions.Lease{SessionID: fixture.sessionID, Identity: identity, LockOwner: fixture.leaseHolder}
	released, err := store.lLMSQLiteOwner.ReleaseOutcome(ctx, lease)
	if !released.Acknowledged || !errors.Is(err, injected) || submits != 1 {
		t.Fatalf("released=%+v handoff=%v submissions=%d", released, err, submits)
	}
	var holder string
	if err := fixture.db.QueryRowContext(ctx, `SELECT COALESCE(lease_holder, '') FROM agent_sessions WHERE session_id=?`, fixture.sessionID).Scan(&holder); err != nil {
		t.Fatal(err)
	}
	if holder != "" {
		t.Fatalf("acknowledged release left lease holder %q", holder)
	}
	released, err = store.lLMSQLiteOwner.ReleaseOutcome(ctx, lease)
	if released.Acknowledged || err == nil || submits != 1 {
		t.Fatalf("repeat released=%+v error=%v submissions=%d", released, err, submits)
	}
}
