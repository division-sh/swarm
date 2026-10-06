package store_test

import "testing"

func TestNativeLoopClaimFixturesDoNotReceiveRawAuthority(t *testing.T) {
	findings := debtLoadPersistenceAuthorityFindings(t, persistenceAuthorityRepoRoot(t))
	for _, finding := range findings {
		if nativeLoopFixtureAuthority(finding) {
			t.Errorf("native loop fixture regained raw authority: %s", finding.registryLine())
		}
	}
}

func nativeLoopFixtureAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/store/storetest/activity_claim_reply_loss.go":
		return true
	case "internal/runtime/pipeline/activity_journal_test.go":
		switch finding.Enclosing {
		case "TestLoopActivityClaimOrdersAgainstRepeatAndCloseOnBothStores", "VerifyLoopActivityClaimOrdersAgainstRepeatAndCloseOnBothStoresForTest", "seedLoopActivityInstance", "seedNativeLoopActivityInstance":
			return true
		}
	case "internal/runtime/pipeline/activity_engine_test.go":
		switch finding.Enclosing {
		case "TestLoopActivityClaimCommitAcknowledgmentLossReconcilesWithoutDispatch", "VerifyLoopActivityClaimCommitAcknowledgmentLossReconcilesWithoutDispatchForTest", "(github.com/division-sh/swarm/internal/runtime/pipeline.activityCommitAckLossRunner).RunRuntimeMutationContext", "activityCommitAckLossRunner":
			return true
		}
	}
	return false
}

func TestNativeLoopFixtureGuardRejectsRawParametersAndCallbackEvidence(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyLoopActivityClaimCommitAcknowledgmentLossReconcilesWithoutDispatchForTest(db *sql.DB){db.QueryRow("one")}`,
		`package fixture;import("context";"database/sql");func VerifyLoopActivityClaimCommitAcknowledgmentLossReconcilesWithoutDispatchForTest(read func(context.Context,*sql.Tx)error){_ = read}`,
	} {
		findings := debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/activity_engine_test.go", source)
		rejected := false
		for _, finding := range findings {
			rejected = rejected || nativeLoopFixtureAuthority(finding)
		}
		if !rejected {
			t.Fatal("native loop verifier recovered raw authority")
		}
	}
}
