package store_test

import "testing"

func TestNativeChannelTerminalFixturesDoNotReceiveRawAuthority(t *testing.T) {
	findings := debtLoadPersistenceAuthorityFindings(t, persistenceAuthorityRepoRoot(t))
	for _, finding := range findings {
		if nativeChannelFixtureAuthority(finding) {
			t.Errorf("native channel/terminal fixture regained raw authority: %s", finding.registryLine())
		}
	}
}

func nativeChannelFixtureAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/activity_engine_test.go":
		switch finding.Enclosing {
		case "TestChannelProjectedActivityResultJournalsAndReplaysAcrossSelectedStores", "VerifyChannelProjectedActivityResultJournalsAndReplaysAcrossSelectedStoresForTest",
			"TestChannelActivityPostCommitAcknowledgmentLossStateBlocksRedispatchAcrossSelectedStores", "VerifyChannelActivityPostCommitAcknowledgmentLossStateBlocksRedispatchAcrossSelectedStoresForTest",
			"newActivityJournalStoreForCase":
			return true
		}
	case "internal/runtime/pipeline/acknowledged_store_result_consumer_test.go":
		return finding.Enclosing == "TestActivityTerminalPostCommitErrorPublishesJournaledResultBothStores" ||
			finding.Enclosing == "VerifyActivityTerminalPostCommitErrorPublishesJournaledResultBothStoresForTest"
	case "internal/runtime/pipeline/activity_journal_acknowledged_fixture_test.go":
		return finding.Enclosing == "TestActivityJournalFixtureTerminalAcknowledgementSurvivesPostCommitError" ||
			finding.Enclosing == "VerifyActivityJournalFixtureTerminalAcknowledgementSurvivesPostCommitErrorForTest"
	}
	return false
}

func TestNativeChannelFixtureGuardRejectsRawParametersAndCallbackEvidence(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyChannelProjectedActivityResultJournalsAndReplaysAcrossSelectedStoresForTest(db *sql.DB){db.QueryRow("one")}`,
		`package fixture;import("context";"database/sql");func VerifyChannelActivityPostCommitAcknowledgmentLossStateBlocksRedispatchAcrossSelectedStoresForTest(read func(context.Context,*sql.Tx)error){_ = read}`,
	} {
		findings := debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/activity_engine_test.go", source)
		rejected := false
		for _, finding := range findings {
			rejected = rejected || nativeChannelFixtureAuthority(finding)
		}
		if !rejected {
			t.Fatal("native channel verifier recovered raw evidence authority")
		}
	}
}
