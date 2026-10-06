package store_test

import "testing"

func TestNativeJournalFixturesDoNotReceiveRawAuthority(t *testing.T) {
	findings := debtLoadPersistenceAuthorityFindings(t, persistenceAuthorityRepoRoot(t))
	for _, finding := range findings {
		if nativeJournalFixtureAuthority(finding) {
			t.Errorf("native journal fixture regained raw authority: %s", finding.registryLine())
		}
	}
}

func nativeJournalFixtureAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/store/storetest/workflow_journal_storage.go":
		return true
	case "internal/runtime/pipeline/activity_journal_test.go":
		switch finding.Enclosing {
		case "TestActivityJournalFixtureTerminalNoopBothStores", "VerifyActivityJournalFixtureTerminalNoopBothStoresForTest",
			"TestActivityAttemptJournalSQLiteAndPostgres", "VerifyActivityAttemptJournalSQLiteAndPostgresForTest",
			"TestActivityAttemptJournalPreservesReplyContextAcrossRestart", "VerifyActivityAttemptJournalPreservesReplyContextAcrossRestartForTest",
			"seedActivityReplyContext":
			return true
		}
	}
	return false
}

func TestNativeJournalFixtureGuardRejectsRawParametersAndCallbackEvidence(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyActivityAttemptJournalSQLiteAndPostgresForTest(db *sql.DB){db.QueryRow("one")}`,
		`package fixture;import("context";"database/sql");func VerifyActivityAttemptJournalSQLiteAndPostgresForTest(read func(context.Context,*sql.Tx)error){_ = read}`,
	} {
		findings := debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/activity_journal_test.go", source)
		rejected := false
		for _, finding := range findings {
			rejected = rejected || nativeJournalFixtureAuthority(finding)
		}
		if !rejected {
			t.Fatal("native journal verifier recovered raw evidence authority")
		}
	}
}
