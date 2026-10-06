package store_test

import "testing"

func TestNativeMockFixturesDoNotReceiveRawAuthority(t *testing.T) {
	findings := debtLoadPersistenceAuthorityFindings(t, persistenceAuthorityRepoRoot(t))
	for _, finding := range findings {
		if nativeMockFixtureAuthority(finding) {
			t.Errorf("native mock fixture regained raw authority: %s", finding.registryLine())
		}
	}
}

func nativeMockFixtureAuthority(finding authorityFinding) bool {
	if !finding.RawSQL || finding.File != "internal/runtime/pipeline/activity_engine_test.go" {
		return false
	}
	switch finding.Enclosing {
	case "TestPipelineActivityRequestMockFlowLocalProviderConnectorUsesGeneratedResponseAndJournal", "VerifyPipelineActivityRequestMockFlowLocalProviderConnectorUsesGeneratedResponseAndJournalForTest",
		"TestPipelineActivityRequestMockTerminalReplayDoesNotRequireCurrentResponsePlan", "VerifyPipelineActivityRequestMockTerminalReplayDoesNotRequireCurrentResponsePlanForTest",
		"TestPipelineActivityRequestMockAdmissionFailsBeforeJournalCredentialsAndHTTP", "VerifyPipelineActivityRequestMockAdmissionFailsBeforeJournalCredentialsAndHTTPForTest",
		"TestMockOnlyPostureRejectsLiveActivityBeforeJournalCredentialsAndHTTP", "VerifyMockOnlyPostureRejectsLiveActivityBeforeJournalCredentialsAndHTTPForTest":
		return true
	}
	return false
}

func TestNativeMockFixtureGuardRejectsRawParametersAndCallbackEvidence(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyPipelineActivityRequestMockAdmissionFailsBeforeJournalCredentialsAndHTTPForTest(db *sql.DB){db.QueryRow("one")}`,
		`package fixture;import("context";"database/sql");func VerifyMockOnlyPostureRejectsLiveActivityBeforeJournalCredentialsAndHTTPForTest(read func(context.Context,*sql.Tx)error){_ = read}`,
	} {
		findings := debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/activity_engine_test.go", source)
		rejected := false
		for _, finding := range findings {
			rejected = rejected || nativeMockFixtureAuthority(finding)
		}
		if !rejected {
			t.Fatal("native mock verifier recovered raw evidence authority")
		}
	}
}
