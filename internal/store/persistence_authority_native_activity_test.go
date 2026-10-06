package store_test

import (
	"strings"
	"testing"
)

func TestNativeActivitySetupDoesNotReceiveRawAuthority(t *testing.T) {
	findings := debtLoadPersistenceAuthorityFindings(t, persistenceAuthorityRepoRoot(t))
	for _, finding := range findings {
		if nativeActivitySetupAuthority(finding) {
			t.Errorf("native activity fixture regained raw authority: %s", finding.registryLine())
		}
	}
}

func nativeActivitySetupAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/workflow_activity_native_fixture_test.go",
		"internal/runtime/pipeline/workflow_activity_native_fixture_external_test.go":
		return true
	case "internal/runtime/pipeline/activity_engine_test.go":
		name := strings.TrimSuffix(strings.TrimPrefix(finding.Enclosing, "Verify"), "ForTest")
		name = strings.TrimPrefix(name, "Test")
		switch name {
		case "PipelineActivityRequestExecutesNonIdempotentHTTPToolOnceWithStaticCredentials",
			"GeneratedSyntheticConnectorUsesCanonicalActivityJournalOnReplay",
			"PipelineActivityRequestNonIdempotentFailureDoesNotRetry",
			"PipelineActivityRequestNonIdempotentTransportErrorMarksUncertain",
			"PipelineActivityRequestStartedJournalBlocksProviderRedispatchWithoutTerminalizing",
			"PipelineActivityRequestConcurrentDuplicatePreservesOriginalTerminalResult",
			"PipelineActivityRequestMissingCredentialFailsAfterClaimBeforeDispatch",
			"PipelineActivityRequestTelegramConnectorMissingTokenFailsAfterClaimBeforeDispatch":
			return true
		}
	}
	return false
}
