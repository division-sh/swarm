package store_test

import (
	"strings"
	"testing"
)

func pipelineObservationAuthorityViolations(findings []authorityFinding) []string {
	var violations []string
	for _, finding := range findings {
		if pipelineObservationConsumerIsClosed(finding.File, finding.Enclosing) && (finding.RawSQL || finding.Kind == "callback-type") {
			violations = append(violations, finding.key())
		}
	}
	return violations
}

func pipelineObservationConsumerIsClosed(path, declaration string) bool {
	switch path {
	case "internal/store/storetest/pipeline_storage_observation.go",
		"internal/store/storetest/handler_selection_storage.go",
		"internal/store/storetest/connector_credential_storage.go":
		return true
	case "internal/store/storetest/activity_result_readback.go":
		return declaration == "ReadActivityAttemptStorage"
	case "internal/runtime/connector_journal_readback_test.go":
		return declaration == "runtimeConnectorActivityRows" || declaration == "runtimeConnectorActivityRowsForSource"
	case "internal/runtime/pipeline/handler_rule_selection_supported_surface_external_test.go":
		return declaration == "assertPersistedHandlerRuleSelectionInFlow"
	case "internal/runtime/pipeline/a2_map_fan_out_execution_external_test.go":
		return strings.HasSuffix(declaration, ").outputs") || strings.HasSuffix(declaration, ").compositeReceipt") || strings.HasSuffix(declaration, ").compositeOutputs")
	}
	if strings.HasPrefix(path, "internal/runtime/") && strings.HasSuffix(path, "_supported_surface_test.go") {
		return connectorObservationDeclarationIsClosed(declaration)
	}
	return false
}

func connectorObservationDeclarationIsClosed(declaration string) bool {
	switch declaration {
	case "tryLoadGitHubAppIssueCommentActivityAttempt", "countGitHubAppIssueCommentActivityAttempts", "countGitHubAppIssueCommentActivityAttemptsForSource",
		"tryLoadGitHubActivityAttempt", "countGitHubActivityAttemptsForSource",
		"tryLoadMicrosoftGraphActivityAttempt", "countMicrosoftGraphActivityAttempts", "countMicrosoftGraphActivityAttemptsForSource",
		"tryLoadNotionManagedConnectorActivityAttempt", "countNotionManagedConnectorActivityAttempts", "countNotionManagedConnectorActivityAttemptsForSource",
		"tryLoadSlackManagedConnectorActivityAttempt", "countSlackManagedConnectorActivityAttempts", "countSlackManagedConnectorActivityAttemptsForSource",
		"tryLoadTelegramConnectorSupportedSurfaceActivityAttempt", "countTelegramConnectorSupportedSurfaceActivityAttempts", "countTelegramConnectorSupportedSurfaceActivityAttemptsForEvent", "telegramConnectorSupportedSurfaceActivityStatusForEvent",
		"assertSlackManagedConnectorNoStoredSecret", "assertTelegramConnectorSupportedSurfaceNoStoredSecret":
		return true
	}
	return false
}

func TestPipelineObservationCompletedConsumersStayClosed(t *testing.T) {
	findings := loadPersistenceAuthorityFindings(t, persistenceAuthorityRepoRoot(t))
	if violations := pipelineObservationAuthorityViolations(findings); len(violations) != 0 {
		t.Fatalf("completed pipeline/connector observations regained raw authority:\n%s", strings.Join(violations, "\n"))
	}
}

func TestPipelineObservationGuardRejectsRawAndCallbackAuthority(t *testing.T) {
	for _, row := range []struct{ path, declaration string }{
		{"internal/store/storetest/pipeline_storage_observation.go", "ReadFanOutRunProgressStorage"},
		{"internal/store/storetest/handler_selection_storage.go", "ReadHandlerSelectionStorage"},
		{"internal/store/storetest/connector_credential_storage.go", "ReadConnectorCredentialLeakStorage"},
		{"internal/store/storetest/activity_result_readback.go", "ReadActivityAttemptStorage"},
		{"internal/runtime/connector_journal_readback_test.go", "runtimeConnectorActivityRows"},
		{"internal/runtime/pipeline/handler_rule_selection_supported_surface_external_test.go", "assertPersistedHandlerRuleSelectionInFlow"},
		{"internal/runtime/slack_connector_managed_credential_supported_surface_test.go", "assertSlackManagedConnectorNoStoredSecret"},
		{"internal/runtime/telegram_connector_supported_surface_test.go", "tryLoadTelegramConnectorSupportedSurfaceActivityAttempt"},
	} {
		for _, signature := range []string{"db *sql.DB", "read func(context.Context,*sql.Tx) error"} {
			source := "package fixture\nimport(\"context\";\"database/sql\")\nfunc " + row.declaration + "(ctx context.Context," + signature + "){}"
			if violations := pipelineObservationAuthorityViolations(authorityFindingsFromSource(t, row.path, source)); len(violations) == 0 {
				t.Fatalf("%s/%s accepted raw/callback authority", row.path, row.declaration)
			}
		}
	}
	for _, name := range []string{"outputs", "compositeReceipt", "compositeOutputs"} {
		source := "package fixture\nimport(\"context\";\"database/sql\")\ntype execution struct{}\nfunc(p *execution) " + name + "(ctx context.Context, db *sql.DB){}"
		if violations := pipelineObservationAuthorityViolations(authorityFindingsFromSource(t, "internal/runtime/pipeline/a2_map_fan_out_execution_external_test.go", source)); len(violations) == 0 {
			t.Fatalf("method %s accepted raw authority", name)
		}
	}
}
