package swarmflowtest

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogCollectBootIssues_DoesNotReintroduceFlowLocalEventWarningDrift(t *testing.T) {
	repoRoot := repoRootForTest(t)
	bundle := catalogLoadBootBundle(t, filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-local-events"))

	issues := catalogCollectBootIssues(bundle)
	for _, issue := range issues {
		if issue.Category != "EVENT-NO-SCHEMA" {
			continue
		}
		if strings.Contains(issue.Message, "child/child.internal") || strings.Contains(issue.Message, "child/child.done") {
			t.Fatalf("unexpected flow-local event warning drift: %#v", issues)
		}
	}
}

func TestCatalogCollectBootIssues_DoesNotReintroduceFlowOutputConsumerWarningDrift(t *testing.T) {
	repoRoot := repoRootForTest(t)
	bundle := catalogLoadBootBundle(t, filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-required-agents-child"))

	issues := catalogCollectBootIssues(bundle)
	for _, issue := range issues {
		if issue.Category == "EVENT-NO-CONSUMER" && strings.Contains(issue.Message, "analysis.done") {
			t.Fatalf("unexpected flow-owned output consumer warning drift: %#v", issues)
		}
	}
}
